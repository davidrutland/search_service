#!/usr/bin/env python3
"""Small, bounded-concurrency web retrieval service for a local LLM.

Pipeline: DDG -> Brave fallback -> URL validation/dedupe -> HTTP+Readability
-> bounded Camoufox fallback -> short LexRank summary -> JSON results.

Camoufox is a single-thread-owned scarce resource. Browser jobs are bounded
and cancelled before they enter the browser executor after request timeout.
"""
from __future__ import annotations

import html
import http.server
import ipaddress
import json
import logging
import logging.handlers
import queue
import re
import socket
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import httpx
from camoufox.sync_api import Camoufox
from sumy.nlp.tokenizers import Tokenizer
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.lex_rank import LexRankSummarizer

HOST = "0.0.0.0"
PORT = 8787
DEFAULT_LIMIT = 3
MAX_LIMIT = 20
MAX_CANDIDATES = 10
HTTP_TIMEOUT = 10.0
BROWSER_TIMEOUT = 15.0
SEARCH_TIMEOUT_MS = 10000
BROWSER_SETTLE_MS = 250
MIN_ARTICLE_CHARS = 500
MAX_SUMMARY_CHARS = 2000
MAX_CONTENT_BYTES = 2 * 1024 * 1024
MAX_ARTICLE_TEXT_BYTES = 10 * 1024 * 1024
SUMMARY_SENTENCES = 3
MAX_REDIRECTS = 10
HTTP_WORKERS = 3
BROWSER_QUEUE_SIZE = 8
TARGET_USABLE_ARTICLES = 2
ENHANCE_GLOBAL_CONCURRENCY = 3
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/140 Safari/537.36"
SCRIPT_DIR = Path(__file__).resolve().parent
READABILITY_JS_PATH = SCRIPT_DIR / "lib" / "Readability.js"
LOG_DIR = SCRIPT_DIR / "logs"
VERSION = "5.0"

logger = logging.getLogger("search_service")
logger.setLevel(logging.INFO)
logger.propagate = False
LOG_DIR.mkdir(parents=True, exist_ok=True)
_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "search_service.log", maxBytes=10 * 1024 * 1024,
    backupCount=5, encoding="utf-8")
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(threadName)s %(message)s"))
logger.addHandler(_handler)

BLOCKED_SUBNETS = tuple(ipaddress.ip_network(x) for x in (
    "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8",
    "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24",
    "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24",
    "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4"))
BLOCKED_IPV6_SUBNETS = tuple(ipaddress.ip_network(x) for x in (
    "::/128", "::1/128", "::ffff:0:0/96", "fc00::/7", "fe80::/10",
    "ff00::/8", "2001:db8::/32"))


def _blocked_ip(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    nets = BLOCKED_SUBNETS if address.version == 4 else BLOCKED_IPV6_SUBNETS
    return any(address in n for n in nets) or not address.is_global


def _resolve_host(hostname: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except (socket.gaierror, OSError):
        return []
    out: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    seen: set[str] = set()
    for info in infos:
        try:
            addr = ipaddress.ip_address(info[4][0])
        except (ValueError, IndexError):
            continue
        if str(addr) not in seen:
            seen.add(str(addr)); out.append(addr)
    return out


def is_url_safe(url: str) -> bool:
    try:
        p = urlparse(url)
        if p.scheme.lower() not in {"http", "https"} or not p.hostname:
            return False
        if p.username is not None or p.password is not None:
            return False
        if p.port is not None and not 1 <= p.port <= 65535:
            return False
        try:
            addr = ipaddress.ip_address(p.hostname)
        except ValueError:
            resolved = _resolve_host(p.hostname)
            return bool(resolved) and all(not _blocked_ip(a) for a in resolved)
        return not _blocked_ip(addr)
    except (ValueError, UnicodeError):
        return False


def clean_text(value: Any) -> str:
    return "" if value is None else re.sub(r"\s+", " ", html.unescape(str(value))).strip()


def decode_ddg_url(url: str) -> str:
    try:
        p = urlparse(url)
        target = parse_qs(p.query).get("uddg") if p.path == "/l/" else None
        return unquote(target[0]) if target else url
    except Exception:
        return url


def canonical_url(url: str) -> str:
    try:
        p = urlparse(decode_ddg_url(url))
        scheme = p.scheme.lower(); host = (p.hostname or "").lower().removeprefix("www.")
        port = p.port
        netloc = host if port in (None, 80 if scheme == "http" else 443) else f"{host}:{port}"
        path = p.path or "/"
        if path != "/": path = path.rstrip("/")
        return p._replace(scheme=scheme, netloc=netloc, path=path, fragment="").geturl()
    except Exception:
        return url


def is_html_content(content_type: str) -> bool:
    c = (content_type or "").lower()
    return "text/html" in c or "application/xhtml+xml" in c


def is_probably_html(text: str) -> bool:
    s = text[:2000].lstrip().lower()
    return any(x in s for x in ("<!doctype html", "<html", "<head", "<body"))

GENERIC_HUB_SEGMENTS = {"search", "tag", "tags", "category", "categories", "topic", "topics", "author", "authors", "archive", "archives", "feed", "rss"}
KNOWN_HUB_RULES = {"reuters.com": ("/", "/technology", "/world", "/business", "/markets", "/sports", "/lifestyle", "/politics", "/topics"), "techcrunch.com": ("/", "/category", "/tag", "/topics"), "news.google.com": ("/", "/topics", "/search")}


def is_obvious_hub(url: str) -> bool:
    try:
        p = urlparse(url); host = (p.hostname or "").lower().removeprefix("www."); path = p.path.rstrip("/") or "/"
        for h, prefixes in KNOWN_HUB_RULES.items():
            if host == h and any(path == x or (x != "/" and path.startswith(x.rstrip("/") + "/")) for x in prefixes): return True
        parts = [x.lower() for x in path.split("/") if x]
        return bool(parts and parts[0] in GENERIC_HUB_SEGMENTS) or path in {"/search", "/feed", "/rss", "/sitemap.xml"}
    except Exception:
        return True


def normalise_result(url: str, title: str, snippet: str) -> dict[str, Any] | None:
    url = canonical_url(url); title = clean_text(title); snippet = clean_text(snippet)
    if not url or not title or not is_url_safe(url) or is_obvious_hub(url): return None
    return {"url": url, "title": title, "snippet": snippet}


class BrowserExecutor:
    """Single owner thread for all Camoufox/Playwright objects."""
    def __init__(self) -> None:
        self._tasks: queue.PriorityQueue[tuple[int, int, Callable[[Any], Any], Future[Any], str, float]] = queue.PriorityQueue(maxsize=BROWSER_QUEUE_SIZE)
        self._sequence = 0
        self._sequence_lock = threading.Lock()
        self._stop = threading.Event(); self._ready = threading.Event(); self._startup_error: BaseException | None = None
        self._thread = threading.Thread(target=self._run, name="camoufox-owner", daemon=True); self._thread.start()
        if not self._ready.wait(60): raise RuntimeError("Timed out waiting for Camoufox startup")
        if self._startup_error: raise RuntimeError(f"Camoufox startup failed: {self._startup_error}")

    def _run(self) -> None:
        try:
            with Camoufox(headless=True, locale="en-GB") as browser:
                self._ready.set()
                while not self._stop.is_set() or not self._tasks.empty():
                    try: priority, _, task, future, label, queued = self._tasks.get(timeout=0.25)
                    except queue.Empty: continue
                    if future.cancelled(): self._tasks.task_done(); continue
                    started = time.perf_counter()
                    logger.info("BROWSER start label=%s wait=%.3fs q=%d", label, started - queued, self._tasks.qsize())
                    try:
                        result = task(browser)
                    except BaseException as exc:
                        if not future.cancelled(): future.set_exception(exc)
                        logger.warning("BROWSER failed label=%s exec=%.3fs error=%s", label, time.perf_counter() - started, type(exc).__name__)
                    else:
                        if not future.cancelled(): future.set_result(result)
                        logger.info("BROWSER done label=%s exec=%.3fs total=%.3fs", label, time.perf_counter() - started, time.perf_counter() - queued)
                    finally: self._tasks.task_done()
        except BaseException as exc:
            self._startup_error = exc; self._ready.set()
            while True:
                try: _, _, _, future, _, _ = self._tasks.get_nowait()
                except queue.Empty: break
                if not future.done(): future.set_exception(exc)
                self._tasks.task_done()

    def call(self, task: Callable[[Any], Any], timeout: float, label: str) -> Any:
        future: Future[Any] = Future(); queued = time.perf_counter()
        priority = 0 if label.startswith("search:") else 1
        with self._sequence_lock:
            sequence = self._sequence
            self._sequence += 1
        try:
            self._tasks.put_nowait((priority, sequence, task, future, label, queued))
        except queue.Full:
            raise RuntimeError("Browser queue is full")
        logger.info("BROWSER queued label=%s q=%d", label, self._tasks.qsize())
        try:
            return future.result(timeout=timeout)
        except FutureTimeoutError:
            # Cancelling succeeds if the task has not been taken by the owner.
            # If it is already running, the owner continues only until the
            # Playwright operation's own bounded timeout returns.
            cancelled = future.cancel()
            logger.warning("BROWSER caller_timeout label=%s cancelled=%s elapsed=%.3fs", label, cancelled, time.perf_counter() - queued)
            raise

    def shutdown(self) -> None:
        self._stop.set(); self._thread.join(timeout=BROWSER_TIMEOUT + 5)

browser_executor: BrowserExecutor | None = None
SEARCH_LOCK = threading.Lock()
ENHANCE_SEMAPHORE = threading.BoundedSemaphore(ENHANCE_GLOBAL_CONCURRENCY)


def _search_ddg(browser: Any, query: str) -> list[dict[str, Any]]:
    page = browser.new_page()
    try:
        page.goto("https://html.duckduckgo.com/html/?q=" + quote(query), wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT_MS)
        page.wait_for_timeout(BROWSER_SETTLE_MS)
        rows = page.evaluate("""() => Array.from(document.querySelectorAll('.result')).map(el => { const a=el.querySelector('a.result__a'); const s=el.querySelector('.result__snippet'); return {url:a?.href||'',title:a?.textContent||'',snippet:s?.textContent||''}; })""")
        out=[]; seen=set()
        for row in rows:
            item=normalise_result(decode_ddg_url(row.get("url","")), row.get("title",""), row.get("snippet",""))
            if item and item["url"] not in seen:
                seen.add(item["url"]); out.append(item)
                if len(out)>=MAX_CANDIDATES: break
        return out
    finally: page.close()


def _search_brave(browser: Any, query: str) -> list[dict[str, Any]]:
    page=browser.new_page()
    try:
        page.goto("https://search.brave.com/search?q=" + quote(query), wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT_MS)
        page.wait_for_timeout(BROWSER_SETTLE_MS)
        rows=page.evaluate("""() => Array.from(document.querySelectorAll('.snippet, [data-type="search-result"], .snippet-content')).map(el => { const a=el.querySelector('a.result-header, a[href]'); const s=el.querySelector('.snippet-description, .snippet-description-container, [data-snippet]'); return {url:a?.href||'',title:a?.textContent||'',snippet:s?.textContent||''}; })""")
        out=[]; seen=set()
        for row in rows:
            item=normalise_result(row.get("url",""),row.get("title",""),row.get("snippet",""))
            if item and item["url"] not in seen:
                seen.add(item["url"]); out.append(item)
                if len(out)>=MAX_CANDIDATES: break
        return out
    finally: page.close()


def search(query: str) -> list[dict[str, Any]]:
    if browser_executor is None: raise RuntimeError("Browser executor is not running")
    logger.info("SEARCH start query=%r", query)
    try:
        result=browser_executor.call(lambda b:_search_ddg(b,query),30,"search:ddg")
        if result: return result
    except Exception as exc: logger.warning("DDG search failed: %s", exc)
    try:
        return browser_executor.call(lambda b:_search_brave(b,query),30,"search:brave")
    except Exception as exc:
        logger.warning("Brave search failed: %s", exc); return []

READABILITY_SOURCE: str | None = None
READABILITY_LOCK = threading.Lock()

def get_readability_source() -> str:
    global READABILITY_SOURCE
    if READABILITY_SOURCE is None:
        with READABILITY_LOCK:
            if READABILITY_SOURCE is None: READABILITY_SOURCE = READABILITY_JS_PATH.read_text(encoding="utf-8")
    return READABILITY_SOURCE


def html_to_text(source: str) -> str:
    source=re.sub(r"<(script|style|noscript)\b[^>]*>.*?</\1>"," ",source,flags=re.I|re.S)
    return clean_text(re.sub(r"<[^>]+>"," ",source))



def _readability_on_page(page: Any, html_source: str | None = None) -> tuple[str, str]:
    if html_source is not None:
        page.set_content(html_source, wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT_MS)
    result = page.evaluate(
        """(source) => {
            eval(source);
            const article = new Readability(document).parse();
            return article ? {
                title: article.title || '',
                text: article.textContent || ''
            } : null;
        }""",
        get_readability_source(),
    )
    if not result:
        return "", ""
    return clean_text(result.get("title")), clean_text(result.get("text"))


def extract_readability(html_source: str) -> tuple[str, str]:
    if browser_executor is None:
        raise RuntimeError("Browser executor is not running")

    def task(browser: Any) -> tuple[str, str]:
        page = browser.new_page()
        try:
            return _readability_on_page(page, html_source)
        finally:
            page.close()

    return browser_executor.call(task, BROWSER_TIMEOUT, "article:readability")


def fetch_http(url: str) -> dict[str, Any]:
    started = time.perf_counter()
    logger.info("HTTP start url=%s", url)
    try:
        if not is_url_safe(url):
            return {"ok": False, "reason": "unsafe_url"}
        with httpx.Client(
            follow_redirects=False, timeout=HTTP_TIMEOUT,
            headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5"},
        ) as client:
            current = url
            for _ in range(MAX_REDIRECTS + 1):
                with client.stream("GET", current) as response:
                    if response.is_redirect:
                        location = response.headers.get("location")
                        if not location:
                            return {"ok": False, "reason": "redirect_without_location"}
                        current = urljoin(str(response.url), location)
                        if not is_url_safe(current):
                            return {"ok": False, "reason": "unsafe_redirect"}
                        continue
                    if response.status_code >= 400:
                        return {"ok": False, "reason": f"http_{response.status_code}"}
                    content_type = response.headers.get("content-type", "")
                    if not is_html_content(content_type):
                        # Some sites omit Content-Type; collect a small prefix to
                        # determine whether the response is plausibly HTML.
                        prefix = b""
                        for chunk in response.iter_bytes(4096):
                            prefix += chunk
                            if len(prefix) >= 4096:
                                break
                        if not is_probably_html(prefix.decode("utf-8", errors="ignore")):
                            return {"ok": False, "reason": "not_html"}
                        chunks = [prefix]
                        total = len(prefix)
                        for chunk in response.iter_bytes(65536):
                            total += len(chunk)
                            if total > MAX_CONTENT_BYTES:
                                return {"ok": False, "reason": "content_too_large"}
                            chunks.append(chunk)
                    else:
                        chunks = []
                        total = 0
                        for chunk in response.iter_bytes(65536):
                            total += len(chunk)
                            if total > MAX_CONTENT_BYTES:
                                return {"ok": False, "reason": "content_too_large"}
                            chunks.append(chunk)
                    body = b"".join(chunks)
                    encoding = response.encoding or "utf-8"
                    source = body.decode(encoding, errors="replace")
                    title, text = extract_readability(source)
                    if len(text) < MIN_ARTICLE_CHARS:
                        return {"ok": False, "reason": "article_too_short"}
                    return {"ok": True, "title": title, "text": text, "source": "http", "elapsed": time.perf_counter() - started}
            return {"ok": False, "reason": "too_many_redirects"}
    except (httpx.HTTPError, UnicodeError, ValueError, OSError) as exc:
        return {"ok": False, "reason": type(exc).__name__}
    finally:
        logger.info("HTTP end url=%s elapsed=%.3fs", url, time.perf_counter() - started)


def fetch_browser(url: str) -> dict[str, Any]:
    if browser_executor is None: return {"ok":False,"reason":"browser_unavailable"}
    def task(browser: Any) -> dict[str,Any]:
        page=browser.new_page()
        try:
            page.route("**/*", lambda route: route.abort() if (urlparse(route.request.url).scheme in {"http", "https"} and not is_url_safe(route.request.url)) else route.continue_())
            page.goto(url,wait_until="domcontentloaded",timeout=SEARCH_TIMEOUT_MS)
            page.wait_for_timeout(BROWSER_SETTLE_MS)
            title,text=_readability_on_page(page)
        finally: page.close()
        if len(text)<MIN_ARTICLE_CHARS: return {"ok":False,"reason":"browser_article_too_short"}
        return {"ok":True,"title":title,"text":text,"source":"browser"}
    try:
        return browser_executor.call(task,BROWSER_TIMEOUT,"article:camoufox")
    except FutureTimeoutError: return {"ok":False,"reason":"browser_timeout"}
    except Exception as exc: return {"ok":False,"reason":type(exc).__name__}


def summarise(text: str) -> str:
    text=text[:MAX_ARTICLE_TEXT_BYTES]
    if len(text)<=MAX_SUMMARY_CHARS: return text
    try:
        parser=PlaintextParser.from_string(text,Tokenizer("english"))
        sentences=LexRankSummarizer()(parser.document,SUMMARY_SENTENCES)
        summary=clean_text(" ".join(str(s) for s in sentences))
        return summary[:MAX_SUMMARY_CHARS] or text[:MAX_SUMMARY_CHARS]
    except Exception:
        return text[:MAX_SUMMARY_CHARS]


def enhance_one(candidate: dict[str, Any]) -> dict[str, Any] | None:
    started = time.perf_counter()
    url = candidate["url"]
    logger.info("CANDIDATE start url=%s", url)
    acquired = ENHANCE_SEMAPHORE.acquire(timeout=HTTP_TIMEOUT + BROWSER_TIMEOUT + 5)
    if not acquired:
        logger.warning("CANDIDATE skipped_global_capacity url=%s", url)
        return None
    try:
        http = fetch_http(url)
        if http.get("ok"):
            return {
                **candidate,
                "title": http.get("title") or candidate["title"],
                "summary": summarise(http["text"]),
                "content_source": "article_http",
            }
        browser = fetch_browser(url)
        if browser.get("ok"):
            return {
                **candidate,
                "title": browser.get("title") or candidate["title"],
                "summary": summarise(browser["text"]),
                "content_source": "article_browser",
            }
        logger.info(
            "CANDIDATE unusable url=%s http=%s browser=%s total=%.3fs",
            url, http.get("reason"), browser.get("reason"),
            time.perf_counter() - started,
        )
        return None
    finally:
        ENHANCE_SEMAPHORE.release()


def enhance_candidates(candidates: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if not candidates or limit <= 0:
        return []
    started = time.perf_counter()
    results: list[dict[str, Any]] = []
    executor = ThreadPoolExecutor(max_workers=HTTP_WORKERS, thread_name_prefix="enhance")
    pending: set[Future[dict[str, Any] | None]] = set()
    future_candidates: dict[Future[dict[str, Any] | None], tuple[int, dict[str, Any]]] = {}
    result_order: dict[str, int] = {}
    next_index = 0
    try:
        while next_index < len(candidates) and len(pending) < HTTP_WORKERS:
            candidate = candidates[next_index]
            future = executor.submit(enhance_one, candidate)
            pending.add(future)
            future_candidates[future] = (next_index, candidate)
            next_index += 1

        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                try:
                    result = future.result()
                except Exception as exc:
                    _, candidate = future_candidates.get(future, (0, {}))
                    logger.warning(
                        "candidate worker failed url=%s error=%s",
                        candidate.get("url", ""), type(exc).__name__,
                    )
                    result = None
                finally:
                    index, _ = future_candidates.pop(future, (0, {}))
                if result:
                    results.append(result)
                    result_order[result["url"]] = index

            if len(results) >= min(limit, TARGET_USABLE_ARTICLES):
                break

            while next_index < len(candidates) and len(pending) < HTTP_WORKERS:
                candidate = candidates[next_index]
                future = executor.submit(enhance_one, candidate)
                pending.add(future)
                future_candidates[future] = (next_index, candidate)
                next_index += 1
    finally:
        for future in pending:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)

    usable_urls = {r["url"] for r in results}
    results.sort(key=lambda r: result_order.get(r["url"], len(candidates)))
    ordered = results[:limit]
    if len(ordered) < limit:
        for candidate in candidates:
            if candidate["url"] in usable_urls:
                continue
            ordered.append({
                **candidate,
                "summary": candidate["snippet"],
                "content_source": "search_snippet",
            })
            if len(ordered) >= limit:
                break

    logger.info(
        "ENHANCE done elapsed=%.3fs usable=%d returned=%d",
        time.perf_counter() - started, len(results), len(ordered),
    )
    return ordered[:limit]


def _json(handler: http.server.BaseHTTPRequestHandler, payload: Any, status: int=200) -> None:
    body=json.dumps(payload,ensure_ascii=False,separators=(",",":")).encode("utf-8")
    handler.send_response(status); handler.send_header("Content-Type","application/json; charset=utf-8"); handler.send_header("Content-Length",str(len(body))); handler.send_header("Cache-Control","no-store"); handler.end_headers(); handler.wfile.write(body)


REQUEST_IDS=0
REQUEST_LOCK=threading.Lock()

def next_request_id()->int:
    global REQUEST_IDS
    with REQUEST_LOCK: REQUEST_IDS+=1; return REQUEST_IDS


class SearchHandler(http.server.BaseHTTPRequestHandler):
    server_version="SearchService/5.0"
    def log_message(self,format:str,*args:Any)->None: logger.info("HTTP %s",format%args)
    def do_GET(self)->None:
        request_id=next_request_id(); started=time.perf_counter()
        try:
            parsed=urlparse(self.path)
            if parsed.path=="/health":
                _json(self,{"ok":True,"version":VERSION}); return
            if parsed.path!="/search": _json(self,{"error":"not_found"},404); return
            qs=parse_qs(parsed.query); query=clean_text(qs.get("q",[""])[0])
            if not query: _json(self,{"error":"missing q"},400); return
            try: limit=min(max(int(qs.get("limit",[DEFAULT_LIMIT])[0]),1),MAX_LIMIT)
            except ValueError: _json(self,{"error":"invalid limit"},400); return
            logger.info("REQUEST start id=%d query=%r limit=%d",request_id,query,limit)
            search_started=time.perf_counter()
            with SEARCH_LOCK: candidates=search(query)
            logger.info("REQUEST search_end id=%d elapsed=%.3fs candidates=%d",request_id,time.perf_counter()-search_started,len(candidates))
            results=enhance_candidates(candidates,limit)
            logger.info("REQUEST enhance_end id=%d total=%.3fs results=%d",request_id,time.perf_counter()-search_started,len(results))
            _json(self,{"query":query,"results":results,"metadata":{"version":VERSION,"elapsed":round(time.perf_counter()-started,3),"candidates":len(candidates)}})
            logger.info("REQUEST response_sent id=%d total=%.3fs",request_id,time.perf_counter()-started)
        except (BrokenPipeError, ConnectionResetError):
            logger.info("REQUEST client_disconnected id=%d total=%.3fs",request_id,time.perf_counter()-started)
        except Exception as exc:
            logger.exception("REQUEST failed id=%d total=%.3fs",request_id,time.perf_counter()-started)
            try: _json(self,{"error":str(exc)},500)
            except (BrokenPipeError,ConnectionResetError): pass


def main()->None:
    global browser_executor
    browser_executor=BrowserExecutor()
    server=http.server.ThreadingHTTPServer((HOST,PORT),SearchHandler)
    server.daemon_threads=True
    logger.info("starting search_service version=%s host=%s port=%d",VERSION,HOST,PORT)
    try: server.serve_forever()
    except KeyboardInterrupt: logger.info("shutdown requested")
    finally:
        server.shutdown(); server.server_close()
        if browser_executor is not None: browser_executor.shutdown()


if __name__=="__main__": main()
