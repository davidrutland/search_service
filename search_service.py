#!/usr/bin/env python3
"""Small, bounded-concurrency web retrieval service for a local LLM.

Pipeline: DDG -> Brave fallback -> URL validation/dedupe -> HTTP+local Readability
-> short extractive enhancement appended to snippets -> JSON results.

Camoufox is a single-thread-owned scarce resource used for search only.
Article retrieval never falls back to Camoufox; failed HTTP enhancement preserves
the original search-engine snippet unchanged.
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
import sys
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError as FutureTimeoutError, wait, FIRST_COMPLETED
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import httpx
from camoufox.sync_api import Camoufox
try:
    from readability import Document
except ImportError:
    Document = None  # type: ignore[assignment,misc]
from sumy.nlp.tokenizers import Tokenizer
from sumy.parsers.plaintext import PlaintextParser

HOST = "0.0.0.0"
PORT = 8787
DEFAULT_LIMIT = 10
MAX_LIMIT = 20
MAX_CANDIDATES = 5
HTTP_TIMEOUT = 6.0
BROWSER_TIMEOUT = 5.0
SEARCH_TIMEOUT_MS = 6500
BROWSER_SETTLE_MS = 250
REQUEST_DEADLINE = 12.0
ENHANCE_BUDGET = 7.0
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
LOG_DIR = SCRIPT_DIR / "logs"
VERSION = "7.0-fast"

logger = logging.getLogger("search_service")
logger.setLevel(logging.INFO)
logger.propagate = False
LOG_DIR.mkdir(parents=True, exist_ok=True)

_handler = logging.handlers.RotatingFileHandler(
    LOG_DIR / "search_service.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8",
)
_formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(threadName)s %(message)s"
)
_handler.setFormatter(_formatter)
logger.addHandler(_handler)

#_stdout_handler = logging.StreamHandler(sys.stdout)
#_stdout_handler.setFormatter(_formatter)
#logger.addHandler(_stdout_handler)


BLOCKED_SUBNETS = tuple(
    ipaddress.ip_network(x)
    for x in (
        "0.0.0.0/8",
        "10.0.0.0/8",
        "100.64.0.0/10",
        "127.0.0.0/8",
        "169.254.0.0/16",
        "172.16.0.0/12",
        "192.0.0.0/24",
        "192.0.2.0/24",
        "192.168.0.0/16",
        "198.18.0.0/15",
        "198.51.100.0/24",
        "203.0.113.0/24",
        "224.0.0.0/4",
        "240.0.0.0/4",
    )
)

BLOCKED_IPV6_SUBNETS = tuple(
    ipaddress.ip_network(x)
    for x in (
        "::/128",
        "::1/128",
        "::ffff:0:0/96",
        "fc00::/7",
        "fe80::/10",
        "ff00::/8",
        "2001:db8::/32",
    )
)


def _blocked_ip(
    address: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> bool:
    nets = BLOCKED_SUBNETS if address.version == 4 else BLOCKED_IPV6_SUBNETS
    return any(address in n for n in nets) or not address.is_global


def _resolve_host(
    hostname: str,
) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    try:
        infos = socket.getaddrinfo(
            hostname,
            None,
            socket.AF_UNSPEC,
            socket.SOCK_STREAM,
        )
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
            seen.add(str(addr))
            out.append(addr)

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
    return "" if value is None else re.sub(
        r"\s+",
        " ",
        html.unescape(str(value)),
    ).strip()


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
        scheme = p.scheme.lower()
        host = (p.hostname or "").lower().removeprefix("www.")
        port = p.port

        netloc = (
            host
            if port in (None, 80 if scheme == "http" else 443)
            else f"{host}:{port}"
        )

        path = p.path or "/"
        if path != "/":
            path = path.rstrip("/")

        return p._replace(
            scheme=scheme,
            netloc=netloc,
            path=path,
            fragment="",
        ).geturl()

    except Exception:
        return url


def is_html_content(content_type: str) -> bool:
    c = (content_type or "").lower()
    return "text/html" in c or "application/xhtml+xml" in c


def is_probably_html(text: str) -> bool:
    s = text[:2000].lstrip().lower()
    return any(
        x in s
        for x in (
            "<!doctype html",
            "<html",
            "<head",
            "<body",
        )
    )


GENERIC_HUB_SEGMENTS = {
    "search",
    "tag",
    "tags",
    "category",
    "categories",
    "topic",
    "topics",
    "author",
    "authors",
    "archive",
    "archives",
    "feed",
    "rss",
}

KNOWN_HUB_RULES = {
    "reuters.com": (
        "/",
        "/technology",
        "/world",
        "/business",
        "/markets",
        "/sports",
        "/lifestyle",
        "/politics",
        "/topics",
    ),
    "techcrunch.com": (
        "/",
        "/category",
        "/tag",
        "/topics",
    ),
    "news.google.com": (
        "/",
        "/topics",
        "/search",
    ),
}


def is_obvious_hub(url: str) -> bool:
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower().removeprefix("www.")
        path = p.path.rstrip("/") or "/"

        for h, prefixes in KNOWN_HUB_RULES.items():
            if host == h and any(
                path == x
                or (x != "/" and path.startswith(x.rstrip("/") + "/"))
                for x in prefixes
            ):
                return True

        parts = [x.lower() for x in path.split("/") if x]

        return bool(parts and parts[0] in GENERIC_HUB_SEGMENTS) or path in {
            "/search",
            "/feed",
            "/rss",
            "/sitemap.xml",
        }

    except Exception:
        return True


def normalise_result(
    url: str,
    title: str,
    snippet: str,
) -> dict[str, Any] | None:
    url = canonical_url(url)
    title = clean_text(title)
    snippet = clean_text(snippet)

    if (
        not url
        or not title
        or not is_url_safe(url)
        or is_obvious_hub(url)
    ):
        return None

    return {
        "url": url,
        "title": title,
        "snippet": snippet,
    }


class BrowserExecutor:
    """Single owner thread for all Camoufox/Playwright objects."""

    def __init__(self) -> None:
        self._tasks: queue.PriorityQueue[
            tuple[
                int,
                int,
                Callable[[Any], Any],
                Future[Any],
                str,
                float,
                float,
            ]
        ] = queue.PriorityQueue(maxsize=BROWSER_QUEUE_SIZE)

        self._sequence = 0
        self._sequence_lock = threading.Lock()

        self._stop = threading.Event()
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None

        self._thread = threading.Thread(
            target=self._run,
            name="camoufox-owner",
            daemon=True,
        )
        self._thread.start()

        if not self._ready.wait(60):
            raise RuntimeError("Timed out waiting for Camoufox startup")

        if self._startup_error:
            raise RuntimeError(
                f"Camoufox startup failed: {self._startup_error}"
            )

    def _run(self) -> None:
        try:
            with Camoufox(headless=True, locale="en-GB") as browser:
                self._ready.set()

                while not self._stop.is_set() or not self._tasks.empty():
                    try:
                        (
                            priority,
                            _,
                            task,
                            future,
                            label,
                            queued,
                            deadline,
                        ) = self._tasks.get(timeout=0.25)
                    except queue.Empty:
                        continue

                    if (
                        future.cancelled()
                        or time.perf_counter() >= deadline
                    ):
                        if not future.done():
                            future.cancel()

                        logger.info(
                            "BROWSER expired label=%s wait=%.3fs",
                            label,
                            time.perf_counter() - queued,
                        )

                        self._tasks.task_done()
                        continue

                    started = time.perf_counter()

                    logger.info(
                        "BROWSER start label=%s wait=%.3fs q=%d",
                        label,
                        started - queued,
                        self._tasks.qsize(),
                    )

                    try:
                        result = task(browser)

                    except BaseException as exc:
                        if not future.cancelled():
                            future.set_exception(exc)

                        logger.warning(
                            "BROWSER failed label=%s exec=%.3fs error=%s",
                            label,
                            time.perf_counter() - started,
                            type(exc).__name__,
                        )

                    else:
                        if not future.cancelled():
                            future.set_result(result)

                        logger.info(
                            "BROWSER done label=%s exec=%.3fs total=%.3fs",
                            label,
                            time.perf_counter() - started,
                            time.perf_counter() - queued,
                        )

                    finally:
                        self._tasks.task_done()

        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()

            while True:
                try:
                    (
                        _,
                        _,
                        _,
                        future,
                        _,
                        _,
                        _,
                    ) = self._tasks.get_nowait()
                except queue.Empty:
                    break

                if not future.done():
                    future.set_exception(exc)

                self._tasks.task_done()

    def call(
        self,
        task: Callable[[Any], Any],
        timeout: float,
        label: str,
    ) -> Any:
        future: Future[Any] = Future()
        queued = time.perf_counter()
        deadline = queued + max(0.0, timeout)

        if timeout <= 0:
            future.cancel()
            raise FutureTimeoutError()

        priority = 0 if label.startswith("search:") else 1

        with self._sequence_lock:
            sequence = self._sequence
            self._sequence += 1

        try:
            self._tasks.put_nowait(
                (
                    priority,
                    sequence,
                    task,
                    future,
                    label,
                    queued,
                    deadline,
                )
            )
        except queue.Full:
            raise RuntimeError("Browser queue is full")

        logger.info(
            "BROWSER queued label=%s q=%d",
            label,
            self._tasks.qsize(),
        )

        try:
            return future.result(timeout=timeout)

        except FutureTimeoutError:
            # Cancelling succeeds if the task has not been taken by the owner.
            # If it is already running, the owner continues only until the
            # Playwright operation's own bounded timeout returns.
            cancelled = future.cancel()

            logger.warning(
                "BROWSER caller_timeout label=%s cancelled=%s elapsed=%.3fs",
                label,
                cancelled,
                time.perf_counter() - queued,
            )

            raise

    def shutdown(self) -> None:
        self._stop.set()
        self._thread.join(
            timeout=max(BROWSER_TIMEOUT, REQUEST_DEADLINE) + 5
        )


browser_executor: BrowserExecutor | None = None

ENHANCE_SEMAPHORE = threading.BoundedSemaphore(
    ENHANCE_GLOBAL_CONCURRENCY
)


def _search_ddg(
    browser: Any,
    query: str,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    page = browser.new_page()

    try:
        page.goto(
            "https://html.duckduckgo.com/html/?q=" + quote(query),
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )

        page.wait_for_timeout(
            min(
                BROWSER_SETTLE_MS,
                max(0, timeout_ms // 4),
            )
        )

        rows = page.evaluate(
            """() => Array.from(document.querySelectorAll('.result')).map(el => {
                const a = el.querySelector('a.result__a');
                const s = el.querySelector('.result__snippet');
                return {
                    url: a?.href || '',
                    title: a?.textContent || '',
                    snippet: s?.textContent || ''
                };
            })"""
        )

        out = []
        seen = set()

        for row in rows:
            item = normalise_result(
                decode_ddg_url(row.get("url", "")),
                row.get("title", ""),
                row.get("snippet", ""),
            )

            if item and item["url"] not in seen:
                seen.add(item["url"])
                out.append(item)

                if len(out) >= MAX_CANDIDATES:
                    break

        return out

    finally:
        page.close()


def _search_brave(
    browser: Any,
    query: str,
    timeout_ms: int,
) -> list[dict[str, Any]]:
    page = browser.new_page()

    try:
        page.goto(
            "https://search.brave.com/search?q=" + quote(query),
            wait_until="domcontentloaded",
            timeout=timeout_ms,
        )

        page.wait_for_timeout(
            min(
                BROWSER_SETTLE_MS,
                max(0, timeout_ms // 4),
            )
        )

        rows = page.evaluate(
            """() => Array.from(
                document.querySelectorAll(
                    '.snippet, [data-type="search-result"], .snippet-content'
                )
            ).map(el => {
                const a = el.querySelector(
                    'a.result-header, a[href]'
                );
                const s = el.querySelector(
                    '.snippet-description, .snippet-description-container, [data-snippet]'
                );
                return {
                    url: a?.href || '',
                    title: a?.textContent || '',
                    snippet: s?.textContent || ''
                };
            })"""
        )

        out = []
        seen = set()

        for row in rows:
            item = normalise_result(
                row.get("url", ""),
                row.get("title", ""),
                row.get("snippet", ""),
            )

            if item and item["url"] not in seen:
                seen.add(item["url"])
                out.append(item)

                if len(out) >= MAX_CANDIDATES:
                    break

        return out

    finally:
        page.close()


def search(
    query: str,
    deadline: float,
) -> list[dict[str, Any]]:
    if browser_executor is None:
        raise RuntimeError("Browser executor is not running")

    logger.info("SEARCH start query=%r", query)

    def remaining() -> float:
        return max(
            0.0,
            deadline - time.perf_counter(),
        )

    for engine, task_factory in (
        ("ddg", _search_ddg),
        ("brave", _search_brave),
    ):
        left = remaining()

        if left < 0.75:
            logger.warning(
                "SEARCH deadline reached before %s",
                engine,
            )
            break

        call_timeout = min(
            SEARCH_TIMEOUT_MS / 1000.0 + 0.25,
            left,
        )

        timeout_ms = max(
            250,
            int(
                max(
                    0.25,
                    call_timeout - 0.1,
                )
                * 1000
            ),
        )

        try:
            result = browser_executor.call(
                lambda b, tf=task_factory, tm=timeout_ms:
                    tf(b, query, tm),
                call_timeout,
                f"search:{engine}",
            )

            if result:
                logger.info(
                    "SEARCH RESULT POOL engine=%s count=%d",
                    engine,
                    len(result),
                )

                for index, item in enumerate(result, 1):
                    logger.info(
                        "SEARCH RESULT index=%d title=%r url=%s snippet=%r",
                        index,
                        item.get("title", ""),
                        item.get("url", ""),
                        item.get("snippet", ""),
                    )

                return result

            logger.warning(
                "SEARCH RESULT POOL engine=%s count=0",
                engine,
            )

        except Exception as exc:
            logger.warning(
                "%s search failed error=%s",
                engine.upper(),
                type(exc).__name__,
            )

    return []


def html_to_text(source: str) -> str:
    source = re.sub(
        r"<(script|style|noscript)\b[^>]*>.*?</\1>",
        " ",
        source,
        flags=re.I | re.S,
    )

    return clean_text(
        re.sub(
            r"<[^>]+>",
            " ",
            source,
        )
    )


def extract_readability(
    html_source: str,
) -> tuple[str, str]:
    """Extract an article locally from already-fetched HTML.

    This deliberately does not consume the shared Camoufox executor.
    """

    if Document is None:
        raise RuntimeError(
            "local Readability unavailable; install readability-lxml"
        )

    document = Document(html_source)

    title = clean_text(document.title())

    article_html = document.summary(
        html_partial=True
    )

    text = clean_text(
        html_to_text(article_html)
    )

    return title, text


def fetch_http(
    url: str,
    deadline: float,
) -> dict[str, Any]:
    started = time.perf_counter()

    logger.info(
        "HTTP start url=%s",
        url,
    )

    try:
        if time.perf_counter() >= deadline:
            return {
                "ok": False,
                "reason": "deadline",
            }

        if not is_url_safe(url):
            return {
                "ok": False,
                "reason": "unsafe_url",
            }

        timeout = min(
            HTTP_TIMEOUT,
            max(
                0.25,
                deadline - time.perf_counter(),
            ),
        )

        with httpx.Client(
            follow_redirects=False,
            timeout=timeout,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": (
                    "text/html,application/xhtml+xml;q=0.9,"
                    "*/*;q=0.5"
                ),
            },
        ) as client:
            current = url

            for _ in range(MAX_REDIRECTS + 1):
                request_timeout = min(
                    HTTP_TIMEOUT,
                    max(
                        0.25,
                        deadline - time.perf_counter(),
                    ),
                )

                with client.stream(
                    "GET",
                    current,
                    timeout=request_timeout,
                ) as response:

                    if time.perf_counter() >= deadline:
                        return {
                            "ok": False,
                            "reason": "deadline",
                        }

                    if response.is_redirect:
                        location = response.headers.get("location")

                        if not location:
                            return {
                                "ok": False,
                                "reason": "redirect_without_location",
                            }

                        current = urljoin(
                            str(response.url),
                            location,
                        )

                        if not is_url_safe(current):
                            return {
                                "ok": False,
                                "reason": "unsafe_redirect",
                            }

                        continue

                    if response.status_code >= 400:
                        return {
                            "ok": False,
                            "reason": f"http_{response.status_code}",
                        }

                    content_type = response.headers.get(
                        "content-type",
                        "",
                    )

                    if not is_html_content(content_type):
                        # Some sites omit Content-Type; collect a small
                        # prefix to determine whether the response is
                        # plausibly HTML.
                        prefix = b""

                        for chunk in response.iter_bytes(4096):
                            prefix += chunk

                            if len(prefix) >= 4096:
                                break

                        if not is_probably_html(
                            prefix.decode(
                                "utf-8",
                                errors="ignore",
                            )
                        ):
                            return {
                                "ok": False,
                                "reason": "not_html",
                            }

                        chunks = [prefix]
                        total = len(prefix)

                        for chunk in response.iter_bytes(65536):
                            total += len(chunk)

                            if total > MAX_CONTENT_BYTES:
                                return {
                                    "ok": False,
                                    "reason": "content_too_large",
                                }

                            chunks.append(chunk)

                    else:
                        chunks = []
                        total = 0

                        for chunk in response.iter_bytes(65536):
                            total += len(chunk)

                            if total > MAX_CONTENT_BYTES:
                                return {
                                    "ok": False,
                                    "reason": "content_too_large",
                                }

                            chunks.append(chunk)

                    body = b"".join(chunks)

                    encoding = response.encoding or "utf-8"

                    source = body.decode(
                        encoding,
                        errors="replace",
                    )

                    if time.perf_counter() >= deadline:
                        return {
                            "ok": False,
                            "reason": "deadline",
                        }

                    title, text = extract_readability(source)

                    if len(text) < MIN_ARTICLE_CHARS:
                        return {
                            "ok": False,
                            "reason": "article_too_short",
                        }

                    return {
                        "ok": True,
                        "title": title,
                        "text": text,
                        "source": "http",
                        "elapsed": (
                            time.perf_counter() - started
                        ),
                    }

            return {
                "ok": False,
                "reason": "too_many_redirects",
            }

    except (
        httpx.HTTPError,
        UnicodeError,
        ValueError,
        OSError,
        RuntimeError,
    ) as exc:
        return {
            "ok": False,
            "reason": type(exc).__name__,
        }

    finally:
        logger.info(
            "HTTP end url=%s elapsed=%.3fs",
            url,
            time.perf_counter() - started,
        )


SUMMARY_MAX_CHARS = 1200
SUMMARY_MIN_SENTENCE_CHARS = 40
SUMMARY_MAX_SENTENCE_CHARS = 500

SUMMARY_STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "being",
    "between",
    "could",
    "from",
    "have",
    "into",
    "more",
    "most",
    "other",
    "over",
    "said",
    "some",
    "such",
    "than",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "under",
    "were",
    "which",
    "while",
    "with",
    "would",
    "your",
}


def _summary_terms(query: str) -> set[str]:
    words = re.findall(
        r"[A-Za-z0-9][A-Za-z0-9'’-]{2,}",
        clean_text(query).lower(),
    )

    return {
        w.strip("'’")
        for w in words
        if w.strip("'’") not in SUMMARY_STOPWORDS
    }


def summarise(
    text: str,
    query: str = "",
    title: str = "",
) -> str:
    """Return a compact, query-aware extractive summary.

    The search snippet remains the explicit fallback when enhancement
    fails. This function only handles successful article extraction.
    """

    text = clean_text(
        text[:MAX_ARTICLE_TEXT_BYTES]
    )

    if not text:
        return ""

    if len(text) <= SUMMARY_MIN_SENTENCE_CHARS:
        return text[:SUMMARY_MAX_CHARS]

    try:
        parser = PlaintextParser.from_string(
            text,
            Tokenizer("english"),
        )

        sentences = [
            clean_text(str(s))
            for s in parser.document.sentences
        ]

        sentences = [
            s
            for s in sentences
            if (
                SUMMARY_MIN_SENTENCE_CHARS
                <= len(s)
                <= SUMMARY_MAX_SENTENCE_CHARS
            )
        ]

        if not sentences:
            return text[:SUMMARY_MAX_CHARS]

        query_terms = _summary_terms(query)
        title_terms = _summary_terms(title)

        def words(value: str) -> list[str]:
            return re.findall(
                r"[A-Za-z0-9][A-Za-z0-9'’-]{2,}",
                value.lower(),
            )

        def score(
            sentence: str,
            index: int,
        ) -> float:
            tokens = words(sentence)
            token_set = set(tokens)

            if not tokens:
                return -1.0

            value = (
                5.0
                * len(token_set & query_terms)
            )

            value += (
                2.0
                * len(token_set & title_terms)
            )

            query_clean = clean_text(
                query
            ).lower()

            if (
                query_clean
                and query_clean in sentence.lower()
            ):
                value += 4.0

            value += min(
                len(tokens),
                35,
            ) / 35.0

            value += (
                max(
                    0.0,
                    1.0
                    - (
                        index
                        / max(
                            len(sentences),
                            1,
                        )
                    ),
                )
                * 1.5
            )

            lower = sentence.lower()

            if any(
                marker in lower
                for marker in (
                    "cookie",
                    "subscribe",
                    "sign up",
                    "privacy policy",
                    "terms of use",
                    "advertisement",
                    "all rights reserved",
                )
            ):
                value -= 8.0

            return value

        ranked = sorted(
            enumerate(sentences),
            key=lambda item: score(
                item[1],
                item[0],
            ),
            reverse=True,
        )

        selected: list[tuple[int, str]] = []
        selected_terms: set[str] = set()

        for index, sentence in ranked:
            if len(selected) >= SUMMARY_SENTENCES:
                break

            token_set = set(
                words(sentence)
            )

            if (
                selected
                and token_set
                and token_set <= selected_terms
            ):
                continue

            selected.append(
                (
                    index,
                    sentence,
                )
            )

            selected_terms.update(
                token_set
            )

        if not selected:
            return text[:SUMMARY_MAX_CHARS]

        selected.sort(
            key=lambda item: item[0]
        )

        output: list[str] = []
        total = 0

        for _, sentence in selected:
            extra = len(sentence) + (
                1 if output else 0
            )

            if (
                total + extra
                > SUMMARY_MAX_CHARS
            ):
                break

            output.append(sentence)
            total += extra

        summary = clean_text(
            " ".join(output)
        )

        return (
            summary[:SUMMARY_MAX_CHARS]
            or text[:SUMMARY_MAX_CHARS]
        )

    except Exception as exc:
        logger.warning(
            "SUMMARY failed error=%s",
            type(exc).__name__,
        )

        return text[:SUMMARY_MAX_CHARS]


def enhance_one(
    candidate: dict[str, Any],
    query: str = "",
    deadline: float | None = None,
) -> dict[str, Any] | None:
    started = time.perf_counter()
    url = candidate["url"]

    if deadline is None:
        deadline = started + ENHANCE_BUDGET

    logger.info(
        "CANDIDATE start url=%s",
        url,
    )

    acquired = ENHANCE_SEMAPHORE.acquire(
        timeout=max(
            0.0,
            min(
                1.0,
                deadline - time.perf_counter(),
            ),
        )
    )

    if not acquired:
        logger.warning(
            "CANDIDATE skipped_global_capacity url=%s",
            url,
        )
        return None

    try:
        if time.perf_counter() >= deadline:
            return None

        http = fetch_http(
            url,
            deadline,
        )

        if http.get("ok"):
            logger.info(
                "CANDIDATE article_http url=%s chars=%d",
                url,
                len(http.get("text", "")),
            )

            enhanced = summarise(
                http["text"],
                query,
                http.get("title")
                or candidate["title"],
            )

            snippet = candidate["snippet"]

            if enhanced:
                snippet = (
                    f"{snippet}\n\n{enhanced}"
                    if snippet
                    else enhanced
                )

            return {
                **candidate,
                "title": (
                    http.get("title")
                    or candidate["title"]
                ),
                "snippet": snippet,
                "content_source": "article_http",
            }

        # Article enhancement is deliberately best-effort.
        # There is no browser fallback here: preserve the original
        # search-engine snippet unchanged.
        logger.info(
            "CANDIDATE HTTP unusable url=%s reason=%s; "
            "keeping search snippet",
            url,
            http.get("reason", "unknown"),
        )

        return None

    finally:
        ENHANCE_SEMAPHORE.release()


def enhance_candidates(
    candidates: list[dict[str, Any]],
    limit: int,
    query: str = "",
    deadline: float | None = None,
) -> list[dict[str, Any]]:
    if not candidates or limit <= 0:
        logger.info(
            "ENHANCEMENT CANDIDATES count=0 target=%d",
            TARGET_USABLE_ARTICLES,
        )
        return []

    started = time.perf_counter()

    if deadline is None:
        deadline = started + ENHANCE_BUDGET

    target = min(
        limit,
        TARGET_USABLE_ARTICLES,
    )

    logger.info(
        "ENHANCEMENT CANDIDATES count=%d target=%d",
        len(candidates),
        target,
    )

    for index, candidate in enumerate(
        candidates,
        1,
    ):
        logger.info(
            "ENHANCEMENT CANDIDATE index=%d title=%r url=%s snippet=%r",
            index,
            candidate.get("title", ""),
            candidate.get("url", ""),
            candidate.get("snippet", ""),
        )

    results: list[dict[str, Any]] = []

    executor = ThreadPoolExecutor(
        max_workers=HTTP_WORKERS,
        thread_name_prefix="enhance",
    )

    pending: set[
        Future[dict[str, Any] | None]
    ] = set()

    future_candidates: dict[
        Future[dict[str, Any] | None],
        tuple[int, dict[str, Any]],
    ] = {}

    result_order: dict[str, int] = {}
    next_index = 0

    try:
        while (
            next_index < len(candidates)
            and len(pending) < HTTP_WORKERS
            and time.perf_counter() < deadline
        ):
            candidate = candidates[next_index]

            future = executor.submit(
                enhance_one,
                candidate,
                query,
                deadline,
            )

            pending.add(future)
            future_candidates[future] = (
                next_index,
                candidate,
            )

            next_index += 1

        while pending:
            remaining = (
                deadline
                - time.perf_counter()
            )

            if remaining <= 0:
                break

            done, pending = wait(
                pending,
                timeout=remaining,
                return_when=FIRST_COMPLETED,
            )

            if not done:
                break

            for future in done:
                try:
                    result = future.result()

                except Exception as exc:
                    _, candidate = future_candidates.get(
                        future,
                        (0, {}),
                    )

                    logger.warning(
                        "candidate worker failed url=%s error=%s",
                        candidate.get("url", ""),
                        type(exc).__name__,
                    )

                    result = None

                finally:
                    index, _ = future_candidates.pop(
                        future,
                        (0, {}),
                    )

                if result:
                    results.append(result)
                    result_order[result["url"]] = index

            if len(results) >= target:
                break

            while (
                next_index < len(candidates)
                and len(pending) < HTTP_WORKERS
                and time.perf_counter() < deadline
            ):
                candidate = candidates[next_index]

                future = executor.submit(
                    enhance_one,
                    candidate,
                    query,
                    deadline,
                )

                pending.add(future)
                future_candidates[future] = (
                    next_index,
                    candidate,
                )

                next_index += 1

    finally:
        for future in pending:
            future.cancel()

        executor.shutdown(
            wait=False,
            cancel_futures=True,
        )

    usable_urls = {
        r["url"]
        for r in results
    }

    results.sort(
        key=lambda r: result_order.get(
            r["url"],
            len(candidates),
        )
    )

    ordered = results[:limit]

    # Anything that was not successfully enhanced is returned unchanged
    # from the search result pool.
    if len(ordered) < limit:
        for candidate in candidates:
            if candidate["url"] in usable_urls:
                continue

            ordered.append(
                {
                    **candidate,
                    "content_source": "search_snippet",
                }
            )

            if len(ordered) >= limit:
                break

    final_results = ordered[:limit]

    logger.info(
        "FINAL OUTPUT count=%d enhanced=%d snippet_fallback=%d elapsed=%.3fs",
        len(final_results),
        sum(
            1
            for r in final_results
            if r.get("content_source")
            != "search_snippet"
        ),
        sum(
            1
            for r in final_results
            if r.get("content_source")
            == "search_snippet"
        ),
        time.perf_counter() - started,
    )

    for index, result in enumerate(
        final_results,
        1,
    ):
        logger.info(
            "FINAL RESULT index=%d source=%s title=%r url=%s snippet=%r",
            index,
            result.get(
                "content_source",
                "unknown",
            ),
            result.get("title", ""),
            result.get("url", ""),
            result.get("snippet", ""),
        )

    logger.info(
        "ENHANCE done elapsed=%.3fs usable=%d returned=%d",
        time.perf_counter() - started,
        len(results),
        len(final_results),
    )

    return final_results


def _json(
    handler: http.server.BaseHTTPRequestHandler,
    payload: Any,
    status: int = 200,
) -> None:
    body = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    handler.send_response(status)
    handler.send_header(
        "Content-Type",
        "application/json; charset=utf-8",
    )
    handler.send_header(
        "Content-Length",
        str(len(body)),
    )
    handler.send_header(
        "Cache-Control",
        "no-store",
    )
    handler.end_headers()
    handler.wfile.write(body)


REQUEST_IDS = 0
REQUEST_LOCK = threading.Lock()


def next_request_id() -> int:
    global REQUEST_IDS

    with REQUEST_LOCK:
        REQUEST_IDS += 1
        return REQUEST_IDS


class SearchHandler(
    http.server.BaseHTTPRequestHandler
):
    server_version = "SearchService/7.0-fast"

    def log_message(
        self,
        format: str,
        *args: Any,
    ) -> None:
        logger.info(
            "HTTP %s",
            format % args,
        )

    def do_GET(self) -> None:
        request_id = next_request_id()
        started = time.perf_counter()

        try:
            parsed = urlparse(self.path)

            if parsed.path == "/health":
                _json(
                    self,
                    {
                        "ok": True,
                        "version": VERSION,
                    },
                )
                return

            if parsed.path != "/search":
                _json(
                    self,
                    {"error": "not_found"},
                    404,
                )
                return

            qs = parse_qs(parsed.query)
            query = clean_text(
                qs.get("q", [""])[0]
            )

            if not query:
                _json(
                    self,
                    {"error": "missing q"},
                    400,
                )
                return

            try:
                limit = min(
                    max(
                        int(
                            qs.get(
                                "limit",
                                [DEFAULT_LIMIT],
                            )[0]
                        ),
                        1,
                    ),
                    MAX_LIMIT,
                )

            except ValueError:
                _json(
                    self,
                    {"error": "invalid limit"},
                    400,
                )
                return

            logger.info(
                "REQUEST start id=%d query=%r limit=%d",
                request_id,
                query,
                limit,
            )

            search_started = time.perf_counter()

            deadline = (
                started
                + REQUEST_DEADLINE
            )

            candidates = search(
                query,
                deadline,
            )

            logger.info(
                "REQUEST SEARCH RESULT POOL id=%d count=%d elapsed=%.3fs",
                request_id,
                len(candidates),
                time.perf_counter()
                - search_started,
            )

            enhancement_candidates = candidates[
                :MAX_CANDIDATES
            ]

            logger.info(
                "REQUEST ENHANCEMENT CANDIDATES id=%d count=%d target=%d",
                request_id,
                len(enhancement_candidates),
                min(
                    limit,
                    TARGET_USABLE_ARTICLES,
                ),
            )

            results = enhance_candidates(
                enhancement_candidates,
                limit,
                query,
                min(
                    deadline,
                    time.perf_counter()
                    + ENHANCE_BUDGET,
                ),
            )

            logger.info(
                "REQUEST FINAL OUTPUT id=%d count=%d total=%.3fs",
                request_id,
                len(results),
                time.perf_counter()
                - search_started,
            )

            source_counts = {}

            logger.info(
                "REQUEST FINAL OUTPUT DETAILS id=%d count=%d",
                request_id,
                len(results),
            )

            for index, result in enumerate(
                results,
                1,
            ):
                logger.info(
                    "REQUEST FINAL RESULT id=%d index=%d source=%s title=%r url=%s",
                    request_id,
                    index,
                    result.get(
                        "content_source",
                        "unknown",
                    ),
                    result.get("title", ""),
                    result.get("url", ""),
                )

            for result in results:
                source = result.get(
                    "content_source",
                    "unknown",
                )

                source_counts[source] = (
                    source_counts.get(
                        source,
                        0,
                    )
                    + 1
                )

            logger.info(
                "REQUEST sources id=%d sources=%s",
                request_id,
                source_counts,
            )

            _json(
                self,
                {
                    "query": query,
                    "results": results,
                    "metadata": {
                        "version": VERSION,
                        "elapsed": round(
                            time.perf_counter()
                            - started,
                            3,
                        ),
                        "candidates": len(candidates),
                        "content_sources": source_counts,
                    },
                },
            )

            logger.info(
                "REQUEST response_sent id=%d total=%.3fs",
                request_id,
                time.perf_counter()
                - started,
            )

        except (
            BrokenPipeError,
            ConnectionResetError,
        ):
            logger.info(
                "REQUEST client_disconnected id=%d total=%.3fs",
                request_id,
                time.perf_counter()
                - started,
            )

        except Exception as exc:
            logger.exception(
                "REQUEST failed id=%d total=%.3fs",
                request_id,
                time.perf_counter()
                - started,
            )

            try:
                _json(
                    self,
                    {"error": str(exc)},
                    500,
                )

            except (
                BrokenPipeError,
                ConnectionResetError,
            ):
                pass


def main() -> None:
    global browser_executor

    browser_executor = BrowserExecutor()

    server = http.server.ThreadingHTTPServer(
        (HOST, PORT),
        SearchHandler,
    )

    server.daemon_threads = True

    logger.info(
        "starting search_service version=%s host=%s port=%d",
        VERSION,
        HOST,
        PORT,
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        logger.info("shutdown requested")

    finally:
        server.shutdown()
        server.server_close()

        if browser_executor is not None:
            browser_executor.shutdown()


if __name__ == "__main__":
    main()
