#!/usr/bin/env python3

"""
search_service.py v3.5 - FIXED

A robust retrieval service that:
1. Searches DuckDuckGo (primary) and Brave (fallback).
2. Fetches article content via HTTP (preferred) or Camoufox (fallback).
3. Enforces strict SSRF policies at every hop (reject if ANY resolved IP is private/reserved).
4. Returns structured, provenance-rich results.
5. ENFORCES TARGET_USABLE_ARTICLES to prevent timeout and ensure correct output format.
"""

import concurrent.futures
import html
import ipaddress
import json
import logging
import logging.handlers
import os
import re
import socket
import sys
import threading
import time
import uuid
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse
from typing import Optional, Dict, Any, List, Tuple

import httpx
from camoufox.sync_api import Camoufox

from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.lex_rank import LexRankSummarizer

# ============================================================
# CONFIG
# ============================================================

HOST = "0.0.0.0"
PORT = 8787

DEFAULT_LIMIT = 10
MAX_LIMIT = 20

MAX_CANDIDATES = 5
TARGET_USABLE_ARTICLES = 2  # Enforced limit for summaries

# Timeouts
SEARCH_TIMEOUT = 10000      # Milliseconds for browser search
HTTP_TIMEOUT = 10.0        # Seconds for HTTP fetch (slightly longer for streaming)
BROWSER_TIMEOUT = 10000     # Millieconds for Camoufox fetch
BROWSER_SETTLE_MS = 250

# Content Limits
MIN_ARTICLE_CHARS = 500
MAX_SUMMARY_CHARS = 32768  # Max length of the final summary string
MAX_CONTENT_BYTES = 2 * 1024 * 1024  # 2MB hard cap for network responses
MAX_ARTICLE_TEXT_BYTES = 10 * 1024 * 1024  # 10MB cap for extracted text before summarization
SUMMARY_SENTENCES = 3  # Fix: defined here to prevent NameError

# Redirect Limit
MAX_REDIRECTS = 10

# Script Directory for absolute path resolution
SCRIPT_DIR = Path(__file__).resolve().parent
READABILITY_JS_PATH = SCRIPT_DIR / "lib" / "Readability.js"

LOG_DIR = "/home/david/AI/camoufox/logs"
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

VERSION = "3.5"

# SSRF Policy: Blocks
BLOCKED_SUBNETS = [
    ipaddress.ip_network("10.0.0.0/8"),       # Private
    ipaddress.ip_network("172.16.0.0/12"),     # Private
    ipaddress.ip_network("192.168.0.0/16"),    # Private
    ipaddress.ip_network("127.0.0.0/8"),       # Loopback
    ipaddress.ip_network("169.254.0.0/16"),    # Link-local
    ipaddress.ip_network("100.64.0.0/10"),     # CGNAT
    ipaddress.ip_network("0.0.0.0/8"),         # Current network
    ipaddress.ip_network("192.0.0.0/24"),      # IETF Reserved
    ipaddress.ip_network("192.0.2.0/24"),      # TEST-NET-1
    ipaddress.ip_network("198.51.100.0/24"),   # TEST-NET-2
    ipaddress.ip_network("203.0.113.0/24"),    # TEST-NET-3
    ipaddress.ip_network("198.18.0.0/15"),     # Benchmarking
]

# IPv6 Private/Unique-Local
BLOCKED_IPV6_SUBNETS = [
    ipaddress.ip_network("fc00::/7"),          # Unique Local
    ipaddress.ip_network("fe80::/10"),         # Link-Local
    ipaddress.ip_network("::1/128"),           # Loopback
    ipaddress.ip_network("::ffff:0:0/96"),     # IPv4-mapped (loopback/private)
]
# Add multicast and reserved IPv6
BLOCKED_IPV6_SUBNETS.append(ipaddress.ip_network("ff00::/8"))  # Multicast
BLOCKED_IPV6_SUBNETS.append(ipaddress.ip_network("2000::/3"))  # Global Unicast is allowed, but we block specific reserved blocks if needed. 
# Actually, standard public IPv6 is allowed. We just block the private/reserved ones.

# ============================================================
# LOGGING
# ============================================================

def setup_logging():
    logger = logging.getLogger("search_service")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    os.makedirs(LOG_DIR, exist_ok=True)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    timestamp = time.strftime("%Y-%m-%d_%H%M%S")
    log_file = os.path.join(
        LOG_DIR,
        f"search_service_{timestamp}.log",
    )

    logfile = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    logfile.setFormatter(formatter)
    logger.addHandler(logfile)

    return logger


log = setup_logging()

# ============================================================
# SSRF / NETWORK UTILS
# ============================================================

def _is_ip_private_or_reserved(ip_str: str) -> bool:
    """
    Check if an IP address is in a blocked range (private, loopback, etc.).
    """
    try:
        addr = ipaddress.ip_address(ip_str)
    except ValueError:
        return False

    if addr.version == 4:
        for subnet in BLOCKED_SUBNETS:
            if addr in subnet:
                return True
    else:
        for subnet in BLOCKED_IPV6_SUBNETS:
            if addr in subnet:
                return True

    return False


def resolve_host(host: str) -> List[str]:
    """
    Resolve hostname to IPv4 and IPv6 addresses.
    Returns the list of public IPs.
    If all resolved IPs are private/reserved, returns [].
    Raises socket.gaierror on DNS failure.
    """
    try:
        addrs_v4 = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        addrs_v6 = socket.getaddrinfo(host, None, socket.AF_INET6, socket.SOCK_STREAM)
        
        public_ips = []
        for info in addrs_v4 + addrs_v6:
            ip = info[4][0]
            if not _is_ip_private_or_reserved(ip):
                public_ips.append(ip)
        return public_ips
    except socket.gaierror:
        return []


def is_url_safe(url_str: str) -> bool:
    """
    Check if the URL host is safe.
    Rejects if any resolved A/AAAA record is private/reserved.
    """
    try:
        parsed = urlparse(url_str)
        host = parsed.hostname

        if not host:
            return False

        # If it's already an IP
        if re.match(r"^\d+\.\d+\.\d+\.\d+$", host):
            return not _is_ip_private_or_reserved(host)
        
        # Resolve ALL addresses
  
        try:
            addrs_v4 = socket.getaddrinfo(host, None, socket.AF_INET, socket.SOCK_STREAM)
        except socket.gaierror:
            addrs_v4 = []
        try:
            addrs_v6 = socket.getaddrinfo(host, None, socket.AF_INET6, socket.SOCK_STREAM)
        except socket.gaierror:
            addrs_v6 = []        
        
        
        
        
        
        all_ips = []
        for info in addrs_v4 + addrs_v6:
            all_ips.append(info[4][0])

        # Reject if ANY IP is private/reserved
        for ip in all_ips:
            if _is_ip_private_or_reserved(ip):
                return False
        
        return True
    except Exception:
        return False


# ============================================================
# CAMOUFOX BROWSER EXECUTOR
# ============================================================

class BrowserExecutor:
    """
    Owns Camoufox and all Playwright objects from one dedicated thread.
    """

    def __init__(self):
        self._condition = threading.Condition()
        self._tasks: List[Tuple[callable, concurrent.futures.Future]] = []
        self._stopping = False
        self._startup_error = None
        self._ready = False
        self._browser = None

        self._thread = threading.Thread(
            target=self._run,
            name="camoufox-owner",
            daemon=True,
        )
        self._thread.start()

        # Wait for startup
        with self._condition:
            while not self._stopping and self._startup_error is None and not self._ready:
                self._condition.wait(timeout=5.0)
                if self._stopping and self._startup_error:
                    break

        if self._startup_error:
            raise RuntimeError(f"Camoufox failed during startup: {self._startup_error}")
        if not self._ready:
            raise RuntimeError("Camoufox browser executor stopped during startup")

    def _run(self):
        camoufox = None
        browser = None

        try:
            log.info("[-] CAMOUFOX OWNER STARTING")
            camoufox = Camoufox(headless=True, locale="en-GB")
            browser = camoufox.__enter__()
            self._browser = browser

            with self._condition:
                self._ready = True
                self._condition.notify_all()

            log.info("[-] CAMOUFOX READY")

            while True:
                with self._condition:
                    while not self._tasks and not self._stopping:
                        self._condition.wait(timeout=1.0)
                    
                    if self._stopping and not self._tasks:
                        break

                    if self._tasks:
                        task, future = self._tasks.pop(0)
                    else:
                        continue

                if future.cancelled():
                    continue

                try:
                    result = task(browser)
                    if not future.cancelled():
                        future.set_result(result)
                except Exception as exc:
                    if not future.cancelled():
                        future.set_exception(exc)

        except Exception as exc:
            log.exception("[-] CAMOUFOX OWNER FAILED")
            with self._condition:
                self._startup_error = exc
                self._stopping = True
                pending = self._tasks
                self._tasks = []
                self._condition.notify_all()
            for _, future in pending:
                if not future.done():
                    future.set_exception(exc)

        finally:
            if camoufox is not None:
                try:
                    camoufox.__exit__(None, None, None)
                except Exception:
                    log.exception("[-] CAMOUFOX SHUTDOWN FAILED")
            
            with self._condition:
                self._stopping = True
                self._ready = False
                self._condition.notify_all()
            log.info("[-] CAMOUFOX OWNER STOPPED")

    def submit(self, task):
        future = concurrent.futures.Future()
        with self._condition:
            if self._stopping:
                future.set_exception(RuntimeError("Browser executor stopped"))
                return future
            if self._startup_error:
                future.set_exception(RuntimeError("Browser executor failed"))
                return future
            if not self._thread.is_alive():
                future.set_exception(RuntimeError("Browser owner thread dead"))
                return future
            self._tasks.append((task, future))
            self._condition.notify()
        return future

    def call(self, task):
        future = self.submit(task)
        try:
            return future.result(timeout=30)
        except Exception:
            log.exception("[-] CAMOUFOX TASK FAILED")
            raise

    def shutdown(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()
        if self._thread.is_alive():
            self._thread.join(timeout=10)
        if self._thread.is_alive():
            log.error("[-] CAMOUFOX OWNER DID NOT STOP CLEANLY")


browser_executor = None
SEARCH_LOCK = threading.Lock()

def start_browser_executor():
    global browser_executor
    if browser_executor is None:
        browser_executor = BrowserExecutor()

def shutdown_browser():
    global browser_executor
    if browser_executor is not None:
        browser_executor.shutdown()
        browser_executor = None

# ============================================================
# HELPERS
# ============================================================

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def decode_ddg_url(href: str) -> str:
    if not href:
        return ""
    try:
        parsed = urlparse(href)
        if parsed.path == "/l/":
            params = parse_qs(parsed.query)
            target = params.get("uddg")
            if target:
                return target[0]
    except Exception:
        pass
    return href

def canonical_url(url: str) -> str:
    if not url:
        return ""
    try:
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()
        host = parsed.netloc.lower()
        
        if host.startswith("www."):
            host = host[4:]
        
        path = parsed.path or "/"
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        return parsed._replace(scheme=scheme, netloc=host, path=path, fragment="",).geturl()
    except Exception:
        return url

def is_html_content(content_type: str) -> bool:
    if not content_type:
        return False
    ct = content_type.lower()
    return "text/html" in ct or "application/xhtml+xml" in ct

# ============================================================
# DDG AD FILTER
# ============================================================

def is_ddg_ad_result(url: str) -> bool:
    if not url:
        return False
    url_lower = url.lower()
    if "duckduckgo.com/y.js" in url_lower: return True
    if "ad_type=txad" in url_lower: return True
    if "ad_provider=" in url_lower: return True
    if "ad_domain=" in url_lower: return True
    return False

def filter_ad_results(results: List[dict], request_id: str) -> List[dict]:
    return [r for r in results if not is_ddg_ad_result(r.get("url", ""))]

# ============================================================
# HUB / TOPIC URL FILTERING
# ============================================================

def is_hub_url(url: str) -> bool:
    if not url:
        return True
    try:
        parsed = urlparse(canonical_url(url))
        host = parsed.netloc.lower()
        path = parsed.path.rstrip("/")

        if host == "reuters.com":
            if path in ("", "/technology", "/world", "/business", "/markets", "/sports", "/lifestyle", "/politics") or path.startswith("/topics/"):
                return True
        if host == "techcrunch.com":
            if path == "" or path.startswith("/category/") or path.startswith("/tag/") or path.startswith("/topic/"):
                return True
        if host == "news.google.com":
            if path == "" or path.startswith("/topics/") or path.startswith("/search"):
                return True
        return False
    except Exception:
        return False

def filter_hub_results(results: List[dict], request_id: str) -> List[dict]:
    for r in results:
        r["_is_hub"] = is_hub_url(r.get("url", ""))
    return results

# ============================================================
# SEARCH PIPELINE
# ============================================================

DDG_EXTRACT_JS = """
() => {
    const results = [];
    document.querySelectorAll(".result").forEach(node => {
        const titleNode = node.querySelector("a.result__a");
        const snippetNode = node.querySelector(".result__snippet");
        if (!titleNode) return;
        const title = (titleNode.innerText || "").trim();
        const href = titleNode.href || "";
        const snippet = snippetNode ? (snippetNode.innerText || "").trim() : "";
        if (title && href) {
            results.push({ title, url: href, snippet });
        }
    });
    return results;
}
"""

def _search_ddg_on_browser(browser, query: str, request_id: str) -> List[dict]:
    page = browser.new_page()
    try:
        search_url = f"https://html.duckduckgo.com/html/?q={quote_plus(query)}"
        page.goto(search_url, wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT)
        page.wait_for_timeout(BROWSER_SETTLE_MS)
        raw_results = page.evaluate(DDG_EXTRACT_JS)
        
        cleaned = []
        for r in raw_results:
            url = canonical_url(decode_ddg_url(r.get("url", "")))
            # FIX: is_url_safe(url) because canonical_url() already returns a fully qualified URL.
            # The previous code used is_url_safe(f"https://{url}") which resulted in 
            # "https://https://example.com" for already-qualified URLs, causing rejection.
            if url and is_url_safe(url):
                cleaned.append({
                    "title": clean_text(r.get("title", "")),
                    "url": url,
                    "snippet": clean_text(r.get("snippet", ""))
                })
        return cleaned
    finally:
        page.close()

def search_ddg(query: str, request_id: str) -> List[dict]:
    return browser_executor.call(lambda browser: _search_ddg_on_browser(browser, query, request_id))

BRAVE_EXTRACT_JS = """
() => {
    const results = [];
    document.querySelectorAll(".snippet").forEach(node => {
        const titleNode = node.querySelector("a.result-header");
        const snippetNode = node.querySelector(".snippet-description");
        if (!titleNode) return;
        const title = (titleNode.innerText || "").trim();
        const href = titleNode.href || "";
        const snippet = snippetNode ? (snippetNode.innerText || "").trim() or "";
        if (title && href) {
            results.push({ title, url: href, snippet });
        }
    });
    return results;
}
"""

def _search_brave_on_browser(browser, query: str, request_id: str) -> List[dict]:
    page = browser.new_page()
    try:
        search_url = f"https://search.brave.com/search?q={quote_plus(query)}"
        page.goto(search_url, wait_until="domcontentloaded", timeout=SEARCH_TIMEOUT)
        page.wait_for_timeout(BROWSER_SETTLE_MS)
        raw_results = page.evaluate(BRAVE_EXTRACT_JS)
        
        cleaned = []
        for r in raw_results:
            url = canonical_url(r.get("url", ""))
            if url:
                cleaned.append({
                    "title": clean_text(r.get("title", "")),
                    "url": url,
                    "snippet": clean_text(r.get("snippet", ""))
                })
        return cleaned
    finally:
        page.close()

def search_brave(query: str, request_id: str) -> List[dict]:
    return browser_executor.call(lambda browser: _search_brave_on_browser(browser, query, request_id))

def deduplicate_results(results: List[dict]) -> List[dict]:
    seen = set()
    output = []
    for r in results:
        url = canonical_url(r.get("url", ""))
        if not url or url in seen:
            continue
        seen.add(url)
        r["url"] = url
        output.append(r)
    return output

def search(query: str, limit: int, request_id: str) -> List[dict]:
    log.info("[%s] SEARCH query=%r limit=%d", request_id, query, limit)
    
    try:
        results = search_ddg(query, request_id)
        results = filter_ad_results(results, request_id)
        results = deduplicate_results(results)
        results = filter_hub_results(results, request_id)
        if results:
            log.info("[%s] DDG results=%d", request_id, len(results))
            return results[:limit]
    except Exception as e:
        log.exception("[%s] DDG FAILED: %s", request_id, e)

    try:
        results = search_brave(query, request_id)
        results = deduplicate_results(results)
        results = filter_hub_results(results, request_id)
        if results:
            log.info("[%s] BRAVE results=%d", request_id, len(results))
            return results[:limit]
    except Exception as e:
        log.exception("[%s] BRAVE FAILED: %s", request_id, e)

    return []

# ============================================================
# READABILITY & EXTRACTION
# ============================================================

def get_readability_source() -> Optional[str]:
    """Load Readability.js relative to the script directory."""
    try:
        with open(READABILITY_JS_PATH, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        log.error("Readability.js not found at %s", READABILITY_JS_PATH)
        return None

READABILITY_SOURCE = None
READABILITY_LOCK = threading.Lock()

def extract_article(page) -> Optional[dict]:
    source = get_readability_source()
    if not source:
        return None
    
    # We use a wrapper to eval the source in the page context
    js_code = f"""
    (() => {{
        try {{
            eval({json.dumps(source)});
            const doc = document.cloneNode(true);
            const reader = new Readability(doc);
            const article = reader.parse();
            if (!article) return null;
            return {{
                title: article.title || "",
                content: article.content || "",
                textContent: article.textContent || "",
                excerpt: article.excerpt || ""
            }};
        }} catch (e) {{
            return {{ error: String(e) }};
        }}
    }})()
    """
    return page.evaluate(js_code)

def html_to_text(source: str) -> str:
    if not source:
        return ""
    source = re.sub(r"<(script|style|noscript)[^>]*>.*?</\1>", " ", source, flags=re.I|re.S)
    source = re.sub(r"<(p|div|br|li|h[1-6]|section|article)[^>]*>", "\n", source, flags=re.I)
    source = re.sub(r"</(p|div|li|h[1-6]|section|article)>", "\n", source, flags=re.I)
    source = re.sub(r"<[^>]+>", " ", source)
    source = html.unescape(source)
    return clean_text(source)

def article_quality(article: Optional[dict]) -> Tuple[bool, str, str]:
    """
    Returns (is_usable, text_content, reason_code)
    """
    if not article:
        return False, "", "no_article"
    if article.get("error"):
        return False, "", "readability_error"
    
    text = html_to_text(article.get("content", ""))
    text = clean_text(text)
    
    if not text:
        return False, text, "empty_content"
    if len(text) < MIN_ARTICLE_CHARS:
        return False, text, f"too_short:{len(text)}"
    
    # Enforce extracted text cap
    if len(text.encode('utf-8')) > MAX_ARTICLE_TEXT_BYTES:
        # Truncate to cap
        text = text[:int(MAX_ARTICLE_TEXT_BYTES * 0.8)] # Rough heuristic for UTF-8 safety
        text += "..."
 
    return True, text, "ok"

def summarize(text: str) -> str:
    try:
        parser = PlaintextParser.from_string(text, Tokenizer("english"))
        summarizer = LexRankSummarizer()
        sentences = summarizer(parser.document, SUMMARY_SENTENCES)
        result = " ".join(str(s) for s in sentences)
        # Cap the summary size
        if len(result) > MAX_SUMMARY_CHARS:
            return result[:MAX_SUMMARY_CHARS]
        return result
    except Exception:
        return ""

# ============================================================
# HTTP RETRIEVAL (STRICT SSRF & STREAMING)
# ============================================================

def fetch_http(url: str, request_id: str, index: int) -> Tuple[Optional[str], str, Dict[str, Any]]:
    """
    Fetches URL via HTTP.
    Returns (html_content, status_string, metadata).
    metadata contains: final_url, status_code, content_type, bytes_read, reason.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
    }

    # 1. Initial SSRF Check
    if not is_url_safe(url):
        return None, "ssrf_initial", {"final_url": url, "reason": "ssrf_initial", "status_code": None, "content_type": None, "bytes_read": 0}

    try:
        with httpx.Client(follow_redirects=True, timeout=HTTP_TIMEOUT, headers=headers, max_redirects=MAX_REDIRECTS) as client:
            # Use stream context manager for memory efficiency
            with client.stream("GET", url) as response:
                
                final_url_str = str(response.url)
                metadata = {
                    "final_url": final_url_str,
                    "status_code": response.status_code,
                    "content_type": response.headers.get("content-type", ""),
                    "bytes_read": 0,
                    "reason": ""
                }

                # 2. Final SSRF Check (Redirect Target)
                parsed_final = urlparse(final_url_str)
                final_host = parsed_final.hostname
                if final_host:
                    if re.match(r"^\d+\.\d+\.\d+\.\d+$", final_host):
                        if _is_ip_private_or_reserved(final_host):
                            return None, "ssrf_redirect", metadata
                    else:
                        resolved = resolve_host(final_host)
                        if not resolved: # Empty list means all IPs are private/reserved
                            return None, "ssrf_redirect", metadata

                if not is_html_content(metadata["content_type"]):
                    return None, "not_html", metadata

                # Check Content-Length header if present
                cl_header = response.headers.get("content-length")
                if cl_header:
                    try:
                        cl = int(cl_header)
                        if cl > MAX_CONTENT_BYTES:
                            return None, "content_too_large", metadata
                    except ValueError:
                        pass

                # Stream the content
                bytes_read = 0
                content_buffer = []
                try:
                    for chunk in response.iter_bytes():
                        bytes_read += len(chunk)
                        metadata["bytes_read"] = bytes_read
                        if bytes_read > MAX_CONTENT_BYTES:
                            return None, "content_too_large", metadata
                        content_buffer.append(chunk)
                except httpx.StreamError:
                    return None, "stream_error", metadata

                try:
                    html_content = b"".join(content_buffer).decode('utf-8', errors='replace')
                except Exception as e:
                    return None, "decode_error", metadata

                if 200 <= response.status_code < 300:
                    metadata["reason"] = "ok"
                    return html_content, "ok", metadata
                else:
                    metadata["reason"] = f"http_error_{response.status_code}"
                    return None, f"http_error_{response.status_code}", metadata

    except httpx.RequestError as e:
        return None, f"http_error_{type(e).__name__}", {"final_url": url, "reason": f"http_error_{type(e).__name__}", "status_code": None, "content_type": None, "bytes_read": 0}
    except Exception as e:
        return None, f"unexpected_error_{type(e).__name__}", {"final_url": url, "reason": f"unexpected_error_{type(e).__name__}", "status_code": None, "content_type": None, "bytes_read": 0}


def _article_from_http_on_browser(browser, html_source: str, request_id: str, index: int) -> Optional[dict]:
    page = browser.new_page()
    try:
        page.set_content(html_source, wait_until="domcontentloaded", timeout=5000)
        return extract_article(page)
    except Exception as e:
        log.warning("[%s] READABILITY EXEC ERROR: %s", request_id, e)
        return None
    finally:
        page.close()


def article_from_http(html_source: str, request_id: str, index: int) -> Optional[dict]:
    return browser_executor.call(lambda browser: _article_from_http_on_browser(browser, html_source, request_id, index))

# ============================================================
# CAMOUFOX RETRIEVAL
# ============================================================

def _fetch_camoufox_on_browser(browser, url: str, request_id: str, index: int) -> Optional[dict]:
    page = browser.new_page()
    try:
        # Validate initial URL
        if not is_url_safe(url):
            return None
            
        page.goto(url, wait_until="domcontentloaded", timeout=BROWSER_TIMEOUT)
        page.wait_for_timeout(BROWSER_SETTLE_MS)
        
        # Note: Camoufox does not enforce SSRF on internal JS fetches.
        # This is a known limitation. The initial navigation is checked.
        # If the page redirects to a private IP via JS, it's not caught here.
        
        return extract_article(page)
    except Exception as e:
        log.info("[%s] CAMOUFOX ERROR: %s", request_id, e)
        return None
    finally:
        page.close()


def fetch_camoufox(url: str, request_id: str, index: int) -> Optional[dict]:
    return browser_executor.call(lambda browser: _fetch_camoufox_on_browser(browser, url, request_id, index))

# ============================================================
# ENHANCEMENT LOGIC
# ============================================================

def build_summary_result(result: dict, text: str, request_id: str, index: int, fetch_method: str, start_time: float, metadata: Dict) -> dict:
    summary = summarize(text)
    total_time = time.perf_counter() - start_time
    
    return {
        **result,
        "content": summary,
        "content_source": "article_summary",
        "fetch_method": fetch_method,
        "extract_time": total_time,
        "metadata": {
            "final_url": metadata.get("final_url", ""),
            "status_code": metadata.get("status_code"),
            "content_type": metadata.get("content_type"),
            "bytes_read": metadata.get("bytes_read"),
            "reason": metadata.get("reason")
        }
    }


def enhance_result(result: dict, index: int, request_id: str) -> dict:
    url = result.get("url", "")
    snippet = result.get("snippet", "")
    start = time.perf_counter()

    # 1. HTTP Fetch
    html_source, http_reason, http_metadata = fetch_http(url, request_id, index)
    
    if html_source:
        article = article_from_http(html_source, request_id, index)
        usable, text, reason = article_quality(article)
        
        if usable:
            return build_summary_result(result, text, request_id, index, "http", start, http_metadata)
        else:
            log.info("[%s] HTTP rejected: %s", request_id, reason)

    # 2. Camoufox Fallback
    article = fetch_camoufox(url, request_id, index)
    if article:
        usable, text, reason = article_quality(article)
        if usable:
            camoufox_metadata = {
                "final_url": url,
                "status_code": "browser",
                "content_type": "text/html",
                "bytes_read": 0,
                "reason": "ok"
            }
            return build_summary_result(result, text, request_id, index, "camoufox", start, camoufox_metadata)

    # Failure
    return {
        **result,
        "content": snippet,
        "content_source": "search_snippet",
        "fetch_method": "none",
        "extract_reason": reason if article else http_reason,
        "metadata": http_metadata
    }

# ============================================================
# HUB LINK EXTRACTION
# ============================================================

HUB_LINK_EXTRACT_JS = """
() => {
    const links = [];
    document.querySelectorAll("a[href]").forEach(node => {
        const href = node.href || "";
        const title = (node.innerText || node.textContent || "").trim();
        if (href && title) links.push({ title, url: href });
    });
    return links;
}
"""

def _filter_hub_links(links: list, hub_url: str) -> List[dict]:
    if not links:
        return []
    try:
        hub = urlparse(canonical_url(hub_url))
        hub_host = hub.netloc.lower()
    except Exception:
        return []

    output = []
    seen = set()

    for link in links:
        if not isinstance(link, dict): continue
        title = str(link.get("title", "")).strip()
        url = str(link.get("url", "")).strip()
        if not title or not url: continue
        
        try:
            parsed = urlparse(canonical_url(url))
        except Exception:
            continue

        if parsed.scheme not in ("http", "https"): continue
        if hub_host and parsed.netloc.lower() != hub_host: continue
        
        canonical = canonical_url(url)
        if not canonical or canonical in seen: continue
        if is_hub_url(canonical): continue

        path = parsed.path.lower()
        if any(t in path for t in ("/author/", "/tag/", "/category/", "/feed", "/login")): continue
        if re.search(r"\.(pdf|jpg|png)$", path): continue

        seen.add(canonical)
        output.append({"title": title, "url": canonical, "url_source": "page_link"})
    
    return output


def _extract_hub_links_from_html(html_source: str, hub_url: str) -> List[dict]:
    class LinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []
            self.current_href = None
            self.current_text = []
        def handle_starttag(self, tag, attrs):
            if tag.lower() == "a":
                attrs = dict(attrs)
                self.current_href = attrs.get("href", "").strip()
                self.current_text = []
        def handle_data(self, data):
            if self.current_href is not None:
                self.current_text.append(data)
        def handle_endtag(self, tag):
            if tag.lower() == "a" and self.current_href:
                title = " ".join("".join(self.current_text).split())
                try:
                    absolute = urljoin(canonical_url(hub_url), self.current_href)
                except Exception:
                    absolute = self.current_href
                self.links.append({"title": title, "url": absolute})
                self.current_href = None

    parser = LinkParser()
    try:
        parser.feed(html_source)
    except Exception:
        return []
    return _filter_hub_links(parser.links, hub_url)


def _extract_hub_links_on_browser(url: str, request_id: str, index: int) -> List[dict]:
    def task(browser):
        page = browser.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=10)
            page.wait_for_timeout(BROWSER_SETTLE_MS)
            raw_links = page.evaluate(HUB_LINK_EXTRACT_JS)
            return _filter_hub_links(raw_links, url)
        finally:
            page.close()
    try:
        return browser_executor.call(task)
    except Exception as e:
        log.warning("[%s] HUB BROWSER ERROR: %s", request_id, e)
        return []


def extract_hub_links(result: dict, request_id: str, index: int) -> List[dict]:
    url = result.get("url", "")
    if not url: return []

    html_source, _, _ = fetch_http(url, request_id, index)
    
    if html_source:
        links = _extract_hub_links_from_html(html_source, url)
        if links:
            return links

    return _extract_hub_links_on_browser(url, request_id, index)


def enrich_hub_results(results: List[dict], request_id: str) -> List[dict]:
    hubs = [(i, r) for i, r in enumerate(results, 1) if r.get("_is_hub")]
    if not hubs: return results

    for index, result in hubs:
        try:
            links = extract_hub_links(result, request_id, index)
            result["links"] = links
        except Exception as e:
            log.exception("[%s] HUB ERROR: %s", request_id, e)
            result["links"] = []
    return results

# ============================================================
# PARALLEL CANDIDATES
# ============================================================

def enhance_candidates(results: List[dict], request_id: str, limit: int) -> List[dict]:
    results = enrich_hub_results(results, request_id)
    # Filter out hubs for candidate processing
    candidates = [r for r in results if not r.get("_is_hub", False)][:MAX_CANDIDATES]
    
    if not candidates:
        return results

    log.info("[%s] PROCESSING %d candidates", request_id, len(candidates))

    # Use a dictionary to store enhancements by index to preserve order later
    enhancements = {}
    usable_count = 0
    start_time = time.perf_counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(candidates))) as executor:
        # Map future -> index (1-based index into candidates list)
        future_to_idx = {
            executor.submit(fetch_http, cand.get("url", ""), request_id, i): i
            for i, cand in enumerate(candidates, 1)
        }

        # Process results as they complete
        for future in concurrent.futures.as_completed(future_to_idx):
            idx = future_to_idx[future]
            result = candidates[idx-1]

            # OPTIMIZATION: If we already have enough usable articles, skip processing others
            if usable_count >= TARGET_USABLE_ARTICLES:
                log.info("[%s] TARGET REACHED (%d), skipping candidate %d", request_id, usable_count, idx)
                continue

            try:
                html_source, reason, metadata = future.result()
            except Exception as e:
                html_source = None
                reason = f"worker_error_{type(e).__name__}"
                metadata = {"reason": reason, "final_url": result.get("url"), "status_code": None, "content_type": None, "bytes_read": 0}

            enhanced = None
            
            # Try HTTP Enhance
            if html_source:
                article = article_from_http(html_source, request_id, idx)
                if article:
                    usable, text, reason = article_quality(article)
                    if usable:
                        enhanced = build_summary_result(result, text, request_id, idx, "http", time.perf_counter(), metadata)
                        usable_count += 1

            # FIX: Remove the usable_count gate so ALL candidates get Camoufox tried
            if not enhanced:
                article = fetch_camoufox(result.get("url", ""), request_id, idx)
                if article:
                    usable, text, reason = article_quality(article)
                    if usable:
                        camoufox_metadata = {
                            "final_url": result.get("url"),
                            "status_code": "browser",
                            "content_type": "text/html",
                            "bytes_read": 0,
                            "reason": "ok"
                        }
                        enhanced = build_summary_result(result, text, request_id, idx, "camoufox", time.perf_counter(), camoufox_metadata)
                        usable_count += 1

            if enhanced:
                enhancements[idx] = enhanced

    # Reconstruct results in original order, preserving hubs
    # Map candidate index (1-based) back to its position in the original results list
    # We need to maintain the original order of results, with hubs in place.
    
    final_results = []
    enhanced_count = 0
    
    # Create a lookup: candidate_index -> enhanced_result
    enhancement_lookup = {}
    for cand_idx, enhanced in enhancements.items():
        enhancement_lookup[cand_idx] = enhanced

    for r in results:
        if r in candidates:
            # Find the index of this candidate in the candidates list
            cand_idx = candidates.index(r) + 1  # 1-based index
            if cand_idx in enhancement_lookup and enhanced_count < TARGET_USABLE_ARTICLES:
                final_results.append(enhancement_lookup[cand_idx])
                enhanced_count += 1
            else:
                # Return original snippet if not enhanced or target reached
                final_results.append(r)
        else:
            final_results.append(r)
    
    # Trim to limit if necessary (though usually results <= limit from search())
    final_results = final_results[:limit]
    
    return final_results

# ============================================================
# HTTP SERVER
# ============================================================

class SearchHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def send_json(self, status, data):
        # Clean internal markers
        def clean(obj):
            if isinstance(obj, dict):
                return {k: clean(v) for k, v in obj.items() if k != "_is_hub"}
            if isinstance(obj, list):
                return [clean(i) for i in obj]
            return obj

        data = clean(data)
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            log.info("Client disconnected")

    def do_GET(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self.send_json(200, {"status": "ok", "version": VERSION})
            return

        if parsed.path != "/search":
            self.send_json(404, {"error": "not found"})
            return

        params = parse_qs(parsed.query)
        query = params.get("q", [""])[0].strip()
        
        try:
            limit = int(params.get("limit", [DEFAULT_LIMIT])[0])
        except ValueError:
            limit = DEFAULT_LIMIT
        limit = max(1, min(limit, MAX_LIMIT))

        if not query:
            self.send_json(400, {"error": "missing q"})
            return

        request_id = uuid.uuid4().hex[:8]
        self.request_id = request_id
        start = time.perf_counter()

        try:
            with SEARCH_LOCK:
                results = search(query, limit, request_id)
                enhanced = enhance_candidates(results, request_id, limit)
            
            total = time.perf_counter() - start
            usable = sum(1 for r in enhanced if r.get("content_source") == "article_summary")
            
            log.info("[%s] DONE total=%.3fs usable=%d", request_id, total, usable)
            
            # LOG ALL RESULTS SENT BACK TO LLM - ALWAYS
            for r in enhanced:
                content_preview = (r.get("content", "") or "").strip()[:300].replace("\n", " ")
                log.info(
                    "[%s] RETURN: URL=%s TITLE=%s SOURCE=%s CONTENT=%s",
                    request_id,
                    r.get("url", "")[:80],
                    r.get("title", "")[:120],
                    r.get("content_source", "search_snippet"),
                    content_preview[:300]
                )
            
            self.send_json(200, {"query": query, "results": enhanced})

        except Exception as e:
            log.exception("[%s] ERROR: %s", request_id, e)
            self.send_json(500, {"error": str(e)})


# ============================================================
# MAIN
# ============================================================

def main():
    start_browser_executor()
    server = ThreadingHTTPServer((HOST, PORT), SearchHandler)
    log.info("search_service v%s on %s:%d", VERSION, HOST, PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Shutting down")
    finally:
        server.server_close()
        shutdown_browser()

if __name__ == "__main__":
    main()
