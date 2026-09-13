#!/usr/bin/env python3

import concurrent.futures
import html
import json
import logging
import os
from logging.handlers import RotatingFileHandler
import re
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from html.parser import HTMLParser
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

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

# Search results considered for article enhancement.
MAX_CANDIDATES = 5

# Stop article enhancement after this many usable articles.
TARGET_USABLE_ARTICLES = 2

SEARCH_TIMEOUT = 10000          # Camoufox search timeout, ms
HTTP_TIMEOUT = 5.0              # Ordinary HTTP article retrieval, seconds
FETCH_TIMEOUT = 10000           # Camoufox article fallback timeout, ms
FETCH_SETTLE_MS = 250

MIN_ARTICLE_CHARS = 500
MAX_SUMMARY_CHARS = 30000
SUMMARY_SENTENCES = 8

READABILITY_JS = "lib/Readability.js"

LOG_DIR = "/home/david/AI/camoufox/logs"
LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

VERSION = "3.1"


# ============================================================
# LOGGING
# ============================================================

def setup_logging():
    logger = logging.getLogger("search_service")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    import os
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

    logfile = RotatingFileHandler(
        log_file,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    logfile.setFormatter(formatter)

    # Deliberately no StreamHandler:
    # normal service logging must not go to stdout/stderr.
    logger.addHandler(logfile)

    return logger


log = setup_logging()


# ============================================================
# CAMOUFOX
# ============================================================

class BrowserExecutor:
    """
    Owns Camoufox and all Playwright objects from one dedicated
    thread.

    HTTP worker threads NEVER access Camoufox/Playwright objects.

    The owner thread is persistent for the lifetime of the service.
    Calls fail explicitly if the owner dies instead of hanging forever.
    """

    def __init__(self):
        self._condition = threading.Condition()
        self._tasks = []
        self._stopping = False
        self._startup_error = None
        self._ready = False

        self._thread = threading.Thread(
            target=self._run,
            name="camoufox-owner",
            daemon=True,
        )

        self._thread.start()

        # Wait until Camoufox has either started successfully
        # or failed. This prevents the first request racing startup.
        with self._condition:
            while (
                not self._stopping
                and self._startup_error is None
                and not self._ready
            ):
                self._condition.wait()

        if self._startup_error is not None:
            raise RuntimeError(
                "Camoufox failed during startup"
            ) from self._startup_error

        if not self._ready:
            raise RuntimeError(
                "Camoufox browser executor stopped during startup"
            )

    def _run(self):
        camoufox = None
        browser = None

        try:
            log.info("[-] CAMOUFOX OWNER STARTING")

            camoufox = Camoufox(
                headless=True,
                locale="en-GB",
            )

            browser = camoufox.__enter__()

            with self._condition:
                self._ready = True
                self._condition.notify_all()

            log.info("[-] CAMOUFOX READY")

            while True:
                with self._condition:
                    while (
                        not self._tasks
                        and not self._stopping
                    ):
                        self._condition.wait()

                    if self._stopping and not self._tasks:
                        break

                    task, future = self._tasks.pop(0)

                if future.cancelled():
                    continue

                try:
                    result = task(browser)

                except Exception as exc:
                    if not future.cancelled():
                        future.set_exception(exc)

                else:
                    if not future.cancelled():
                        future.set_result(result)

        except Exception as exc:
            log.exception(
                "[-] CAMOUFOX OWNER FAILED"
            )

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
                    log.exception(
                        "[-] CAMOUFOX SHUTDOWN FAILED"
                    )

            with self._condition:
                self._stopping = True
                self._ready = False
                self._condition.notify_all()

            log.info("[-] CAMOUFOX OWNER STOPPED")

    def submit(self, task):
        future = concurrent.futures.Future()

        with self._condition:

            if self._stopping:
                future.set_exception(
                    RuntimeError(
                        "Camoufox browser executor is stopped"
                    )
                )
                return future

            if self._startup_error is not None:
                future.set_exception(
                    RuntimeError(
                        "Camoufox browser executor failed"
                    )
                )
                return future

            if not self._thread.is_alive():
                future.set_exception(
                    RuntimeError(
                        "Camoufox owner thread is dead"
                    )
                )
                return future

            self._tasks.append((task, future))
            self._condition.notify()

        return future

    def call(self, task):
        future = self.submit(task)

        try:
            return future.result()

        except Exception:
            log.exception(
                "[-] CAMOUFOX TASK FAILED"
            )
            raise

    def shutdown(self):
        with self._condition:
            self._stopping = True
            self._condition.notify_all()

        if self._thread.is_alive():
            self._thread.join(timeout=10)

        if self._thread.is_alive():
            log.error(
                "[-] CAMOUFOX OWNER DID NOT STOP CLEANLY"
            )


browser_executor = None

# Only one complete search/enhancement pipeline may run at a time.
#
# ThreadingHTTPServer can receive overlapping requests from Calivi.
# Camoufox is intentionally single-owner/single-thread, so serialising
# the complete search pipeline prevents requests from competing for
# browser work and leaving abandoned work behind.
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

def clean_text(text):
    if not text:
        return ""

    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def decode_ddg_url(href):
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


def canonical_url(url):
    """
    Conservative URL normalisation for deduplication.

    Does not remove arbitrary query parameters because some sites
    legitimately require them to identify an article.
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)

        scheme = parsed.scheme.lower()
        host = parsed.netloc.lower()

        if host.startswith("www."):
            host = host[4:]

        path = parsed.path or "/"

        # Remove a trailing slash except for root.
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")

        return parsed._replace(
            scheme=scheme,
            netloc=host,
            path=path,
            fragment="",
        ).geturl()

    except Exception:
        return url


def is_html_content(content_type):
    if not content_type:
        return False

    content_type = content_type.lower()
    return "text/html" in content_type or "application/xhtml+xml" in content_type


# ============================================================
# DDG AD FILTER
# ============================================================

def is_ddg_ad_result(url):
    if not url:
        return False

    url_lower = url.lower()

    if "duckduckgo.com/y.js" in url_lower:
        return True

    if "ad_type=txad" in url_lower:
        return True

    if "ad_provider=" in url_lower:
        return True

    if "ad_domain=" in url_lower:
        return True

    return False


def filter_ad_results(results, request_id):
    filtered = []
    removed = 0

    for result in results:
        url = result.get("url", "")

        if is_ddg_ad_result(url):
            removed += 1
            log.info("[%s] FILTERING DDG AD: %s", request_id, url)
            continue

        filtered.append(result)

    if removed:
        log.info("[%s] FILTERED %d DDG AD RESULT(S)", request_id, removed)

    return filtered


# ============================================================
# HUB / TOPIC URL FILTERING
# ============================================================

def is_hub_url(url):
    """
    Reject obvious section/topic/homepage URLs.

    This is deliberately conservative: it removes known hub forms
    without rejecting legitimate article URLs merely because they
    live under a publication's section.
    """
    if not url:
        return True

    try:
        parsed = urlparse(canonical_url(url))
        host = parsed.netloc.lower()
        path = parsed.path.rstrip("/")

        # Reuters
        if host == "reuters.com":
            if path in (
                "",
                "/technology",
                "/world",
                "/business",
                "/markets",
                "/sports",
                "/lifestyle",
                "/politics",
            ):
                return True

            if path.startswith("/topics/"):
                return True

        # TechCrunch
        if host == "techcrunch.com":
            if path == "":
                return True

            if path.startswith("/category/"):
                return True

            if path.startswith("/tag/"):
                return True

            if path.startswith("/topic/"):
                return True

        # Google News
        if host == "news.google.com":
            if path == "":
                return True

            if path.startswith("/topics/"):
                return True

            if path.startswith("/search"):
                return True

        return False

    except Exception:
        return False


def filter_hub_results(results, request_id):
    """
    Preserve hub/topic results, but mark them as hubs.

    Hub pages can contain the exact URLs of the underlying articles,
    so they must remain visible to the caller. They are excluded from
    article fetching later by enhance_candidates().
    """
    for result in results:
        url = result.get("url", "")

        if is_hub_url(url):
            result["_is_hub"] = True

            log.info(
                "[%s] HUB RESULT RETAINED: %s",
                request_id,
                url,
            )

        else:
            result["_is_hub"] = False

    return results


# ============================================================
# DDG SEARCH
# ============================================================

DDG_EXTRACT_JS = """
() => {
    const results = [];

    document.querySelectorAll(".result").forEach(node => {

        const titleNode = node.querySelector("a.result__a");
        const snippetNode = node.querySelector(".result__snippet");

        if (!titleNode) {
            return;
        }

        const title = (titleNode.innerText || "").trim();
        const href = titleNode.href || "";
        const snippet = snippetNode
            ? (snippetNode.innerText || "").trim()
            : "";

        if (!title || !href) {
            return;
        }

        results.push({
            title,
            url: href,
            snippet
        });
    });

    return results;
}
"""


def _search_ddg_on_browser(browser, query, request_id):
    page = browser.new_page()

    try:
        start = time.perf_counter()

        search_url = (
            "https://html.duckduckgo.com/html/?q="
            + quote_plus(query)
        )

        page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=SEARCH_TIMEOUT,
        )

        page.wait_for_timeout(250)

        results = page.evaluate(DDG_EXTRACT_JS)

        cleaned = []

        for result in results:
            result["url"] = canonical_url(
                decode_ddg_url(result.get("url", ""))
            )
            result["title"] = clean_text(
                result.get("title", "")
            )
            result["snippet"] = clean_text(
                result.get("snippet", "")
            )

            if result["url"]:
                cleaned.append(result)

        elapsed = time.perf_counter() - start

        log.info(
            "[%s] DDG results=%d duration=%.3fs",
            request_id,
            len(cleaned),
            elapsed,
        )

        return cleaned

    finally:
        page.close()


def search_ddg(query, request_id):
    return browser_executor.call(
        lambda browser: _search_ddg_on_browser(
            browser,
            query,
            request_id,
        )
    )


# ============================================================
# BRAVE SEARCH
# ============================================================

BRAVE_EXTRACT_JS = """
() => {
    const results = [];

    document.querySelectorAll(".snippet").forEach(node => {

        const titleNode = node.querySelector(
            "a.result-header"
        );

        const snippetNode = node.querySelector(
            ".snippet-description"
        );

        if (!titleNode) {
            return;
        }

        const title = (titleNode.innerText || "").trim();
        const href = titleNode.href || "";

        const snippet = snippetNode
            ? (snippetNode.innerText || "").trim()
            : "";

        if (!title || !href) {
            return;
        }

        results.push({
            title,
            url: href,
            snippet
        });
    });

    return results;
}
"""


def _search_brave_on_browser(browser, query, request_id):
    page = browser.new_page()

    try:
        start = time.perf_counter()

        search_url = (
            "https://search.brave.com/search?q="
            + quote_plus(query)
        )

        page.goto(
            search_url,
            wait_until="domcontentloaded",
            timeout=SEARCH_TIMEOUT,
        )

        page.wait_for_timeout(250)

        results = page.evaluate(BRAVE_EXTRACT_JS)

        cleaned = []

        for result in results:
            result["title"] = clean_text(
                result.get("title", "")
            )
            result["url"] = canonical_url(
                result.get("url", "")
            )
            result["snippet"] = clean_text(
                result.get("snippet", "")
            )

            if result["url"]:
                cleaned.append(result)

        elapsed = time.perf_counter() - start

        log.info(
            "[%s] BRAVE results=%d duration=%.3fs",
            request_id,
            len(cleaned),
            elapsed,
        )

        return cleaned

    finally:
        page.close()


def search_brave(query, request_id):
    return browser_executor.call(
        lambda browser: _search_brave_on_browser(
            browser,
            query,
            request_id,
        )
    )


# ============================================================
# SEARCH
# ============================================================

def deduplicate_results(results):
    seen = set()
    output = []

    for result in results:
        url = canonical_url(result.get("url", ""))

        if not url or url in seen:
            continue

        seen.add(url)
        result["url"] = url
        output.append(result)

    return output


def search(query, limit, request_id):
    log.info(
        "[%s] SEARCH query=%r limit=%d",
        request_id,
        query,
        limit,
    )

    # DDG primary.
    try:
        results = search_ddg(query, request_id)
        results = filter_ad_results(results, request_id)
        results = deduplicate_results(results)
        results = filter_hub_results(results, request_id)

        if results:
            log.info(
                "[%s] SEARCH provider=DDG usable=%d",
                request_id,
                len(results),
            )
            return results[:limit]

        log.info(
            "[%s] DDG returned no usable results; falling back to Brave",
            request_id,
        )

    except Exception as exc:
        log.exception(
            "[%s] DDG SEARCH FAILED: %s",
            request_id,
            exc,
        )

    # Brave fallback.
    try:
        results = search_brave(query, request_id)
        results = deduplicate_results(results)
        results = filter_hub_results(results, request_id)

        if results:
            log.info(
                "[%s] SEARCH provider=Brave usable=%d",
                request_id,
                len(results),
            )
            return results[:limit]

    except Exception as exc:
        log.exception(
            "[%s] BRAVE SEARCH FAILED: %s",
            request_id,
            exc,
        )

    return []


# ============================================================
# READABILITY
# ============================================================

def load_readability():
    with open(
        READABILITY_JS,
        "r",
        encoding="utf-8",
    ) as f:
        return f.read()


READABILITY_SOURCE = None
READABILITY_LOCK = threading.Lock()


def get_readability_source():
    global READABILITY_SOURCE

    with READABILITY_LOCK:
        if READABILITY_SOURCE is None:
            READABILITY_SOURCE = load_readability()

        return READABILITY_SOURCE


def extract_article(page):
    readability_source = get_readability_source()

    return page.evaluate(
        """
        ({readabilitySource}) => {

            try {

                eval(readabilitySource);

                const documentClone =
                    document.cloneNode(true);

                const reader =
                    new Readability(documentClone);

                const article =
                    reader.parse();

                if (!article) {
                    return null;
                }

                return {
                    title: article.title || "",
                    content: article.content || "",
                    textContent: article.textContent || "",
                    excerpt: article.excerpt || ""
                };

            } catch (error) {

                return {
                    error: String(error)
                };
            }
        }
        """,
        {
            "readabilitySource": readability_source
        },
    )


# ============================================================
# HTML → TEXT
# ============================================================

def html_to_text(source):
    if not source:
        return ""

    source = re.sub(
        r"<(script|style|noscript)[^>]*>.*?</\1>",
        " ",
        source,
        flags=re.I | re.S,
    )

    source = re.sub(
        r"<(p|div|br|li|h[1-6]|section|article)[^>]*>",
        "\n",
        source,
        flags=re.I,
    )

    source = re.sub(
        r"</(p|div|li|h[1-6]|section|article)>",
        "\n",
        source,
        flags=re.I,
    )

    source = re.sub(
        r"<[^>]+>",
        " ",
        source,
    )

    source = html.unescape(source)

    lines = []

    for line in source.splitlines():
        line = clean_text(line)

        if line:
            lines.append(line)

    return "\n".join(lines)


# ============================================================
# ARTICLE QUALITY
# ============================================================

def article_quality(article):
    if not article:
        return False, "", "no_article"

    if article.get("error"):
        return False, "", "readability_error"

    text = html_to_text(article.get("content", ""))
    text = clean_text(text)

    if len(text) < MIN_ARTICLE_CHARS:
        return False, text, f"too_short:{len(text)}"

    if len(text) > MAX_SUMMARY_CHARS:
        return False, text, f"too_large:{len(text)}"

    return True, text, "ok"


# ============================================================
# SUMY
# ============================================================

def summarize(text):
    parser = PlaintextParser.from_string(
        text,
        Tokenizer("english"),
    )

    summarizer = LexRankSummarizer()

    sentences = summarizer(
        parser.document,
        SUMMARY_SENTENCES,
    )

    return " ".join(
        str(sentence)
        for sentence in sentences
    )


# ============================================================
# ARTICLE FETCHING
# ============================================================

def fetch_http(url, request_id, index):
    start = time.perf_counter()

    log.info(
        "[%s] FETCH[%d] HTTP start url=%s",
        request_id,
        index,
        url,
    )

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 "
                "(KHTML, like Gecko) "
                "Chrome/140.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,"
                "application/xml;q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "en-GB,en;q=0.9",
        }

        with httpx.Client(
            follow_redirects=True,
            timeout=HTTP_TIMEOUT,
            headers=headers,
        ) as client:
            response = client.get(url)

        elapsed = time.perf_counter() - start
        content_type = response.headers.get("content-type", "")

        log.info(
            "[%s] FETCH[%d] HTTP response status=%d type=%r "
            "duration=%.3fs final_url=%s",
            request_id,
            index,
            response.status_code,
            content_type,
            elapsed,
            str(response.url),
        )

        if response.status_code < 200 or response.status_code >= 300:
            return None, f"http_status:{response.status_code}"

        if not is_html_content(content_type):
            return None, f"not_html:{content_type or 'missing'}"

        return response.text, "ok"

    except Exception as exc:
        elapsed = time.perf_counter() - start

        log.info(
            "[%s] FETCH[%d] HTTP failed duration=%.3fs error=%s",
            request_id,
            index,
            elapsed,
            exc,
        )

        return None, f"http_error:{exc}"


def _article_from_http_on_browser(
    browser,
    html_source,
    request_id,
    index,
):
    """
    Execute Readability on the dedicated browser-owning thread.
    """
    page = browser.new_page()

    try:
        page.set_content(
            html_source,
            wait_until="domcontentloaded",
            timeout=FETCH_TIMEOUT,
        )

        return extract_article(page)

    except Exception as exc:
        log.info(
            "[%s] FETCH[%d] HTTP Readability execution failed: %s",
            request_id,
            index,
            exc,
        )
        return None

    finally:
        page.close()


def article_from_http(html_source, request_id, index):
    """
    Execute Readability through the browser-owning thread.
    """
    return browser_executor.call(
        lambda browser: _article_from_http_on_browser(
            browser,
            html_source,
            request_id,
            index,
        )
    )



def _fetch_camoufox_on_browser(
    browser,
    url,
    request_id,
    index,
):
    start = time.perf_counter()

    log.info(
        "[%s] FETCH[%d] CAMOUFOX fallback start url=%s",
        request_id,
        index,
        url,
    )

    page = browser.new_page()

    try:
        page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=FETCH_TIMEOUT,
        )

        page.wait_for_timeout(FETCH_SETTLE_MS)

        article = extract_article(page)

        elapsed = time.perf_counter() - start

        if article and not article.get("error"):
            log.info(
                "[%s] FETCH[%d] CAMOUFOX Readability completed "
                "duration=%.3fs",
                request_id,
                index,
                elapsed,
            )
        else:
            log.info(
                "[%s] FETCH[%d] CAMOUFOX Readability failed "
                "duration=%.3fs",
                request_id,
                index,
                elapsed,
            )

        return article

    except Exception as exc:
        elapsed = time.perf_counter() - start

        log.info(
            "[%s] FETCH[%d] CAMOUFOX failed duration=%.3fs error=%s",
            request_id,
            index,
            elapsed,
            exc,
        )

        return None

    finally:
        page.close()


def fetch_camoufox(url, request_id, index):
    """
    Run the complete Camoufox fallback on the browser-owning thread.
    """
    return browser_executor.call(
        lambda browser: _fetch_camoufox_on_browser(
            browser,
            url,
            request_id,
            index,
        )
    )



def enhance_result(result, index, request_id):
    url = result.get("url", "")
    snippet = result.get("snippet", "")

    candidate_start = time.perf_counter()

    # --------------------------------------------------------
    # Stage 1: ordinary HTTP
    # --------------------------------------------------------

    html_source, http_reason = fetch_http(
        url,
        request_id,
        index,
    )

    article = None

    if html_source is not None:
        article = article_from_http(
            html_source,
            request_id,
            index,
        )

        usable, text, quality_reason = article_quality(article)

        if usable:
            log.info(
                "[%s] FETCH[%d] HTTP article usable chars=%d",
                request_id,
                index,
                len(text),
            )

            return build_summary_result(
                result,
                text,
                request_id,
                index,
                "http",
                candidate_start,
            )

        log.info(
            "[%s] FETCH[%d] HTTP article rejected quality=%s; "
            "trying CAMOUFOX",
            request_id,
            index,
            quality_reason,
        )

    else:
        log.info(
            "[%s] FETCH[%d] HTTP retrieval failed reason=%s; "
            "trying CAMOUFOX",
            request_id,
            index,
            http_reason,
        )

    # --------------------------------------------------------
    # Stage 2: Camoufox
    # --------------------------------------------------------

    article = fetch_camoufox(
        url,
        request_id,
        index,
    )

    usable, text, quality_reason = article_quality(article)

    if usable:
        log.info(
            "[%s] FETCH[%d] CAMOUFOX article usable chars=%d",
            request_id,
            index,
            len(text),
        )

        return build_summary_result(
            result,
            text,
            request_id,
            index,
            "camoufox",
            candidate_start,
        )

    log.info(
        "[%s] FETCH[%d] article unusable after both methods "
        "reason=%s; using search snippet",
        request_id,
        index,
        quality_reason,
    )

    return {
        **result,
        "content": snippet,
        "content_source": "search_snippet",
        "fetch_method": "none",
    }


def build_summary_result(
    result,
    text,
    request_id,
    index,
    fetch_method,
    candidate_start,
):
    sumy_start = time.perf_counter()

    try:
        summary = summarize(text)
    except Exception as exc:
        sumy_time = time.perf_counter() - sumy_start

        log.info(
            "[%s] FETCH[%d] SUMY failed duration=%.3fs error=%s",
            request_id,
            index,
            sumy_time,
            exc,
        )

        return {
            **result,
            "content": result.get("snippet", ""),
            "content_source": "search_snippet",
            "fetch_method": fetch_method,
        }

    sumy_time = time.perf_counter() - sumy_start
    total = time.perf_counter() - candidate_start

    if not summary:
        log.info(
            "[%s] FETCH[%d] SUMY returned empty result",
            request_id,
            index,
        )

        return {
            **result,
            "content": result.get("snippet", ""),
            "content_source": "search_snippet",
            "fetch_method": fetch_method,
        }

    log.info(
        "[%s] FETCH[%d] SUCCESS method=%s sumy=%.3fs total=%.3fs",
        request_id,
        index,
        fetch_method,
        sumy_time,
        total,
    )

    return {
        **result,
        "content": summary,
        "content_source": "article_summary",
        "fetch_method": fetch_method,
    }


# ============================================================
# HUB ARTICLE LINK EXTRACTION
# ============================================================

HUB_LINK_EXTRACT_JS = """
() => {
    const links = [];

    document.querySelectorAll("a[href]").forEach(node => {
        const href = node.href || "";
        const title = (node.innerText || node.textContent || "").trim();

        if (!href || !title) {
            return;
        }

        links.push({
            title,
            url: href
        });
    });

    return links;
}
"""


def _filter_hub_links(links, hub_url):
    """
    Keep useful article-looking links from a hub page.

    URLs remain exactly the URLs discovered from the page. We never
    construct article URLs from titles or snippets.
    """
    if not links:
        return []

    try:
        hub = urlparse(canonical_url(hub_url))
        hub_host = hub.netloc.lower()
    except Exception:
        hub_host = ""

    output = []
    seen = set()

    for link in links:
        if not isinstance(link, dict):
            continue

        title = str(link.get("title", "")).strip()
        url = str(link.get("url", "")).strip()

        if not title or not url:
            continue

        try:
            parsed = urlparse(canonical_url(url))
        except Exception:
            continue

        if parsed.scheme not in ("http", "https"):
            continue

        # Avoid sending users/LLMs off to arbitrary external links
        # found in navigation, advertising, social media, etc.
        if hub_host and parsed.netloc.lower() != hub_host:
            continue

        canonical = canonical_url(url)

        if not canonical or canonical in seen:
            continue

        if is_hub_url(canonical):
            continue

        # Obvious non-article/navigation targets.
        path = parsed.path.lower()

        if any(
            token in path
            for token in (
                "/author/",
                "/authors/",
                "/tag/",
                "/category/",
                "/topic/",
                "/search",
                "/feed",
                "/login",
                "/account",
                "/privacy",
                "/terms",
                "/contact",
            )
        ):
            continue

        # Ignore obvious utility/file links.
        if re.search(
            r"\.(pdf|jpg|jpeg|png|gif|webp|mp4|mp3|xml|rss)$",
            path,
        ):
            continue

        seen.add(canonical)

        output.append(
            {
                "title": title,
                "url": canonical,
                "url_source": "page_link",
            }
        )

    return output


def _extract_hub_links_from_html(html_source, hub_url):
    """
    Extract anchor links from ordinary HTTP HTML.

    This deliberately uses only links actually present in the page.
    """
    if not html_source:
        return []

    class LinkParser(HTMLParser):
        def __init__(self):
            super().__init__()
            self.links = []
            self.current_href = None
            self.current_text = []

        def handle_starttag(self, tag, attrs):
            if tag.lower() != "a":
                return

            attrs = dict(attrs)
            href = attrs.get("href")

            if href:
                self.current_href = href.strip()
                self.current_text = []

        def handle_data(self, data):
            if self.current_href is not None:
                self.current_text.append(data)

        def handle_endtag(self, tag):
            if tag.lower() != "a":
                return

            if self.current_href is not None:
                title = " ".join(
                    "".join(self.current_text).split()
                )

                href = self.current_href

                try:
                    absolute = urljoin(
                        canonical_url(hub_url),
                        href,
                    )
                except Exception:
                    absolute = href

                self.links.append(
                    {
                        "title": title,
                        "url": absolute,
                    }
                )

            self.current_href = None
            self.current_text = []

    parser = LinkParser()

    try:
        parser.feed(html_source)
    except Exception:
        return []

    return _filter_hub_links(
        parser.links,
        hub_url,
    )


def _extract_hub_links_on_browser(url, request_id, index):
    """
    Browser-owned fallback for hubs whose article links are rendered
    dynamically.
    """
    def task(browser):
        page = browser.new_page()

        try:
            page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=FETCH_TIMEOUT,
            )

            try:
                page.wait_for_timeout(
                    FETCH_SETTLE_MS
                )
            except Exception:
                pass

            links = page.evaluate(
                HUB_LINK_EXTRACT_JS
            )

            return _filter_hub_links(
                links,
                url,
            )

        finally:
            try:
                page.close()
            except Exception:
                pass

    try:
        return browser_executor.call(task)

    except Exception as exc:
        log.warning(
            "[%s] HUB[%d] browser link extraction failed: %s",
            request_id,
            index,
            exc,
        )
        return []


def extract_hub_links(result, request_id, index):
    """
    Obtain exact article links from a hub result.

    HTTP HTML is tried first. If that produces no useful links,
    browser-owned extraction is used.
    """
    url = result.get("url", "")

    if not url:
        return []

    try:
        html_source, http_reason = fetch_http(
            url,
            request_id,
            index,
        )

    except Exception as exc:
        html_source = None
        http_reason = f"http_error:{exc}"

    if html_source:
        links = _extract_hub_links_from_html(
            html_source,
            url,
        )

        if links:
            log.info(
                "[%s] HUB[%d] extracted %d exact links via HTTP",
                request_id,
                index,
                len(links),
            )
            return links

        log.info(
            "[%s] HUB[%d] HTTP returned no useful links reason=%s; "
            "trying CAMOUFOX",
            request_id,
            index,
            http_reason,
        )

    else:
        log.info(
            "[%s] HUB[%d] HTTP retrieval failed reason=%s; "
            "trying CAMOUFOX",
            request_id,
            index,
            http_reason,
        )

    links = _extract_hub_links_on_browser(
        url,
        request_id,
        index,
    )

    log.info(
        "[%s] HUB[%d] extracted %d exact links via CAMOUFOX",
        request_id,
        index,
        len(links),
    )

    return links


def enrich_hub_results(results, request_id):
    """
    Attach exact article links discovered on hub/topic pages.

    Hub results remain ordinary search results. The additional
    'links' field contains URLs copied from the hub page itself.
    """
    hubs = [
        (index, result)
        for index, result in enumerate(
            results,
            start=1,
        )
        if result.get("_is_hub")
    ]

    if not hubs:
        return results

    log.info(
        "[%s] HUB ENRICHMENT count=%d",
        request_id,
        len(hubs),
    )

    for index, result in hubs:
        try:
            links = extract_hub_links(
                result,
                request_id,
                index,
            )

            if links:
                result["links"] = links
            else:
                result["links"] = []

        except Exception as exc:
            log.exception(
                "[%s] HUB[%d] enrichment failed: %s",
                request_id,
                index,
                exc,
            )
            result["links"] = []

    return results


# ============================================================
# PARALLEL CANDIDATES
# ============================================================


def enhance_candidates(results, request_id):
    """
    Enhance up to five article candidates.

    Hub/topic results are preserved but are not article candidates.
    Their exact page links have already been attached by
    enrich_hub_results().

    HTTP retrieval is parallel.

    No worker thread accesses Camoufox or Playwright.

    Readability and Camoufox fallback execute exclusively on the
    dedicated browser-owning thread.

    Processing stops after TARGET_USABLE_ARTICLES usable articles.
    Results which were not processed remain ordinary search results.
    """
    results = enrich_hub_results(
        results,
        request_id,
    )

    candidates = [
        result
        for result in results
        if not result.get("_is_hub", False)
    ][:MAX_CANDIDATES]

    if not candidates:
        return results

    log.info(
        "[%s] CANDIDATES count=%d target_usable=%d",
        request_id,
        len(candidates),
        TARGET_USABLE_ARTICLES,
    )

    candidate_starts = {
        index: time.perf_counter()
        for index in range(1, len(candidates) + 1)
    }

    enhanced_by_index = {}
    usable_count = 0

    # ------------------------------------------------------------
    # Stage 1:
    #
    # Launch HTTP retrieval for all candidates in parallel.
    # These workers NEVER touch the browser.
    # ------------------------------------------------------------

    executor = concurrent.futures.ThreadPoolExecutor(
        max_workers=len(candidates),
        thread_name_prefix="http-fetch",
    )

    try:
        future_map = {
            executor.submit(
                fetch_http,
                result.get("url", ""),
                request_id,
                index,
            ): (index, result)
            for index, result in enumerate(
                candidates,
                start=1,
            )
        }

        # --------------------------------------------------------
        # Stage 2:
        #
        # Consume completed HTTP requests as they arrive.
        # Browser work is synchronised through browser_executor.
        # --------------------------------------------------------

        for future in concurrent.futures.as_completed(future_map):
            index, result = future_map[future]

            try:
                html_source, http_reason = future.result()

            except Exception as exc:
                log.exception(
                    "[%s] FETCH[%d] HTTP WORKER FAILED: %s",
                    request_id,
                    index,
                    exc,
                )

                html_source = None
                http_reason = f"http_worker_error:{exc}"

            enhanced_result = None

            # ----------------------------------------------------
            # HTTP HTML -> browser-owned Readability
            # ----------------------------------------------------

            if html_source is not None:
                try:
                    article = article_from_http(
                        html_source,
                        request_id,
                        index,
                    )

                    usable, text, quality_reason = article_quality(
                        article
                    )

                    if usable:
                        log.info(
                            "[%s] FETCH[%d] HTTP article usable chars=%d",
                            request_id,
                            index,
                            len(text),
                        )

                        enhanced_result = build_summary_result(
                            result,
                            text,
                            request_id,
                            index,
                            "http",
                            candidate_starts[index],
                        )

                    else:
                        log.info(
                            "[%s] FETCH[%d] HTTP article rejected "
                            "quality=%s; trying CAMOUFOX",
                            request_id,
                            index,
                            quality_reason,
                        )

                except Exception as exc:
                    log.exception(
                        "[%s] FETCH[%d] HTTP Readability failed: %s",
                        request_id,
                        index,
                        exc,
                    )

            else:
                log.info(
                    "[%s] FETCH[%d] HTTP retrieval failed reason=%s; "
                    "trying CAMOUFOX",
                    request_id,
                    index,
                    http_reason,
                )

            # ----------------------------------------------------
            # Camoufox fallback, also browser-owner-thread only.
            # ----------------------------------------------------

            if enhanced_result is None:
                try:
                    article = fetch_camoufox(
                        result.get("url", ""),
                        request_id,
                        index,
                    )

                    usable, text, quality_reason = article_quality(
                        article
                    )

                    if usable:
                        log.info(
                            "[%s] FETCH[%d] CAMOUFOX article usable chars=%d",
                            request_id,
                            index,
                            len(text),
                        )

                        enhanced_result = build_summary_result(
                            result,
                            text,
                            request_id,
                            index,
                            "camoufox",
                            candidate_starts[index],
                        )

                    else:
                        log.info(
                            "[%s] FETCH[%d] article unusable after both "
                            "methods reason=%s; using search snippet",
                            request_id,
                            index,
                            quality_reason,
                        )

                except Exception as exc:
                    log.exception(
                        "[%s] FETCH[%d] CAMOUFOX fallback failed: %s",
                        request_id,
                        index,
                        exc,
                    )

            # ----------------------------------------------------
            # If neither route produced a usable article, preserve
            # the normal search result/snippet.
            # ----------------------------------------------------

            if enhanced_result is None:
                enhanced_result = {
                    **result,
                    "content": result.get("snippet", ""),
                    "content_source": "search_snippet",
                    "fetch_method": "none",
                }

            enhanced_by_index[index] = enhanced_result

            if (
                enhanced_result.get("content_source")
                == "article_summary"
            ):
                usable_count += 1

                log.info(
                    "[%s] USABLE ARTICLE count=%d/%d index=%d",
                    request_id,
                    usable_count,
                    TARGET_USABLE_ARTICLES,
                    index,
                )

                # Stop immediately. Do not wait for another future
                # to complete before leaving the loop.
                if usable_count >= TARGET_USABLE_ARTICLES:
                    break

        # --------------------------------------------------------
        # Cancel anything which has not begun.
        #
        # Already-running HTTP requests are NOT waited for here.
        # --------------------------------------------------------

        for future in future_map:
            if not future.done():
                future.cancel()

    finally:
        # Wait for already-running HTTP workers to terminate.
        # This prevents abandoned workers from leaking into the next
        # Calivi request.
        executor.shutdown(
            wait=True,
            cancel_futures=True,
        )

    # ------------------------------------------------------------
    # Rebuild original result ordering.
    # ------------------------------------------------------------

    output = []

    candidate_urls = {
        canonical_url(result.get("url", ""))
        for result in candidates
    }

    processed_urls = {
        canonical_url(
            candidates[index - 1].get("url", "")
        )
        for index in enhanced_by_index
    }

    for result in results:
        url = canonical_url(result.get("url", ""))

        if url in processed_urls:
            candidate_index = next(
                (
                    index
                    for index, candidate in enumerate(
                        candidates,
                        start=1,
                    )
                    if canonical_url(
                        candidate.get("url", "")
                    ) == url
                ),
                None,
            )

            if candidate_index is not None:
                output.append(
                    enhanced_by_index[candidate_index]
                )
                continue

        output.append(result)

    log.info(
        "[%s] CANDIDATES complete usable=%d processed=%d/%d",
        request_id,
        usable_count,
        len(enhanced_by_index),
        len(candidates),
    )

    return output


# ============================================================
# HTTP SERVER
# ============================================================

def log_calivi_payload(request_id, data):
    try:
        payload = json.dumps(
            data,
            ensure_ascii=False,
            indent=2,
        )

        log.info(
            "[%s] CALIVI PAYLOAD BEGIN\\n%s\\n[%s] CALIVI PAYLOAD END",
            request_id,
            payload,
            request_id,
        )

    except Exception:
        log.exception(
            "[%s] FAILED TO LOG CALIVI PAYLOAD",
            request_id,
        )


class SearchHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def send_json(self, status, data):
        def clean_internal_markers(value):
            if isinstance(value, dict):
                return {
                    key: clean_internal_markers(item)
                    for key, item in value.items()
                    if key != "_is_hub"
                }
            if isinstance(value, list):
                return [
                    clean_internal_markers(item)
                    for item in value
                ]
            return value

        data = clean_internal_markers(data)

        log_calivi_payload(
            getattr(self, "request_id", "-"),
            data,
        )

        body = json.dumps(
            data,
            ensure_ascii=False,
        ).encode("utf-8")

        try:
            self.send_response(status)
            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8",
            )
            self.send_header(
                "Content-Length",
                str(len(body)),
            )
            self.end_headers()
            self.wfile.write(body)
            return True

        except (BrokenPipeError, ConnectionResetError):
            log.info(
                "[%s] CLIENT DISCONNECTED before response was sent",
                getattr(self, "request_id", "-"),
            )
            return False

    def do_GET(self):
        parsed = urlparse(self.path)

        # ----------------------------------------------------
        # Health endpoint
        # ----------------------------------------------------

        if parsed.path == "/health":
            self.send_json(
                200,
                {
                    "status": "ok",
                    "service": "search_service",
                    "version": VERSION,
                },
            )
            return

        # ----------------------------------------------------
        # Search endpoint
        # ----------------------------------------------------

        if parsed.path != "/search":
            self.send_json(
                404,
                {"error": "not found"},
            )
            return

        params = parse_qs(parsed.query)

        query = params.get("q", [""])[0].strip()

        try:
            limit = int(
                params.get(
                    "limit",
                    [DEFAULT_LIMIT],
                )[0]
            )
        except ValueError:
            limit = DEFAULT_LIMIT

        limit = max(
            1,
            min(limit, MAX_LIMIT),
        )

        if not query:
            self.send_json(
                400,
                {"error": "missing q parameter"},
            )
            return

        request_id = uuid.uuid4().hex[:8]
        self.request_id = request_id
        request_start = time.perf_counter()

        try:
            with SEARCH_LOCK:
                log.info(
                    "[%s] SEARCH PIPELINE LOCK ACQUIRED",
                    request_id,
                )

                results = search(
                    query,
                    limit,
                    request_id,
                )

                enhanced = enhance_candidates(
                    results,
                    request_id,
                )

                log.info(
                    "[%s] SEARCH PIPELINE LOCK RELEASED",
                    request_id,
                )

            total = time.perf_counter() - request_start

            usable = sum(
                1
                for result in enhanced
                if result.get("content_source")
                == "article_summary"
            )

            log.info(
                "[%s] REQUEST complete total=%.3fs "
                "results=%d usable_articles=%d",
                request_id,
                total,
                len(enhanced),
                usable,
            )

            self.send_json(
                200,
                {
                    "query": query,
                    "results": enhanced,
                },
            )

        except Exception as exc:
            total = time.perf_counter() - request_start

            log.exception(
                "[%s] REQUEST FAILED total=%.3fs error=%s",
                request_id,
                total,
                exc,
            )

            self.send_json(
                500,
                {"error": str(exc)},
            )


# ============================================================
# MAIN
# ============================================================

def main():
    start_browser_executor()

    server = ThreadingHTTPServer(
        (HOST, PORT),
        SearchHandler,
    )

    log.info(
        "search_service v%s listening on http://%s:%d",
        VERSION,
        HOST,
        PORT,
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        log.info("SHUTTING DOWN")

    finally:
        server.server_close()
        shutdown_browser()


if __name__ == "__main__":
    main()
