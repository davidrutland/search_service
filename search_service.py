#!/usr/bin/env python3
"""
search_service v4.0

Lean, high-precision web retrieval service for LLMs.

Pipeline:

    DuckDuckGo
        ↓
    Brave fallback
        ↓
    canonicalise / deduplicate / SSRF validation
        ↓
    discard obvious hubs
        ↓
    HTTP fetch
        ↓
    Readability extraction
        ↓
    article-quality / link-density filtering
        ↓
    Camoufox fallback for JS-heavy pages
        ↓
    LexRank summary
        ↓
    return first usable enhanced articles
        +
    original search-engine snippets as fallbacks
"""

from __future__ import annotations

import html
import http.server
import ipaddress
import json
import logging
import logging.handlers
import re
import socket
import threading
import time

from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse

import httpx
from camoufox.sync_api import Camoufox
from sumy.nlp.tokenizers import Tokenizer
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.lex_rank import LexRankSummarizer


# ============================================================================
# Configuration
# ============================================================================

HOST = "0.0.0.0"
PORT = 8787

DEFAULT_LIMIT = 3
MAX_LIMIT = 20
MAX_CANDIDATES = 10

SEARCH_TIMEOUT = 10000      # milliseconds for Playwright
HTTP_TIMEOUT = 10.0
BROWSER_TIMEOUT = 10000     # milliseconds for Playwright
BROWSER_SETTLE_MS = 250

MIN_ARTICLE_CHARS = 500
MAX_SUMMARY_CHARS = 32768
MAX_CONTENT_BYTES = 2 * 1024 * 1024
MAX_ARTICLE_TEXT_BYTES = 10 * 1024 * 1024

SUMMARY_SENTENCES = 3
MAX_REDIRECTS = 10

SCRIPT_DIR = Path(__file__).resolve().parent
READABILITY_JS_PATH = SCRIPT_DIR / "lib" / "Readability.js"

# Change this for another installation.
LOG_DIR = "/home/david/AI/camoufox/logs"

LOG_MAX_BYTES = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

VERSION = "4.0"

# v3.x used this as a latency guard. Two genuinely usable articles are
# normally enough for an LLM retrieval call. If you want the service to
# exhaustively try all requested results, set this equal to MAX_LIMIT.
TARGET_USABLE_ARTICLES = 2

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Safari/537.36"
)

SEARCH_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,*/*;q=0.8"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}

HTTP_HEADERS = {
    **SEARCH_HEADERS,
    "Cache-Control": "no-cache",
}


# ============================================================================
# Logging
# ============================================================================

logger = logging.getLogger("search_service")
logger.setLevel(logging.INFO)
logger.propagate = False

Path(LOG_DIR).mkdir(parents=True, exist_ok=True)

log_path = (
    Path(LOG_DIR)
    / f"search_service_{time.strftime('%Y-%m-%d_%H%M%S')}.log"
)

log_handler = logging.handlers.RotatingFileHandler(
    log_path,
    maxBytes=LOG_MAX_BYTES,
    backupCount=LOG_BACKUP_COUNT,
    encoding="utf-8",
)

log_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s %(levelname)s %(threadName)s %(message)s"
    )
)

logger.addHandler(log_handler)


# ============================================================================
# SSRF protection
# ============================================================================

# Explicitly blocked IPv4 ranges.
#
# ipaddress.is_global is also used below as the final gate, so this list is
# intentionally defensive rather than exhaustive.

BLOCKED_SUBNETS = tuple(
    ipaddress.ip_network(net)
    for net in (
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

# Explicitly blocked IPv6 ranges.

BLOCKED_IPV6_SUBNETS = tuple(
    ipaddress.ip_network(net)
    for net in (
        "::/128",
        "::1/128",
        "::ffff:0:0/96",
        "fc00::/7",
        "fe80::/10",
        "ff00::/8",
        "2001:db8::/32",
    )
)


def _is_ip_blocked(
    address: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> bool:
    """
    Return True if an address is not safe for outbound retrieval.

    Global IPv6 is deliberately allowed. In particular, do NOT add
    2000::/3 to BLOCKED_IPV6_SUBNETS.
    """

    if address.version == 4:
        if any(address in subnet for subnet in BLOCKED_SUBNETS):
            return True
    else:
        if any(address in subnet for subnet in BLOCKED_IPV6_SUBNETS):
            return True

    # This catches private/reserved/unspecified/etc. ranges not explicitly
    # enumerated above.
    return not address.is_global


def _resolve_host(
    hostname: str,
) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """Resolve A/AAAA records and return unique parsed addresses."""

    addresses = []
    seen: set[str] = set()

    try:
        infos = socket.getaddrinfo(
            hostname,
            None,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except (socket.gaierror, OSError):
        return []

    for info in infos:
        sockaddr = info[4]

        if not sockaddr:
            continue

        try:
            address = ipaddress.ip_address(sockaddr[0])
        except ValueError:
            continue

        key = str(address)

        if key not in seen:
            seen.add(key)
            addresses.append(address)

    return addresses


def is_url_safe(url: str) -> bool:
    """
    Validate a URL before making a network request.

    DNS failure is treated as unsafe.

    If a hostname resolves to even one blocked/private/reserved address,
    the hostname is rejected. This prevents simple DNS round-robin /
    rebinding cases where one returned address is internal.
    """

    try:
        parsed = urlparse(url)

        scheme = parsed.scheme.lower()
        hostname = parsed.hostname

        if scheme not in {"http", "https"}:
            return False

        if not hostname:
            return False

        # Userinfo is unnecessary for search retrieval and can leak secrets.
        if parsed.username is not None or parsed.password is not None:
            return False

        try:
            port = parsed.port
        except ValueError:
            return False

        if port is not None and not 1 <= port <= 65535:
            return False

        # Literal IP.
        try:
            literal = ipaddress.ip_address(hostname)
        except ValueError:
            literal = None

        if literal is not None:
            return not _is_ip_blocked(literal)

        # Hostname.
        resolved = _resolve_host(hostname)

        # Fail closed on DNS failure.
        if not resolved:
            return False

        # If any DNS answer is internal/private/reserved, reject the host.
        return all(not _is_ip_blocked(address) for address in resolved)

    except (ValueError, UnicodeError):
        return False


# ============================================================================
# General helpers
# ============================================================================

def clean_text(value: Any) -> str:
    if value is None:
        return ""

    return re.sub(
        r"\s+",
        " ",
        html.unescape(str(value)),
    ).strip()


def decode_ddg_url(url: str) -> str:
    try:
        parsed = urlparse(url)

        if parsed.path == "/l/" and parsed.query:
            target = parse_qs(parsed.query).get("uddg")

            if target:
                return unquote(target[0])

    except Exception:
        pass

    return url


def canonical_url(url: str) -> str:
    """
    Canonicalise enough to deduplicate normal search-engine URL variants.

    Query parameters are retained because they can be semantically important.
    Fragments are discarded.
    """

    try:
        parsed = urlparse(decode_ddg_url(url))

        scheme = parsed.scheme.lower()
        hostname = (parsed.hostname or "").lower()

        if hostname.startswith("www."):
            hostname = hostname[4:]

        port = parsed.port

        if port is not None and not (
            (scheme == "http" and port == 80)
            or (scheme == "https" and port == 443)
        ):
            netloc = f"{hostname}:{port}"
        else:
            netloc = hostname

        path = parsed.path or "/"

        if path != "/":
            path = path.rstrip("/")

        return parsed._replace(
            scheme=scheme,
            netloc=netloc,
            path=path,
            fragment="",
        ).geturl()

    except Exception:
        return url


def is_html_content(content_type: str) -> bool:
    content_type = (content_type or "").lower()

    return (
        "text/html" in content_type
        or "application/xhtml+xml" in content_type
    )


def is_probably_html(text: str) -> bool:
    sample = text[:2000].lstrip().lower()

    return (
        "<!doctype html" in sample
        or "<html" in sample
        or "<head" in sample
        or "<body" in sample
    )


# ============================================================================
# Search-result filtering
# ============================================================================

DDG_AD_PATTERNS = (
    "duckduckgo.com/y.js",
    "ad_type=txad",
    "ad_provider=",
    "ad_domain=",
)

# Known high-level hub paths for sites where these are especially common.
#
# "/" means exact root only. It must NOT be treated as a prefix.
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


def is_ddg_ad(url: str) -> bool:
    lowered = url.lower()

    return any(
        pattern in lowered
        for pattern in DDG_AD_PATTERNS
    )


def _hub_path_matches(
    path: str,
    prefixes: tuple[str, ...],
) -> bool:
    for prefix in prefixes:

        # Root is an exact match, never a prefix match.
        if prefix == "/":
            if path == "/":
                return True

            continue

        prefix = prefix.rstrip("/")

        if path == prefix:
            return True

        if path.startswith(prefix + "/"):
            return True

    return False


def is_obvious_hub(url: str) -> bool:
    """
    Reject obvious section/search/tag/category pages.

    This is deliberately conservative. The deeper article-quality checks
    provide the second line of defence.
    """

    try:
        parsed = urlparse(url)

        host = (
            parsed.hostname or ""
        ).lower().removeprefix("www.")

        path = parsed.path.rstrip("/") or "/"

        for known_host, prefixes in KNOWN_HUB_RULES.items():
            if host == known_host and _hub_path_matches(
                path,
                prefixes,
            ):
                return True

        segments = [
            part.lower()
            for part in path.split("/")
            if part
        ]

        if segments and segments[0] in GENERIC_HUB_SEGMENTS:
            return True

        if path in {
            "/search",
            "/feed",
            "/rss",
            "/sitemap.xml",
        }:
            return True

        return False

    except Exception:
        # Malformed URLs should never become final candidates.
        return True


def normalise_search_result(
    url: str,
    title: str,
    snippet: str,
) -> dict[str, Any] | None:
    url = canonical_url(url)
    title = clean_text(title)
    snippet = clean_text(snippet)

    if not url or not title:
        return None

    if not is_url_safe(url):
        return None

    if is_obvious_hub(url):
        return None

    return {
        "url": url,
        "title": title,
        "snippet": snippet,
    }


# ============================================================================
# Dedicated Camoufox owner thread
# ============================================================================

class BrowserExecutor:
    """
    Owns Camoufox and every Playwright object from one dedicated thread.

    Playwright objects are never passed between HTTP worker threads.
    """

    def __init__(self) -> None:
        self._tasks: list[
            tuple[Callable[[Any], Any], Future[Any]]
        ] = []

        self._condition = threading.Condition()
        self._stopping = False

        self._ready = threading.Event()
        self._startup_error: BaseException | None = None

        self._thread = threading.Thread(
            target=self._run,
            name="camoufox-owner",
            daemon=True,
        )

        self._thread.start()

        self._ready.wait(timeout=60)

        if self._startup_error is not None:
            raise RuntimeError(
                f"Camoufox startup failed: {self._startup_error}"
            )

        if not self._ready.is_set():
            raise RuntimeError(
                "Timed out waiting for Camoufox startup"
            )

    def _run(self) -> None:
        try:
            with Camoufox(
                headless=True,
                locale="en-GB",
            ) as browser:

                self._ready.set()

                while True:
                    with self._condition:

                        while (
                            not self._tasks
                            and not self._stopping
                        ):
                            self._condition.wait()

                        if (
                            self._stopping
                            and not self._tasks
                        ):
                            return

                        task, future = self._tasks.pop(0)

                    if future.cancelled():
                        continue

                    try:
                        result = task(browser)

                    except BaseException as exc:
                        if not future.cancelled():
                            future.set_exception(exc)

                    else:
                        if not future.cancelled():
                            future.set_result(result)

        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()

            with self._condition:
                pending = self._tasks
                self._tasks = []

            for _, future in pending:
                if not future.cancelled():
                    future.set_exception(exc)

    def call(
        self,
        task: Callable[[Any], Any],
        timeout: float = 30.0,
    ) -> Any:
        future: Future[Any] = Future()

        with self._condition:
            if self._stopping:
                raise RuntimeError(
                    "Browser executor is stopping"
                )

            self._tasks.append((task, future))
            self._condition.notify()

        return future.result(timeout=timeout)

    def shutdown(self) -> None:
        with self._condition:
            self._stopping = True
            self._condition.notify_all()

        self._thread.join(timeout=15)


browser_executor: BrowserExecutor | None = None

# Prevent multiple simultaneous browser searches from fighting over the
# dedicated Camoufox instance. HTTP article fetches remain concurrent.
SEARCH_LOCK = threading.Lock()


# ============================================================================
# Search engines
# ============================================================================

def _search_ddg_on_browser(
    browser: Any,
    query: str,
) -> list[dict[str, Any]]:

    page = browser.new_page()

    try:
        url = (
            "https://html.duckduckgo.com/html/?q="
            + quote(query)
        )

        page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=SEARCH_TIMEOUT,
        )

        page.wait_for_timeout(BROWSER_SETTLE_MS)

        rows = page.evaluate(
            """
            () => Array.from(
                document.querySelectorAll(".result")
            ).map(el => {
                const a = el.querySelector("a.result__a");
                const s = el.querySelector(".result__snippet");

                return {
                    url: a ? a.href : "",
                    title: a ? a.textContent : "",
                    snippet: s ? s.textContent : ""
                };
            })
            """
        )

        results: list[dict[str, Any]] = []
        seen: set[str] = set()

        for row in rows:

            raw_url = row.get("url", "")

            if is_ddg_ad(raw_url):
                continue

            item = normalise_search_result(
                decode_ddg_url(raw_url),
                row.get("title", ""),
                row.get("snippet", ""),
            )

            if not item:
                continue

            key = canonical_url(item["url"])

            if key in seen:
                continue

            seen.add(key)
            results.append(item)

            if len(results) >= MAX_CANDIDATES:
                break

        return results

    finally:
        page.close()


def _search_brave_on_browser(
    browser: Any,
    query: str,
) -> list[dict[str, Any]]:

    page = browser.new_page()

    try:
        url = (
            "https://search.brave.com/search?q="
            + quote(query)
        )

        page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=SEARCH_TIMEOUT,
        )

        page.wait_for_timeout(BROWSER_SETTLE_MS)

        rows = page.evaluate(
            """
            () => {
                const selectors = [
                    ".snippet",
                    "[data-type='search-result']",
                    ".snippet-content"
                ];

                const nodes = [];
                const seen = new Set();

                for (const selector of selectors) {
                    for (
                        const el
                        of document.querySelectorAll(selector)
                    ) {
                        if (!seen.has(el)) {
                            seen.add(el);
                            nodes.push(el);
                        }
                    }
                }

                return nodes.map(el => {
                    const a =
                        el.querySelector("a.result-header") ||
                        el.querySelector("a[href]");

                    const s =
                        el.querySelector(
                            ".snippet-description"
                        ) ||
                        el.querySelector(
                            ".snippet-description-container"
                        ) ||
                        el.querySelector("[data-snippet]");

                    return {
                        url: a ? a.href : "",
                        title: a ? a.textContent : "",
                        snippet: s ? s.textContent : ""
                    };
                });
            }
            """
        )

        results: list[dict[str, Any]] = []
        seen: set[str] = set()

        for row in rows:

            item = normalise_search_result(
                row.get("url", ""),
                row.get("title", ""),
                row.get("snippet", ""),
            )

            if not item:
                continue

            key = canonical_url(item["url"])

            if key in seen:
                continue

            seen.add(key)
            results.append(item)

            if len(results) >= MAX_CANDIDATES:
                break

        return results

    finally:
        page.close()


def search(query: str) -> list[dict[str, Any]]:
    if browser_executor is None:
        raise RuntimeError(
            "Browser executor is not running"
        )

    logger.info(
        "search start query=%r",
        query,
    )

    try:
        ddg_results = browser_executor.call(
            lambda browser: _search_ddg_on_browser(
                browser,
                query,
            ),
            timeout=30,
        )

        logger.info(
            "DDG returned %d candidates",
            len(ddg_results),
        )

        for i, r in enumerate(ddg_results, 1):
            logger.info(
                "DDG #%d URL=%s SNIPPET=%r",
                i,
                r.get("url", ""),
                (r.get("snippet", "") or "").replace("\\n", " ")[:500],
            )

        if ddg_results:
            return ddg_results

    except Exception as exc:
        logger.warning(
            "DDG search failed: %s",
            exc,
        )

    try:
        brave_results = browser_executor.call(
            lambda browser: _search_brave_on_browser(
                browser,
                query,
            ),
            timeout=30,
        )

        logger.info(
            "Brave returned %d candidates",
            len(brave_results),
        )

        for i, r in enumerate(brave_results, 1):
            logger.info(
                "Brave #%d URL=%s SNIPPET=%r",
                i,
                r.get("url", ""),
                (r.get("snippet", "") or "").replace("\\n", " ")[:500],
            )

        return brave_results

    except Exception as exc:
        logger.warning(
            "Brave search failed: %s",
            exc,
        )

        return []


# ============================================================================
# Readability
# ============================================================================

READABILITY_SOURCE: str | None = None
READABILITY_LOCK = threading.Lock()


def get_readability_source() -> str:
    global READABILITY_SOURCE

    if READABILITY_SOURCE is None:
        with READABILITY_LOCK:
            if READABILITY_SOURCE is None:
                READABILITY_SOURCE = (
                    READABILITY_JS_PATH.read_text(
                        encoding="utf-8"
                    )
                )

    return READABILITY_SOURCE


def html_to_text(source: str) -> str:
    source = re.sub(
        r"<(script|style|noscript)\b[^>]*>.*?</\1>",
        " ",
        source,
        flags=re.I | re.S,
    )

    source = re.sub(
        r"<[^>]+>",
        " ",
        source,
    )

    return clean_text(source)


class _LinkMetricsParser(HTMLParser):
    """
    Measure visible text and text contained inside links.

    This is used after Readability extraction to identify pages which still
    look like hubs rather than deep articles.
    """

    def __init__(self) -> None:
        super().__init__(
            convert_charrefs=True
        )

        self.text_chars = 0
        self.link_text_chars = 0
        self.link_count = 0
        self._in_link = 0

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:

        if tag.lower() == "a":
            self.link_count += 1
            self._in_link += 1

    def handle_endtag(
        self,
        tag: str,
    ) -> None:

        if (
            tag.lower() == "a"
            and self._in_link
        ):
            self._in_link -= 1

    def handle_data(
        self,
        data: str,
    ) -> None:

        n = len(clean_text(data))

        if not n:
            return

        self.text_chars += n

        if self._in_link:
            self.link_text_chars += n


def article_link_metrics(
    content_html: str,
) -> dict[str, float | int]:

    parser = _LinkMetricsParser()

    try:
        parser.feed(content_html)
        parser.close()
    except Exception:
        pass

    text_chars = parser.text_chars
    link_chars = parser.link_text_chars
    links = parser.link_count

    return {
        "text_chars": text_chars,
        "link_chars": link_chars,
        "links": links,
        "link_density": (
            link_chars / max(text_chars, 1)
        ),
        "links_per_1000_chars": (
            links * 1000 / max(text_chars, 1)
        ),
    }


def article_quality(
    article: dict[str, Any],
) -> tuple[bool, str]:

    if not article:
        return False, "readability_empty"

    if article.get("error"):
        return False, "readability_error"

    content_html = article.get("content") or ""

    text = html_to_text(content_html)

    if len(text) < MIN_ARTICLE_CHARS:
        return False, "too_short"

    if len(text.encode("utf-8")) > MAX_ARTICLE_TEXT_BYTES:
        text = text[
            : int(MAX_ARTICLE_TEXT_BYTES * 0.8)
        ]

    metrics = article_link_metrics(
        content_html
    )

    links = int(metrics["links"])
    density = float(
        metrics["link_density"]
    )
    links_per_1000 = float(
        metrics["links_per_1000_chars"]
    )

    # Conservative heuristics. These are deliberately not aggressive enough
    # to reject ordinary articles simply because they contain links.
    if links >= 20 and density > 0.25:
        return False, "high_link_density"

    if links >= 40 and links_per_1000 > 12:
        return False, "hub_like_link_density"

    if (
        len(text) < 1200
        and links >= 15
        and density > 0.20
    ):
        return False, "short_link_heavy_page"

    return True, "ok"


def extract_article_on_page(
    page: Any,
) -> dict[str, Any] | None:

    source = get_readability_source()

    wrapper = f"""
    (() => {{
        try {{
            {source}

            const doc = document.cloneNode(true);
            const reader = new Readability(doc);
            const article = reader.parse();

            if (!article) return null;

            return {{
                title: article.title || "",
                content: article.content || "",
                textContent: article.textContent || "",
                excerpt: article.excerpt || "",
                length: article.length || 0
            }};

        }} catch (e) {{
            return {{
                error: String(e)
            }};
        }}
    }})()
    """

    return page.evaluate(wrapper)


def extract_article_from_html(
    html_source: str,
) -> dict[str, Any] | None:

    if browser_executor is None:
        raise RuntimeError(
            "Browser executor is not running"
        )

    def task(
        browser: Any,
    ) -> dict[str, Any] | None:

        page = browser.new_page()

        try:
            page.set_content(
                html_source,
                wait_until="domcontentloaded",
                # BROWSER_TIMEOUT is already expressed in milliseconds
                # (see the config block above) -- Playwright's `timeout`
                # kwarg also expects milliseconds, so it must be passed
                # through unscaled. Multiplying by 1000 here previously
                # turned a 10-second budget into ~10,000 seconds.
                timeout=BROWSER_TIMEOUT,
            )

            return extract_article_on_page(page)

        finally:
            page.close()

    return browser_executor.call(
        task,
        timeout=30,
    )


# ============================================================================
# Summarisation
# ============================================================================

def summarize(text: str) -> str:
    text = clean_text(text)

    if not text:
        return ""

    try:
        parser = PlaintextParser.from_string(
            text,
            Tokenizer("english"),
        )

        summarizer = LexRankSummarizer()

        sentences = summarizer(
            parser.document,
            SUMMARY_SENTENCES,
        )

        result = clean_text(
            " ".join(
                str(sentence)
                for sentence in sentences
            )
        )

        if result:
            return result[:MAX_SUMMARY_CHARS]

    except Exception as exc:
        logger.warning(
            "LexRank failed: %s",
            exc,
        )

    # Never return an empty result merely because the summariser failed.
    return text[
        : min(
            MAX_SUMMARY_CHARS,
            2000,
        )
    ]


# ============================================================================
# HTTP fetch
# ============================================================================

def fetch_http(
    url: str,
) -> dict[str, Any]:

    start = time.perf_counter()

    metadata = {
        "final_url": url,
        "status_code": 0,
        "content_type": "",
        "bytes_read": 0,
        "reason": "",
    }

    current_url = canonical_url(url)

    for hop in range(
        MAX_REDIRECTS + 1
    ):

        # Every hop is independently checked.
        if not is_url_safe(current_url):
            metadata["reason"] = "ssrf_blocked"

            return {
                "ok": False,
                "html": "",
                "metadata": metadata,
                "elapsed": (
                    time.perf_counter()
                    - start
                ),
            }

        try:
            with httpx.Client(
                follow_redirects=False,
                timeout=HTTP_TIMEOUT,
                headers=HTTP_HEADERS,
                trust_env=False,
            ) as client:

                with client.stream(
                    "GET",
                    current_url,
                ) as response:

                    metadata["status_code"] = (
                        response.status_code
                    )

                    metadata["content_type"] = (
                        response.headers.get(
                            "content-type",
                            "",
                        )
                    )

                    metadata["final_url"] = (
                        current_url
                    )

                    # --------------------------------------------------------
                    # Redirect
                    # --------------------------------------------------------

                    if 300 <= response.status_code < 400:

                        location = (
                            response.headers.get(
                                "location"
                            )
                        )

                        if not location:
                            metadata["reason"] = (
                                "redirect_without_location"
                            )

                            return {
                                "ok": False,
                                "html": "",
                                "metadata": metadata,
                                "elapsed": (
                                    time.perf_counter()
                                    - start
                                ),
                            }

                        if hop >= MAX_REDIRECTS:
                            metadata["reason"] = (
                                "too_many_redirects"
                            )

                            return {
                                "ok": False,
                                "html": "",
                                "metadata": metadata,
                                "elapsed": (
                                    time.perf_counter()
                                    - start
                                ),
                            }

                        current_url = canonical_url(
                            urljoin(
                                current_url,
                                location,
                            )
                        )

                        # Loop back around. The new URL will be SSRF checked
                        # before the next network request.
                        continue

                    # --------------------------------------------------------
                    # HTTP status
                    # --------------------------------------------------------

                    if not (
                        200
                        <= response.status_code
                        < 300
                    ):
                        metadata["reason"] = (
                            f"http_status_"
                            f"{response.status_code}"
                        )

                        return {
                            "ok": False,
                            "html": "",
                            "metadata": metadata,
                            "elapsed": (
                                time.perf_counter()
                                - start
                            ),
                        }

                    # --------------------------------------------------------
                    # Size
                    # --------------------------------------------------------

                    content_length = (
                        response.headers.get(
                            "content-length"
                        )
                    )

                    if content_length:

                        try:
                            if (
                                int(content_length)
                                > MAX_CONTENT_BYTES
                            ):
                                metadata["reason"] = (
                                    "content_too_large"
                                )

                                return {
                                    "ok": False,
                                    "html": "",
                                    "metadata": metadata,
                                    "elapsed": (
                                        time.perf_counter()
                                        - start
                                    ),
                                }

                        except ValueError:
                            pass

                    chunks: list[bytes] = []
                    total = 0

                    for chunk in response.iter_bytes():

                        total += len(chunk)

                        if (
                            total
                            > MAX_CONTENT_BYTES
                        ):
                            metadata["bytes_read"] = total
                            metadata["reason"] = (
                                "content_too_large"
                            )

                            return {
                                "ok": False,
                                "html": "",
                                "metadata": metadata,
                                "elapsed": (
                                    time.perf_counter()
                                    - start
                                ),
                            }

                        chunks.append(chunk)

                    body = b"".join(chunks)

                    metadata["bytes_read"] = len(body)

                    text = body.decode(
                        "utf-8",
                        errors="replace",
                    )

                    # Some badly behaved sites omit Content-Type. Allow them
                    # only if the body actually looks like HTML.
                    if not is_html_content(
                        metadata["content_type"]
                    ):
                        if not (
                            not metadata["content_type"]
                            and is_probably_html(text)
                        ):
                            metadata["reason"] = (
                                "not_html"
                            )

                            return {
                                "ok": False,
                                "html": "",
                                "metadata": metadata,
                                "elapsed": (
                                    time.perf_counter()
                                    - start
                                ),
                            }

                    metadata["reason"] = "ok"

                    return {
                        "ok": True,
                        "html": text,
                        "metadata": metadata,
                        "elapsed": (
                            time.perf_counter()
                            - start
                        ),
                    }

        except httpx.TimeoutException:
            metadata["reason"] = "http_timeout"

        except httpx.HTTPError as exc:
            metadata["reason"] = (
                "http_error:"
                f"{type(exc).__name__}"
            )

        except Exception as exc:
            metadata["reason"] = (
                "fetch_error:"
                f"{type(exc).__name__}"
            )

        # Network failures do not make the same URL safer on retry. Return
        # here and let the caller move to Camoufox.
        return {
            "ok": False,
            "html": "",
            "metadata": metadata,
            "elapsed": (
                time.perf_counter()
                - start
            ),
        }

    metadata["reason"] = "redirect_loop"

    return {
        "ok": False,
        "html": "",
        "metadata": metadata,
        "elapsed": (
            time.perf_counter()
            - start
        ),
    }


# ============================================================================
# Camoufox fallback
# ============================================================================

def _browser_request_is_safe(
    request_url: str,
) -> bool:

    try:
        scheme = urlparse(
            request_url
        ).scheme.lower()

        if scheme in {"http", "https"}:
            return is_url_safe(
                request_url
            )

        if scheme in {"ws", "wss"}:
            translated = (
                request_url
                .replace(
                    "ws://",
                    "http://",
                    1,
                )
                .replace(
                    "wss://",
                    "https://",
                    1,
                )
            )

            return is_url_safe(
                translated
            )

        # Browser local-resource protocols are not allowed.
        if scheme in {"file", "ftp"}:
            return False

        # data:, blob:, about:, etc. do not directly make an arbitrary
        # external network request.
        return True

    except Exception:
        return False


def fetch_browser(
    url: str,
) -> dict[str, Any]:

    start = time.perf_counter()

    metadata = {
        "final_url": url,
        "status_code": 0,
        "content_type": "",
        "bytes_read": 0,
        "reason": "",
    }

    if not is_url_safe(url):
        metadata["reason"] = "ssrf_blocked"

        return {
            "ok": False,
            "article": None,
            "metadata": metadata,
            "elapsed": (
                time.perf_counter()
                - start
            ),
        }

    if browser_executor is None:
        metadata["reason"] = (
            "browser_unavailable"
        )

        return {
            "ok": False,
            "article": None,
            "metadata": metadata,
            "elapsed": (
                time.perf_counter()
                - start
            ),
        }

    def task(
        browser: Any,
    ) -> dict[str, Any]:

        page = browser.new_page()

        def route_handler(
            route: Any,
            request: Any,
        ) -> None:

            if _browser_request_is_safe(
                request.url
            ):
                route.continue_()
            else:
                logger.warning(
                    "Camoufox blocked unsafe request: %s",
                    request.url,
                )

                route.abort(
                    "blockedbyclient"
                )

        # This catches navigation redirects and requests initiated by
        # page JavaScript.
        page.route(
            "**/*",
            route_handler,
        )

        try:
            response = page.goto(
                url,
                wait_until="domcontentloaded",
                # BROWSER_TIMEOUT is already in milliseconds; see the
                # matching note in extract_article_from_html above.
                timeout=BROWSER_TIMEOUT,
            )

            page.wait_for_timeout(
                BROWSER_SETTLE_MS
            )

            final_url = page.url or url

            # Defence in depth: explicitly check the final browser URL too.
            if not is_url_safe(final_url):
                return {
                    "ok": False,
                    "article": None,
                    "metadata": {
                        **metadata,
                        "final_url": final_url,
                        "reason": (
                            "ssrf_blocked_final_url"
                        ),
                    },
                }

            status_code = 0
            content_type = ""

            if response is not None:
                try:
                    status_code = response.status

                    content_type = (
                        response.headers.get(
                            "content-type",
                            "",
                        )
                    )

                    content_length = (
                        response.headers.get(
                            "content-length"
                        )
                    )

                    if content_length:
                        try:
                            metadata["bytes_read"] = (
                                int(content_length)
                            )
                        except ValueError:
                            pass

                except Exception:
                    pass

            if (
                content_type
                and not is_html_content(
                    content_type
                )
            ):
                return {
                    "ok": False,
                    "article": None,
                    "metadata": {
                        **metadata,
                        "final_url": final_url,
                        "status_code": status_code,
                        "content_type": content_type,
                        "reason": "not_html",
                    },
                }

            article = extract_article_on_page(
                page
            )

            return {
                "ok": True,
                "article": article,
                "metadata": {
                    **metadata,
                    "final_url": final_url,
                    "status_code": status_code,
                    "content_type": content_type,
                    "reason": "ok",
                },
            }

        finally:
            page.close()

    try:
        result = browser_executor.call(
            task,
            timeout=30,
        )

        result["elapsed"] = (
            time.perf_counter()
            - start
        )

        return result

    except Exception as exc:
        metadata["reason"] = (
            "browser_error:"
            f"{type(exc).__name__}"
        )

        return {
            "ok": False,
            "article": None,
            "metadata": metadata,
            "elapsed": (
                time.perf_counter()
                - start
            ),
        }


# ============================================================================
# Candidate enhancement
# ============================================================================

def build_summary_result(
    candidate: dict[str, Any],
    article: dict[str, Any],
    fetch_method: str,
    metadata: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:

    text = clean_text(
        article.get(
            "textContent",
            "",
        )
    )

    if not text:
        text = html_to_text(
            article.get(
                "content",
                "",
            )
        )

    summary = summarize(text)

    title = (
        clean_text(
            article.get(
                "title"
            )
        )
        or candidate["title"]
    )

    return {
        # Preserve the original search-engine URL and snippet alongside
        # the enhanced article result.
        "url": candidate["url"],
        "title": title,
        "snippet": candidate.get("snippet", ""),

        # README v4.0 field.
        "summary": summary,

        # v3.x/Calivi compatibility alias.
        "content": summary,

        "content_source": "article_summary",
        "fetch_method": fetch_method,
        "extract_time": round(
            elapsed,
            3,
        ),

        "metadata": {
            "final_url": metadata.get(
                "final_url",
                candidate["url"],
            ),
            "status_code": metadata.get(
                "status_code",
                0,
            ),
            "content_type": metadata.get(
                "content_type",
                "",
            ),
            "bytes_read": metadata.get(
                "bytes_read",
                0,
            ),
            "reason": metadata.get(
                "reason",
                "ok",
            ),
        },
    }


def enhance_one(
    candidate: dict[str, Any],
) -> dict[str, Any] | None:

    started = time.perf_counter()

    # ------------------------------------------------------------
    # Preferred path: HTTP
    # ------------------------------------------------------------

    http_result = fetch_http(
        candidate["url"]
    )

    if http_result["ok"]:

        try:
            article = extract_article_from_html(
                http_result["html"]
            )

            good, reason = article_quality(
                article or {}
            )

            if good:
                return build_summary_result(
                    candidate,
                    article or {},
                    "http",
                    http_result["metadata"],
                    time.perf_counter()
                    - started,
                )

            logger.info(
                "HTTP article rejected "
                "url=%s reason=%s",
                candidate["url"],
                reason,
            )

        except Exception as exc:
            logger.warning(
                "HTTP extraction failed "
                "url=%s error=%s",
                candidate["url"],
                exc,
            )

    # ------------------------------------------------------------
    # Fallback: Camoufox
    # ------------------------------------------------------------

    browser_result = fetch_browser(
        candidate["url"]
    )

    if browser_result["ok"]:

        article = browser_result.get(
            "article"
        )

        good, reason = article_quality(
            article or {}
        )

        if good:
            return build_summary_result(
                candidate,
                article or {},
                "camoufox",
                browser_result["metadata"],
                time.perf_counter()
                - started,
            )

        logger.info(
            "Browser article rejected "
            "url=%s reason=%s",
            candidate["url"],
            reason,
        )

    logger.info(
        "candidate unusable "
        "url=%s http_reason=%s "
        "browser_reason=%s",
        candidate["url"],
        http_result["metadata"].get(
            "reason"
        ),
        browser_result["metadata"].get(
            "reason"
        ),
    )

    return None


RAW_FALLBACK_COUNT = 3
ENHANCEMENT_WORKERS = 3


def enhance_candidates(
    candidates: list[dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:

    if not candidates or limit <= 0:
        return []

    candidates = candidates[
        :MAX_CANDIDATES
    ]

    # Two successful article enhancements are the latency/quality target.
    # Raw search results remain available independently of enhancement.
    target = min(
        limit,
        TARGET_USABLE_ARTICLES,
    )

    # For normal LLM retrieval calls, return at most:
    #   2 enhanced + 3 original search results
    #
    # A smaller API limit still acts as an upper bound.
    output_limit = min(
        limit,
        TARGET_USABLE_ARTICLES + RAW_FALLBACK_COUNT,
    )

    enhanced: list[
        tuple[int, dict[str, Any]]
    ] = []

    executor = ThreadPoolExecutor(
        max_workers=min(
            ENHANCEMENT_WORKERS,
            len(candidates),
        ),
        thread_name_prefix="fetch",
    )

    # Keep only a small number of candidates in flight. This avoids
    # launching ten potentially slow HTTP/Camoufox operations just to use
    # the first two successful results.
    next_index = 0
    future_to_index: dict[
        Future[Any],
        int,
    ] = {}

    def submit_next() -> bool:
        nonlocal next_index

        if next_index >= len(candidates):
            return False

        index = next_index
        next_index += 1

        future = executor.submit(
            enhance_one,
            candidates[index],
        )

        future_to_index[future] = index
        return True

    try:
        for _ in range(
            min(
                ENHANCEMENT_WORKERS,
                len(candidates),
            )
        ):
            submit_next()

        while future_to_index:

            # as_completed() yields whichever candidate finishes first.
            # Therefore the first two suitable articles win on latency,
            # rather than simply the first two search-engine candidates.
            for future in as_completed(
                list(future_to_index)
            ):
                index = future_to_index.pop(
                    future
                )

                try:
                    result = future.result()

                except Exception as exc:
                    logger.warning(
                        "candidate worker failed "
                        "index=%d error=%s",
                        index,
                        exc,
                    )

                    result = None

                if result is not None:
                    enhanced.append(
                        (index, result)
                    )

                    logger.info(
                        "candidate usable "
                        "index=%d enhanced=%d/%d",
                        index,
                        len(enhanced),
                        target,
                    )

                    if len(enhanced) >= target:
                        break

                # A failed/unsuitable candidate does not disappear from the
                # raw fallback pool. Only successfully enhanced candidates
                # are excluded from that pool.

                if len(enhanced) < target:
                    submit_next()

            if len(enhanced) >= target:
                break

    finally:
        # Futures that have not started can be cancelled. Futures already
        # running may finish in the background, but they are no longer part
        # of the request's result path.
        executor.shutdown(
            wait=False,
            cancel_futures=True,
        )

    # Enhanced results are deliberately ordered by completion time:
    # whichever suitable articles became usable first are returned first.
    results = [
        result
        for _, result in enhanced[:target]
    ]

    enhanced_indices = {
        index
        for index, _ in enhanced[:target]
    }

    # Original search-engine results are an independent fallback layer.
    # Failed enhancement attempts remain eligible here.
    raw_remaining = [
        candidate
        for index, candidate
        in enumerate(candidates)
        if index not in enhanced_indices
    ]

    raw_slots = max(
        0,
        output_limit - len(results),
    )

    for candidate in raw_remaining[
        : min(
            RAW_FALLBACK_COUNT,
            raw_slots,
        )
    ]:
        results.append(
            {
                "url": candidate["url"],
                "title": candidate["title"],
                "snippet": candidate.get(
                    "snippet",
                    "",
                ),
                "content_source": "search_snippet",
            }
        )

    return results


# ============================================================================
# HTTP API
# ============================================================================

class SearchHandler(
    http.server.BaseHTTPRequestHandler
):
    server_version = (
        "search_service/4.0"
    )

    def log_message(
        self,
        fmt: str,
        *args: Any,
    ) -> None:

        logger.info(
            "%s - %s",
            self.address_string(),
            fmt % args,
        )

    def _send_json(
        self,
        payload: dict[str, Any],
        status: int = 200,
    ) -> None:

        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")

        self.send_response(status)

        self.send_header(
            "Content-Type",
            "application/json; charset=utf-8",
        )

        self.send_header(
            "Content-Length",
            str(len(body)),
        )

        self.send_header(
            "Cache-Control",
            "no-store",
        )

        self.end_headers()

        self.wfile.write(body)

    def do_GET(self) -> None:
        try:
            parsed = urlparse(
                self.path
            )

            # --------------------------------------------------------
            # Health
            # --------------------------------------------------------

            if parsed.path == "/health":
                self._send_json(
                    {
                        "status": "ok",
                        "version": VERSION,
                    }
                )

                return

            # --------------------------------------------------------
            # Search
            # --------------------------------------------------------

            if parsed.path != "/search":
                self._send_json(
                    {
                        "error": "not_found"
                    },
                    status=404,
                )

                return

            params = parse_qs(
                parsed.query
            )

            query = clean_text(
                params.get(
                    "q",
                    [""],
                )[0]
            )

            if not query:
                self._send_json(
                    {
                        "error": "missing_query"
                    },
                    status=400,
                )

                return

            try:
                limit = int(
                    params.get(
                        "limit",
                        [str(DEFAULT_LIMIT)],
                    )[0]
                )

            except ValueError:
                limit = DEFAULT_LIMIT

            limit = max(
                1,
                min(
                    limit,
                    MAX_LIMIT,
                ),
            )

            started = time.perf_counter()

            # SEARCH_LOCK protects the dedicated Camoufox search browser.
            # Article enhancement is independently concurrent and must not
            # block the next search request.
            with SEARCH_LOCK:
                candidates = search(query)

            results = enhance_candidates(
                candidates,
                limit,
            )

            for i, r in enumerate(results, 1):
                logger.info(
                    "FINAL #%d URL=%s TITLE=%r SNIPPET=%r CONTENT_SOURCE=%s",
                    i,
                    r.get("url", ""),
                    r.get("title", ""),
                    (r.get("snippet", "") or "").replace("\\n", " ")[:500],
                    r.get("content_source", ""),
                )

            elapsed = (
                time.perf_counter()
                - started
            )

            logger.info(
                "search done "
                "query=%r candidates=%d "
                "results=%d elapsed=%.3fs",
                query,
                len(candidates),
                len(results),
                elapsed,
            )

            self._send_json(
                {
                    "query": query,
                    "results": results,
                }
            )

        except Exception as exc:
            logger.exception(
                "request failed"
            )

            self._send_json(
                {
                    "error": str(exc)
                },
                status=500,
            )


# ============================================================================
# Main
# ============================================================================

def main() -> None:
    global browser_executor

    logger.info(
        "starting search_service "
        "version=%s host=%s port=%d",
        VERSION,
        HOST,
        PORT,
    )

    if not READABILITY_JS_PATH.exists():
        raise FileNotFoundError(
            "Readability.js not found: "
            f"{READABILITY_JS_PATH}"
        )

    browser_executor = BrowserExecutor()

    server = http.server.ThreadingHTTPServer(
        (HOST, PORT),
        SearchHandler,
    )

    logger.info(
        "listening on %s:%d",
        HOST,
        PORT,
    )

    try:
        server.serve_forever()

    except KeyboardInterrupt:
        logger.info(
            "shutdown requested"
        )

    finally:
        server.server_close()

        if browser_executor is not None:
            browser_executor.shutdown()
            browser_executor = None

        logger.info(
            "search_service stopped"
        )


if __name__ == "__main__":
    main()
