# Search Service

A local web search and evidence-extraction service for Max and other local AI applications.

The service provides:

1. Web search through DuckDuckGo, with Brave as a fallback.
2. Browser-backed search using a single shared Camoufox instance.
3. HTTP retrieval of candidate result pages.
4. Local Mozilla Readability extraction from retrieved HTML.
5. Lightweight local/LLM-based enhancement of extracted article content.
6. Safe fallback to the original search-engine snippet when page retrieval or enhancement fails.
7. JSON HTTP endpoints suitable for integration with Max.

The design deliberately separates **search** from **article retrieval**. Camoufox is used for search-engine access only; individual article pages are fetched directly over HTTP.

---

## Design goals

The service is designed for a local AI assistant where search results need to become useful evidence without allowing slow or failed pages to stall the entire request.

The main goals are:

* Keep normal searches fast.
* Produce useful cleaned article content where possible.
* Preserve search-engine evidence when page retrieval fails.
* Avoid browser-resource contention.
* Bound total request time.
* Prevent unsafe server-side requests.
* Degrade gracefully rather than failing an entire search because one result is unusable.
* Keep the service independent of the frontend using it.

A key design principle is:

> **Enhancement is optional. Search results remain valid even when enhancement fails.**

---

## Architecture

```text
                    ┌─────────────────────┐
                    │       Client        │
                    │   Max / curl / etc. │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │   search_service    │
                    │     HTTP server     │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │    Search engine     │
                    │  DDG → Brave fallback│
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │      Camoufox       │
                    │  single shared owner│
                    └──────────┬──────────┘
                               │
                         search results
                               │
              ┌────────────────┴────────────────┐
              │                                 │
              ▼                                 ▼
     search-engine snippet              candidate URL
              │                                 │
              │                                 ▼
              │                         bounded HTTP fetch
              │                                 │
              │                                 ▼
              │                         local Readability
              │                                 │
              │                                 ▼
              │                            enhancement
              │                                 │
              └────────────────┬────────────────┘
                               │
                               ▼
                         JSON response
```

### Important resource boundary

There is **one Camoufox browser owner**.

Camoufox is used for search-engine interaction because some search engines require browser automation.

It is **not** used as a fallback for individual article pages.

This prevents failed article requests from filling the browser queue and delaying subsequent searches.

---

## Current version

```text
VERSION = 7.0-fast
```

The current timing configuration is:

| Setting                      |    Value | Purpose                                         |
| ---------------------------- | -------: | ----------------------------------------------- |
| `REQUEST_DEADLINE`           |  `12.0s` | Maximum overall request budget                  |
| `ENHANCE_BUDGET`             |   `7.0s` | Maximum time allocated to result enhancement    |
| `HTTP_TIMEOUT`               |   `6.0s` | Per-page HTTP timeout                           |
| `BROWSER_TIMEOUT`            |   `5.0s` | Browser-executor shutdown/task handling         |
| `SEARCH_TIMEOUT_MS`          | `6500ms` | Search operation timeout                        |
| `HTTP_WORKERS`               |      `3` | Parallel HTTP enhancement workers               |
| `TARGET_USABLE_ARTICLES`     |      `2` | Target number of successfully enhanced articles |
| `ENHANCE_GLOBAL_CONCURRENCY` |      `3` | Global enhancement concurrency                  |

The exact values are deliberately conservative. The service is intended to return useful evidence quickly rather than attempting exhaustive retrieval.

---

# Search

Search requests are sent through the browser-backed search executor.

The normal search sequence is:

```text
DuckDuckGo
    │
    ├── success → use results
    │
    └── failure
          │
          ▼
        Brave
          │
          ├── success → use results
          │
          └── failure → search request fails
```

Search jobs have priority over other browser work.

The browser executor maintains a priority queue so that stale lower-priority work cannot unnecessarily delay a new search.

Because article-level browser fallback has been removed, the browser queue is effectively reserved for search operations.

---

# Article enhancement

After search results are obtained, the service attempts to retrieve useful content from candidate URLs.

The current path is:

```text
candidate URL
     │
     ▼
safe HTTP request
     │
     ▼
HTML
     │
     ▼
Mozilla Readability
     │
     ▼
usable article text?
     │
 ┌───┴────┐
 │        │
yes       no
 │        │
 ▼        ▼
enhance   retain original
 │        search snippet
 ▼
response
```

Successful HTTP retrieval uses local `readability-lxml`.

This is intentional: Readability extraction does not require launching a browser.

---

# No article-level Camoufox fallback

The service does **not** fall back to Camoufox when an individual article cannot be fetched over HTTP.

For example:

```text
HTTP fetch fails
      │
      ▼
keep search-engine snippet
```

It does **not** do:

```text
HTTP fetch fails
      │
      ▼
launch browser
      │
      ▼
fetch article
```

This was removed because browser fallbacks caused slow or failed article requests to occupy the shared Camoufox executor.

The result was that otherwise fast searches could be delayed by stale article-fetch work.

The current behaviour prioritises predictable latency.

---

# Graceful degradation

Search-engine snippets are treated as valid evidence.

If article retrieval or enhancement fails, the service returns the original search result rather than discarding it.

For example:

```json
{
  "title": "Example article",
  "url": "https://example.com/article",
  "snippet": "The original search-engine snippet...",
  "content_source": "search_snippet"
}
```

When article retrieval succeeds:

```json
{
  "title": "Example article",
  "url": "https://example.com/article",
  "snippet": "The original search-engine snippet...",
  "content": "Cleaned article content...",
  "content_source": "article_http"
}
```

The original search snippet is retained when enhancement succeeds rather than being silently replaced.

---

# Security

HTTP retrieval is subject to SSRF protection.

The service validates candidate URLs before making outbound requests and applies bounded retrieval behaviour, including:

* URL validation
* restricted schemes
* protection against requests to unsafe/private destinations
* bounded redirects
* response-size limits
* HTTP timeouts
* content-type handling
* article-length checks

The service should therefore not be treated as a general unrestricted HTTP proxy.

---

# Concurrency

Article enhancement uses a bounded worker pool.

Current configuration:

```text
HTTP_WORKERS = 3
ENHANCE_GLOBAL_CONCURRENCY = 3
```

The service does not attempt to retrieve every search result simultaneously.

Instead, it works toward:

```text
TARGET_USABLE_ARTICLES = 2
```

Once sufficient usable articles have been obtained, additional enhancement work is not required.

This keeps the service focused on obtaining enough useful evidence rather than maximising the number of fetched pages.

---

# Request deadline

Every search request has an overall deadline:

```text
REQUEST_DEADLINE = 12 seconds
```

The handler establishes a deadline when the request starts.

The enhancement phase is additionally bounded by:

```text
ENHANCE_BUDGET = 7 seconds
```

The effective enhancement deadline cannot exceed the overall request deadline.

Conceptually:

```text
request start
│
├────────────── search ──────────────┐
│                                    │
│                         enhancement│
│                         ≤ 7 seconds│
│                                    │
└────────────────────────────────────┘
             ≤ 12 seconds
```

The deadline is a hard architectural constraint rather than an aspiration.

---

# Response behaviour

The service is designed to return partial results wherever possible.

A typical result may contain:

```text
title
url
snippet
content
content_source
```

`content_source` identifies where the usable content came from.

Current values include:

### `search_snippet`

The article could not be usefully retrieved or enhanced.

The original search-engine snippet is returned unchanged.

### `article_http`

The candidate URL was retrieved over HTTP and successfully processed.

The cleaned article content is available in addition to the search snippet.

---

# Enhancement

Enhancement is deliberately secondary to retrieval.

The service does not require enhancement to succeed for a search result to remain useful.

The intended hierarchy is:

```text
search result
    │
    ├── search snippet
    │       ↓
    │   always useful fallback
    │
    └── article retrieval
            │
            ├── success
            │      ↓
            │   richer evidence
            │
            └── failure
                   ↓
              keep snippet
```

This prevents a secondary processing failure from turning a valid search result into a missing result.

---

# Logging

The service logs to its configured log directory using a rotating log file.

Important operational events include:

* service startup
* search requests
* search-engine selection
* search timing
* candidate counts
* HTTP retrieval failures
* enhancement failures
* browser executor activity
* request failures
* request timing

The service should be diagnosed from its logs rather than assuming that a slow request indicates a search-engine failure.

In particular, a slow request should be examined for:

```text
search latency
HTTP retrieval latency
enhancement latency
browser queue activity
```

---

# Running the service

The service is a standalone Python application.

Example:

```bash
/home/david/AI/camoufox/venv/bin/python \
    /home/david/AI/camoufox/search_service.py
```

The default service configuration listens on:

```text
0.0.0.0:8787
```

For local clients, use:

```text
http://127.0.0.1:8787
```

rather than hard-coding a LAN address.

---

# Operational testing

A basic health/search test can be performed with `curl`.

For example:

```bash
curl -sS --max-time 15 \
  'http://127.0.0.1:8787/search?q=latest+news'
```

The exact request parameters should follow the current API implementation.

When diagnosing latency, measure the complete request rather than only the search-engine portion.

For example:

```bash
time curl -sS --max-time 15 \
  'http://127.0.0.1:8787/search?q=example'
```

---

# Failure philosophy

The service intentionally favours:

```text
fast + partial + trustworthy
```

over:

```text
slow + exhaustive + fragile
```

A failed article should not invalidate a successful search.

A failed enhancement should not invalidate an article.

A temporarily unavailable search engine should allow the fallback engine to be attempted.

A browser task that is no longer useful should not be allowed to consume resources indefinitely.

---

# Current limitations

The current service intentionally does **not** provide:

* article retrieval through Camoufox fallback
* unrestricted browser-based page fetching
* exhaustive crawling
* guaranteed retrieval of JavaScript-only articles
* guaranteed extraction from every paywall or anti-bot system
* a general-purpose user-callable URL-fetch API

The last point is important for integration with an AI frontend.

The service currently performs article retrieval **as part of search-result enhancement**. It is not yet a general `fetch(url)` tool for an LLM.

That distinction allows the search service to remain focused and predictable.

---

# Intended integration with Max

The intended search flow is:

```text
Max
 │
 ▼
search query
 │
 ▼
search_service
 │
 ├── search engine
 │
 ├── candidate URLs
 │
 └── article enhancement
 │
 ▼
structured evidence
 │
 ▼
Max reasoning
```

A future general-purpose fetch capability can be layered separately rather than turning the search service into a general web browser.

This keeps two different operations distinct:

```text
SEARCH
Find relevant sources.

FETCH
Retrieve a specific source.
```

That separation is particularly useful for research workflows where Max may first discover a URL and subsequently decide that the complete page needs to be examined.

---

# Version history

## 7.0-fast

Current architecture.

Major characteristics:

* 12-second overall request deadline.
* 7-second enhancement budget.
* Parallel HTTP article retrieval.
* Local `readability-lxml` extraction.
* Three HTTP enhancement workers.
* Two usable article target.
* Search-priority browser executor.
* DuckDuckGo → Brave fallback.
* No article-level Camoufox fallback.
* Original search snippets preserved when enhancement fails.
* Camoufox reserved for search operations.

The removal of article-level browser fallback is intentional and is part of the current latency model.

---

# Development principles

Changes to this service should preserve the following properties:

1. **Do not make search dependent on article enhancement.**
2. **Do not allow a failed article to remove its search result.**
3. **Do not reintroduce article-level Camoufox fallback without measuring its effect on search latency.**
4. **Respect the overall request deadline.**
5. **Keep outbound HTTP retrieval SSRF-safe.**
6. **Keep browser concurrency bounded.**
7. **Prefer deterministic degradation over indefinite retries.**
8. **Preserve useful original search-engine evidence.**
9. **Keep search and general-purpose URL fetching conceptually separate.**

---

# Summary

`search_service.py` is a local, latency-bounded web search and evidence extraction service.

Its current architecture deliberately uses:

```text
Camoufox
    → search

HTTP
    → article retrieval

Readability
    → article extraction

Enhancement
    → richer evidence

Search snippet
    → guaranteed fallback
```

The most important current architectural rule is:

> **Camoufox searches; HTTP fetches articles.**

This keeps the scarce browser resource available for the operation that actually requires it, while allowing ordinary article retrieval to happen concurrently and cheaply.
