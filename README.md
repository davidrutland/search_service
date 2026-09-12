# Local Search Service

A lightweight local HTTP search service that provides web search results to a
local LLM through a simple JSON API.

The service is designed to sit between a local LLM and web search, providing
both search results and, where possible, useful extracted content from the
returned pages.

It deliberately keeps the search process simple and domain-agnostic. It does
not attempt to become a general-purpose web crawler or infer URLs that were
not returned by the search engine.

## Overview

The service performs two main tasks:

1. Search the web and return search results.
2. Attempt to retrieve and extract useful content from the returned URLs.

Search results are treated as authoritative. If the search engine returns a
topic page, category page, homepage, news hub, or other unsuitable result,
the service does not attempt to manufacture the URL of an individual article.

Instead, the original search result is retained.

The LLM can perform another, more specific search if it needs the exact
article.

This keeps the service relatively simple, predictable, and independent of
the HTML structure of individual websites.

## Search

The service exposes a simple HTTP endpoint:

    GET /search?q=<query>

An optional `limit` parameter controls the number of results requested.

Example:

    curl -sG 'http://127.0.0.1:8787/search' \
      --data-urlencode 'q=latest local news Birkenhead Wirral' \
      --data-urlencode 'limit=10'

The response is JSON.

A basic result looks like:

    {
      "title": "Example article",
      "url": "https://example.com/article",
      "snippet": "Search engine supplied description..."
    }

Enhanced results may contain additional fields when useful content has been
successfully retrieved and extracted.

## Search result handling

Search results are kept even when content extraction fails.

This is important because a search result can still be useful to the LLM
even when the page itself cannot be retrieved or parsed.

The service therefore does not discard a result simply because it could not
extract article content from it.

## URL handling

URLs returned by the search engine are treated as authoritative.

The service does not:

- invent article URLs
- guess article slugs from headlines
- construct URLs from snippets
- rewrite URLs into presumed article URLs
- recursively follow links found on pages
- attempt to map individual websites into article databases

For example, if the search engine returns:

    https://example.com/news

and the page contains a headline for an individual story, the service does
not attempt to construct something like:

    https://example.com/news/some-guessed-article-slug

If the exact story is required, the LLM can search for that story directly.

## Article extraction

For a limited number of search results, the service attempts to retrieve
the page and extract useful article content.

HTTP retrieval is preferred because it is cheaper and faster than browser
automation.

If the initial retrieval does not produce usable content, browser-based
retrieval may be attempted.

The extracted content is subjected to a basic quality check rather than
being assumed to be valid simply because the page returned successfully.

The current minimum article length is:

    MIN_ARTICLE_CHARS = 500

If useful content cannot be obtained, the original search result remains
available.

## Candidate limits

The service deliberately limits the amount of expensive content retrieval
performed for each search.

Current defaults are:

    DEFAULT_LIMIT          = 10
    MAX_LIMIT              = 20
    MAX_CANDIDATES         = 5
    TARGET_USABLE_ARTICLES = 2

This means that a request can return up to 20 search results, while only the
first five candidates are considered for content enhancement.

Processing stops once two usable articles have been obtained.

The remaining search results are retained without further expensive
processing.

These limits prevent a single search request from generating an excessive
number of network or browser operations.

## Why the service does not crawl websites

The service intentionally does not attempt to distinguish every possible
type of web page.

Websites use many different structures for:

- articles
- topic pages
- category pages
- homepages
- search pages
- news hubs
- feeds
- article listings
- related-story pages

Trying to reliably classify all of these would require increasingly complex
HTML heuristics and site-specific rules.

It would also create additional requests and potentially turn a single search
into a large crawl.

Instead, the search engine remains responsible for discovering URLs.

The service retrieves and enriches the URLs it is given, but does not attempt
to discover a second layer of URLs.

If the first search returns a hub or other non-specific page, the LLM can
simply perform another search using the story headline or a more specific
query.

## Design principles

### Search first

The search engine is responsible for discovering relevant URLs.

### Enrich, don't crawl

The service can retrieve content from returned URLs, but does not recursively
crawl websites.

### Prefer cheap operations

HTTP retrieval is preferred before more expensive browser-based retrieval.

### Limit expensive work

Only a small number of candidates are processed for article content.

### Preserve search results

A failure to retrieve or extract content does not cause the original result
to be discarded.

### Never manufacture URLs

URLs should come from the search engine rather than being inferred from
headlines, snippets, or page structure.

### Stay domain-agnostic

The service should avoid accumulating special cases for individual websites.

### Let the LLM reason

If a result is too broad or does not identify the exact story required, the
LLM can perform another search rather than requiring the search service to
become increasingly complicated.

## Logging

Logs are written under:

    /home/david/AI/camoufox/logs/

Log entries include timestamps.

Request-associated log messages include a request ID, allowing activity from
different simultaneous requests to be distinguished.

Example:

    2026-09-12 16:42:10,123 INFO [8c4f...] SEARCH ...
    2026-09-12 16:42:10,456 INFO [8c4f...] FETCH[1] ...

Operational logging is written to the log files rather than being routinely
printed to stdout.

## Configuration

The main configuration constants are near the top of `search_service.py`.

Important settings include:

    HOST = "0.0.0.0"
    PORT = 8787

    DEFAULT_LIMIT = 10
    MAX_LIMIT = 20

    MAX_CANDIDATES = 5
    TARGET_USABLE_ARTICLES = 2

    SEARCH_TIMEOUT = 10000
    HTTP_TIMEOUT = 5.0
    FETCH_TIMEOUT = 10000
    FETCH_SETTLE_MS = 250

    MIN_ARTICLE_CHARS = 500
    MAX_SUMMARY_CHARS = 30000
    SUMMARY_SENTENCES = 8

The Readability implementation is located at:

    lib/Readability.js

## API

### Search

    GET /search?q=<query>

Optional:

    GET /search?q=<query>&limit=10

Example:

    curl -sG 'http://127.0.0.1:8787/search' \
      --data-urlencode 'q=latest local news Birkenhead Wirral' \
      --data-urlencode 'limit=10'

Typical response:

    {
      "query": "example search",
      "results": [
        {
          "title": "Example article",
          "url": "https://example.com/article",
          "snippet": "Search result description..."
        }
      ]
    }

Enhanced results may contain additional fields such as extracted content and
the method used to obtain it.

## Dependencies

The service uses Python libraries including:

- `httpx`
- `Camoufox`
- `sumy`

It also uses:

- Mozilla Readability
- Python's standard HTTP server
- Python thread pools for concurrent HTTP retrieval
- Playwright support provided through Camoufox

## Running

The service can be started directly with Python:

    python3 /home/david/AI/camoufox/search_service.py
    
    or if camoufox is using a venv:
    
    /home/david/AI/camoufox/venv/bin/python search_service.py
    
    

The service listens on:

    0.0.0.0:8787

For local testing:

    127.0.0.1:8787

## Version

Current service version:

    3.1
