# Search Service

A local web-search and evidence-retrieval service designed for integration with local AI assistants.

The service combines search-engine results with optional HTTP retrieval, Mozilla Readability extraction, and Sumy extractive summarisation. It exposes a simple HTTP API and is designed to provide useful evidence quickly without allowing slow browser work to block subsequent searches.

## Current version

**Version:** `7.0-fast`

**Python:** 3.13

## What it does

The service accepts a search query and returns search results containing:

* search-engine title
* URL
* original search-engine snippet
* optional retrieved article content
* optional extractive summary
* `content_source` metadata indicating where the returned evidence came from

The normal enhancement path is:

```text
Search query
    │
    ▼
Search engine
    │
    ▼
Search candidates
    │
    ├──► original search-engine snippet
    │
    ▼
HTTP retrieval
    │
    ▼
Mozilla Readability
    │
    ▼
Sumy extractive summarisation
    │
    ▼
Summary appended to original snippet
```

If HTTP retrieval or extraction cannot produce usable article content, the service **keeps the original search-engine snippet unchanged**.

There is deliberately no article-level Camoufox fallback in the current version.

Camoufox is reserved for search operations.

## Architecture

The service has two distinct retrieval mechanisms.

### Search

Search-engine queries can use the browser-backed Camoufox path when required.

Camoufox is managed through a single browser executor because browser instances are relatively expensive and are treated as a scarce resource.

Search jobs have priority over other browser work.

### Article enhancement

Search results can be enhanced by retrieving the result URL directly over HTTP.

The enhancement path is:

1. Fetch the URL with `httpx`.
2. Apply SSRF and response-size safety checks.
3. Extract the main article content with `readability-lxml`.
4. Reject unusably short/invalid extraction.
5. Generate an extractive summary with Sumy.
6. Append the summary to the original search-engine snippet.

This avoids sending successful HTTP retrievals through Camoufox.

### Failed enhancement

If an article cannot be retrieved or extracted successfully:

```text
HTTP enhancement fails
        │
        ▼
Original search-engine snippet retained
```

The service does **not** subsequently launch a browser fallback for that article.

This is intentional. It prevents failed article retrievals from filling the single Camoufox queue and delaying subsequent searches.

## Dependencies

The current runtime dependencies are pinned in `requirements.txt`:

```text
camoufox==0.5.5
httpx==0.28.1
readability-lxml==0.9
sumy==0.13.0
```

The service otherwise uses Python's standard library.

The dependency file describes the service's direct runtime dependencies rather than every transitive package installed into the virtual environment.

## Installation

Python 3.13 is the current tested interpreter.

From the service directory:

```bash
python3.13 -m venv venv
venv/bin/python -m pip install -r requirements.txt
```

Camoufox may require its browser components to be installed/configured according to the Camoufox installation requirements for the installed version.

## Running the service

From the service directory:

```bash
venv/bin/python search_service.py
```

The default service configuration listens on:

```text
0.0.0.0:8787
```

For local use, clients can normally access it through:

```text
http://127.0.0.1:8787
```

Startup logging identifies the service version and listening port.

## Search API

The primary endpoint is:

```text
POST /search
```

The service accepts a JSON search request and returns JSON containing the resulting search candidates and associated metadata.

A typical request is conceptually:

```json
{
  "query": "example search"
}
```

The exact request/response fields should be treated according to the running service implementation, rather than inferred from this README.

## Content sources

Each result can identify the source of its usable content.

### `search_snippet`

The result could not be usefully enhanced.

The search-engine's original snippet is returned unchanged.

### `article_http`

The result was successfully retrieved over HTTP and processed through the article-enhancement pipeline.

The result contains the original search snippet plus the generated extractive summary.

## Time limits

The service is deliberately bounded so that a slow web page does not hold an AI request indefinitely.

Current limits include:

```text
REQUEST_DEADLINE = 12 seconds
ENHANCE_BUDGET   = 7 seconds
HTTP_TIMEOUT     = 6 seconds
BROWSER_TIMEOUT  = 5 seconds
SEARCH_TIMEOUT   = 6.5 seconds
```

These are implementation-level limits and may be adjusted as the service evolves.

The important design constraint is that the complete request should remain comfortably within the approximately 15-second timeout used by the current Max integration.

## Article enhancement concurrency

HTTP article enhancement uses a bounded worker pool.

Current configuration:

```text
HTTP_WORKERS = 3
ENHANCE_GLOBAL_CONCURRENCY = 3
TARGET_USABLE_ARTICLES = 2
```

The service does not attempt to enhance every search result indefinitely.

It works toward obtaining a small number of useful article sources within the available time budget.

This keeps latency predictable when search results contain slow, blocked, malformed, or otherwise unusable URLs.

## Sumy

Sumy provides the extractive summarisation stage of article enhancement.

The current implementation uses Sumy's:

* `Tokenizer`
* `PlaintextParser`
* `LexRankSummarizer`

The summary is used as an enhancement to the search result rather than as a replacement for the original search-engine snippet.

Consequently, when summarisation is unavailable or article retrieval fails, the original search result remains usable.

## Security

The HTTP retrieval path performs SSRF-related validation before retrieving result URLs.

The service also limits HTTP retrieval rather than allowing arbitrary unbounded downloads.

This is important because search results are externally supplied URLs and should not automatically be treated as trusted internal resources.

The service is intended for use as a local/internal component, not as an unrestricted public web proxy.

## Performance characteristics

Typical successful searches should complete in a few seconds rather than tens of seconds.

A normal successful path looks approximately like:

```text
Search
  │
  ├── search results
  │
  └── parallel HTTP enhancement
          │
          ├── article 1 → Readability → Sumy
          ├── article 2 → Readability → Sumy
          └── ...
  │
  ▼
JSON response
```

Slow or failed article URLs do not cause the service to wait for browser fallbacks.

Camoufox remains available for search operations rather than being consumed by article enhancement.

Actual latency depends heavily on the search engine, network conditions, target websites, and whether browser-backed search is required.

## Logging

The service logs its startup configuration and significant search/enhancement events.

Logs are intended to make it possible to distinguish:

* search latency
* HTTP retrieval failures
* unusable article extraction
* successful article enhancement
* request deadline pressure
* browser/search activity

An HTTP enhancement failure is not itself an error requiring a browser retry. The expected behaviour is to retain the original search snippet.

## Troubleshooting

### Service starts but searches fail

Check that the virtual environment contains the pinned dependencies:

```bash
venv/bin/python -m pip show camoufox httpx readability-lxml sumy
```

The expected direct dependency versions are:

```text
camoufox 0.5.5
httpx 0.28.1
readability-lxml 0.9
sumy 0.13.0
```

### HTTP article enhancement frequently fails

This does not necessarily indicate a service failure.

Websites may:

* block automated HTTP clients
* require JavaScript
* redirect repeatedly
* return non-HTML content
* expose very little readable article text
* be unavailable or slow

The expected result in these cases is the original search-engine snippet.

The service does not currently escalate these failures into article-level Camoufox retrieval.

### Searches are slow

Look at the service log first.

The principal things to distinguish are:

1. search-engine/browser latency;
2. HTTP article retrieval latency;
3. request deadline pressure.

Article enhancement is bounded and should not create an unbounded browser queue.

### Port already in use

The default port is `8787`.

Check for an existing listener before starting another instance.

## Design principles

The service intentionally favours:

* predictable latency over exhaustive retrieval;
* useful evidence over maximum article coverage;
* original search snippets as a reliable fallback;
* direct HTTP retrieval whenever possible;
* Readability rather than browser rendering for successful article retrieval;
* extractive summarisation rather than opaque generative rewriting;
* bounded concurrency;
* a single controlled Camoufox instance;
* explicit request deadlines;
* SSRF protection;
* simple JSON integration.

The service is intended to be a retrieval component for another application, rather than a complete search UI.

## Current limitations

The service does not guarantee that every search result can be opened or summarised.

In particular:

* JavaScript-only sites may not be retrievable through the HTTP enhancement path.
* Websites can block automated requests.
* Search-engine results can vary over time.
* Search latency depends partly on the external search provider.
* Article extraction quality depends on the target site's HTML.
* Extractive summaries are summaries of retrieved text, not independently verified claims.
* A failed HTTP enhancement currently falls back directly to the search snippet.
* Camoufox is intentionally not used as an article-level fallback.

## Evaluating the service

A useful basic evaluation should test both the normal and degraded paths.

### Normal search

Use a query that produces several conventional web pages.

Check:

* search completes within the expected latency;
* results contain usable titles/URLs/snippets;
* at least some candidates receive `article_http` enhancement where suitable pages are available.

### Difficult pages

Use queries likely to produce pages that block automated HTTP access or contain difficult HTML.

Check that:

* the request still completes;
* failed enhancement does not stall the entire request;
* the original search-engine snippet remains intact;
* no article-level Camoufox queue develops.

### Repeated searches

Run several searches consecutively.

The important property is that a slow or failed article URL from one request should not create stale browser work that materially delays the next search.

## Integration role

The intended architecture is:

```text
                  ┌─────────────────┐
                  │      Max        │
                  │ local AI client │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  search_service │
                  └────────┬────────┘
                           │
             ┌─────────────┴─────────────┐
             │                           │
             ▼                           ▼
      Search provider              HTTP retrieval
             │                           │
             ▼                           ▼
          Camoufox                 Readability
                                         │
                                         ▼
                                       Sumy
             │                           │
             └─────────────┬─────────────┘
                           ▼
                     JSON evidence
```

Max can therefore treat the service as a relatively small, bounded retrieval primitive without needing to manage Camoufox, HTTP retrieval, Readability, or summarisation itself.

## Licence

MIT
