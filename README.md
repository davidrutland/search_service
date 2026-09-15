# search_service v4.0

A lean, high-precision retrieval service for LLMs. It searches DuckDuckGo/Brave, fetches articles, and returns **only** high-signal, deep-article snippets (no hubs, no noise).

## Features

*   **Dual Search:** DuckDuckGo (primary) + Brave (fallback).
*   **Dual Fetch:** HTTP (preferred) + Camoufox (fallback for JS-heavy sites).
*   **Strict SSRF:** Blocks private/reserved IPs at every hop.
*   **Smart Filtering:** Uses `Readability` to distinguish deep articles from hubs using text/link density heuristics.
*   **Structured Output:** Returns JSON with summaries, provenance, and metadata.

## Installation

### Prerequisites

*   Python 3.10+
*   `node` (for some dependencies, though pure Python works)
*   `libssl` and `libffi` (system deps for `camoufox`/`playwright`)

### 1. Dependencies

```bash
pip install httpx camoufox-python suml lxml
```

*Note: `camoufox-python` requires Playwright browsers to be installed:*
```bash
playwright install chromium
```

### 2. Readability.js

Place `Readability.js` in the `lib/` directory relative to the script:

```bash
mkdir -p lib
curl -o lib/Readability.js https://raw.githubusercontent.com/mozilla/readability/master/Readability.js
```

## Configuration (Patches Required)

Before deploying, you must edit `search_service.py` for your environment:

### 1. Log Directory
Find `LOG_DIR` and change it to your preferred path:
```python
LOG_DIR = "/home/david/AI/camoufox/logs"  # <--- CHANGE THIS
```

### 2. Browser Locale
Find `locale="en-GB"` in `BrowserExecutor.__init__` if you need a different locale:
```python
camoufox = Camoufox(headless=True, locale="en-GB")  # <--- CHANGE IF NEEDED
```

### 3. SSRF Rules (Optional)
The script blocks private IPs by default. Modify `BLOCKED_SUBNETS` and `BLOCKED_IPV6_SUBNETS` if you need to allow internal networks.

### 4. Limits
*   `DEFAULT_LIMIT = 3`: How many results to return by default.
*   `MAX_CANDIDATES = 10`: Max raw search results to process.
*   `MIN_ARTICLE_CHARS = 500`: Minimum text length to be considered an article.

## Deployment

### Run

```bash
python search_service.py
```

The server starts on `0.0.0.0:8787`.

### Systemd Service (Optional)

Create `/etc/systemd/system/search.service`:

```ini
[Unit]
Description=Search Service v4.0
After=network.target

[Service]
Type=simple
User=david
WorkingDirectory=/home/david/AI/search_service
ExecStart=/usr/bin/python3 /home/david/AI/search_service/search_service.py
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable search.service
sudo systemctl start search.service
```

## API

### Search

**GET** `/search?q=<query>&limit=3`

**Response:**
```json
{
  "query": "example",
  "results": [
    {
      "url": "https://example.com/article",
      "title": "Example Article",
      "summary": "A concise summary of the article...",
      "content_source": "article_summary",
      "fetch_method": "http",
      "extract_time": 0.45,
      "metadata": {
        "final_url": "https://example.com/article",
        "status_code": 200,
        "content_type": "text/html",
        "bytes_read": 45000,
        "reason": "ok"
      }
    }
  ]
}
```

### Health

**GET** `/health`

**Response:**
```json
{
  "status": "ok",
  "version": "4.0"
}
```

## Logging

Logs are written to `LOG_DIR` (default `/home/david/AI/camoufox/logs`). Files are rotated by size and time.

## License

MIT
