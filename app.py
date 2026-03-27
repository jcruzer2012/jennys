import os
from typing import Any, Dict, Optional

import requests
from flask import Flask, abort, render_template_string

app = Flask(__name__)

DIRECTUS_URL = os.getenv("DIRECTUS_URL", "http://localhost:8055").rstrip("/")
DIRECTUS_COLLECTION = os.getenv("DIRECTUS_COLLECTION", "pages")
DIRECTUS_TOKEN = os.getenv("DIRECTUS_TOKEN", "")
SHOW_DRAFTS = os.getenv("SHOW_DRAFTS", "false").lower() == "true"
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "10"))

BASE_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{{ title }}</title>
  <style>
    body {
      font-family: Arial, sans-serif;
      max-width: 900px;
      margin: 40px auto;
      padding: 0 20px;
      line-height: 1.6;
      color: #1f2937;
    }
    nav {
      margin-bottom: 2rem;
      padding-bottom: 1rem;
      border-bottom: 1px solid #e5e7eb;
    }
    nav a {
      margin-right: 1rem;
      text-decoration: none;
      color: #2563eb;
    }
    .meta {
      color: #6b7280;
      font-size: 0.95rem;
      margin-bottom: 1rem;
    }
    .card {
      border: 1px solid #e5e7eb;
      border-radius: 12px;
      padding: 1rem 1.25rem;
      margin: 1rem 0;
    }
    .slug {
      font-family: monospace;
      color: #6b7280;
      font-size: 0.9rem;
    }
    .error {
      background: #fef2f2;
      color: #991b1b;
      padding: 1rem;
      border-radius: 10px;
      border: 1px solid #fecaca;
    }
  </style>
</head>
<body>
  <nav>
    <a href="/">Home</a>
    <a href="/debug/pages">Debug Pages</a>
  </nav>
  {{ body|safe }}
</body>
</html>
"""

def directus_headers() -> Dict[str, str]:
    headers = {"Accept": "application/json"}
    if DIRECTUS_TOKEN:
        headers["Authorization"] = f"Bearer {DIRECTUS_TOKEN}"
    return headers

def directus_get(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{DIRECTUS_URL}{path}"
    response = requests.get(
        url,
        headers=directus_headers(),
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()

def fetch_page_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "filter[slug][_eq]": slug,
        "limit": 1,
        "fields": "id,title,slug,body,status,date_updated",
    }
    if not SHOW_DRAFTS:
        params["filter[status][_eq]"] = "published"

    data = directus_get(f"/items/{DIRECTUS_COLLECTION}", params=params)
    items = data.get("data", [])
    return items[0] if items else None

def fetch_pages() -> list[Dict[str, Any]]:
    params: Dict[str, Any] = {
        "sort": "title",
        "fields": "id,title,slug,status,date_updated",
    }
    if not SHOW_DRAFTS:
        params["filter[status][_eq]"] = "published"

    data = directus_get(f"/items/{DIRECTUS_COLLECTION}", params=params)
    return data.get("data", [])

@app.route("/healthz")
def healthz():
    return {"ok": True}, 200

@app.route("/")
def home() -> str:
    pages = fetch_pages()
    cards = []
    for page in pages:
        cards.append(
            f'''<div class="card">
                  <h2><a href="/pages/{page.get("slug", "")}">{page.get("title", "Untitled")}</a></h2>
                  <div class="slug">/{page.get("slug", "")}</div>
                  <div class="meta">Status: {page.get("status", "unknown")} | Updated: {page.get("date_updated", "")}</div>
                </div>'''
        )

    body = "<h1>Directus → Flask Companion</h1><p>This page is reading your Directus collection and linking each page by slug.</p>"
    body += "".join(cards) if cards else "<p>No pages found yet.</p>"
    return render_template_string(BASE_TEMPLATE, title="Home", body=body)

@app.route("/pages/<slug>")
def page_detail(slug: str) -> str:
    page = fetch_page_by_slug(slug)
    if not page:
        abort(404)

    title = page.get("title", "Untitled")
    body_html = page.get("body") or "<p>No body content yet.</p>"
    updated = page.get("date_updated", "")
    status = page.get("status", "unknown")

    body = f"""
      <h1>{title}</h1>
      <div class="meta">Slug: /{slug} | Status: {status} | Updated: {updated}</div>
      <article>{body_html}</article>
    """
    return render_template_string(BASE_TEMPLATE, title=title, body=body)

@app.route("/debug/pages")
def debug_pages() -> str:
    try:
        pages = fetch_pages()
        body = "<h1>Debug Pages</h1><pre>" + repr(pages) + "</pre>"
        return render_template_string(BASE_TEMPLATE, title="Debug Pages", body=body)
    except Exception as exc:
        body = f'<h1>Debug Pages</h1><div class="error">{exc}</div><p>Check DIRECTUS_URL, DIRECTUS_TOKEN, permissions, and collection/field names.</p>'
        return render_template_string(BASE_TEMPLATE, title="Debug Pages", body=body), 500

@app.errorhandler(404)
def not_found(_: Exception):
    body = '<h1>404</h1><p>That page was not found in Directus.</p>'
    return render_template_string(BASE_TEMPLATE, title="Not Found", body=body), 404

@app.errorhandler(requests.HTTPError)
def handle_http_error(exc: requests.HTTPError):
    response = exc.response
    status_code = response.status_code if response is not None else 500
    body = f'<h1>Directus Error</h1><div class="error">{exc}</div>'
    return render_template_string(BASE_TEMPLATE, title="Directus Error", body=body), status_code
