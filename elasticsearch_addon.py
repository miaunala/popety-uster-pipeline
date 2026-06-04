# -*- coding: utf-8 -*-
"""
Elasticsearch Add-on – Swiss Address Search
============================================
This module adds Elasticsearch-based fuzzy address search as an additional
fallback to the existing swisstopo + Nominatim geocoding pipeline.

Why Elasticsearch here:
  The existing pipeline fails on ~36% of addresses, mostly due to typos,
  abbreviations (Str. vs Strasse), etc.
  Elasticsearch fuzzy search tolerates these variations.

Setup:
  1. Install Elasticsearch: https://www.elastic.co/downloads/elasticsearch
     Or via Docker: docker run -p 9200:9200 -e "discovery.type=single-node" elasticsearch:8.13.0
  2. pip install elasticsearch fastapi uvicorn requests

Usage:
  # Build the index (run once):
  python elasticsearch_addon.py --build-index
  => if construction_permits is already done => now can use it, too from construction_permits

  # Start the search API:
  python elasticsearch_addon.py --serve

  # Use as a library in your pipeline:
  from elasticsearch_addon import geocode_elasticsearch
  lat, lon = geocode_elasticsearch("Chammerholzstr. 10, Uster")
"""

import argparse
import time
import requests
from pathlib import Path
from elasticsearch import Elasticsearch, helpers


# ── Configuration ─────────────────────────────────────────────────────────────
ES_HOST       = "http://localhost:9200"   # local Elasticsearch instance
INDEX_NAME    = "swiss_addresses_uster"
DATA_DIR      = Path("/Users/nathalieguibert/Downloads/PopetyTest/")

# Uster bbox in WGS84 for swisstopo search
USTER_CENTER  = "47.347,8.720"


# ── 1. Connect to Elasticsearch ───────────────────────────────────────────────
def get_client() -> Elasticsearch:
    """Return an Elasticsearch client. Raises if not reachable."""
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(
            f"Cannot connect to Elasticsearch at {ES_HOST}.\n"
            "Start it with: docker run -p 9200:9200 -e 'discovery.type=single-node' "
            "elasticsearch:8.13.0"
        )
    print(f"Connected to Elasticsearch at {ES_HOST}")
    return es


# ── 2. Create index with fuzzy-friendly mapping ───────────────────────────────
def create_index(es: Elasticsearch):
    """Create the address index with a mapping optimised for fuzzy search.

    Key decisions:
    - 'address_text' uses the 'standard' analyser (lowercases, tokenises)
    - 'address_suggest' uses completion suggester for autocomplete
    - lat/lon stored as geo_point for future spatial queries
    """
    if es.indices.exists(index=INDEX_NAME):
        print(f"Index '{INDEX_NAME}' already exists — skipping creation.")
        return

    mapping = {
        "mappings": {
            "properties": {
                "address_text": {
                    "type": "text",
                    "analyzer": "standard",
                    # second field for exact/keyword matching
                    "fields": {
                        "keyword": {"type": "keyword"}
                    }
                },
                "address_suggest": {
                    # completion suggester — powers autocomplete
                    "type": "completion"
                },
                "street":   {"type": "keyword"},
                "number":   {"type": "keyword"},
                "city":     {"type": "keyword"},
                "zip":      {"type": "keyword"},
                "location": {"type": "geo_point"},   # lat/lon for spatial queries
                "source":   {"type": "keyword"},     # "swisstopo" / "regbl"
            }
        },
        "settings": {
            "number_of_shards":   1,
            "number_of_replicas": 0,
        }
    }
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"Created index '{INDEX_NAME}'")


# ── 3. Fetch addresses from swisstopo and index them ─────────────────────────
def fetch_uster_addresses() -> list[dict]:
    """Fetch all addresses in Uster from the swisstopo search API.

    Uses the SearchServer endpoint with a broad query to collect all
    street addresses in Uster (PLZ 8610).
    """
    base_url = "https://api3.geo.admin.ch/rest/services/api/SearchServer"
    all_addresses = []
    seen = set()

    # Query each letter of the alphabet to get broad coverage
    # (swisstopo limits results to 50 per query)
    print("Fetching Uster addresses from swisstopo...")
    for prefix in "abcdefghijklmnopqrstuvwxyz":
        params = {
            "type":       "locations",
            "searchText": f"{prefix} uster 8610",
            "lang":       "de",
            "limit":      50,
            "sr":         "4326",
            "type":       "locations",
        }
        try:
            r = requests.get(base_url, params=params, timeout=10)
            if not r.ok:
                continue
            results = r.json().get("results", [])
            for res in results:
                attrs = res.get("attrs", {})
                label = attrs.get("label", "")
                # Only keep addresses in Uster (8610)
                if "8610" not in label and "uster" not in label.lower():
                    continue
                detail = attrs.get("detail", "")
                if detail in seen:
                    continue
                seen.add(detail)
                lat = attrs.get("lat")
                lon = attrs.get("lon")
                if lat and lon:
                    all_addresses.append({
                        "detail": detail,
                        "label":  label,
                        "lat":    float(lat),
                        "lon":    float(lon),
                    })
            time.sleep(0.2)
        except Exception as e:
            print(f"  Error on prefix '{prefix}': {e}")

    print(f"Fetched {len(all_addresses)} unique addresses from swisstopo")
    return all_addresses


def build_index(es: Elasticsearch):
    """Fetch addresses and bulk-index them into Elasticsearch."""
    create_index(es)

    raw = fetch_uster_addresses()

    def generate_docs():
        for addr in raw:
            detail = addr["detail"]
            # Build a clean address text: "Seestrasse 26 Uster 8610"
            address_text = detail.replace(",", " ").strip()
            yield {
                "_index": INDEX_NAME,
                "_source": {
                    "address_text":    address_text,
                    "address_suggest": {"input": [address_text]},
                    "street":          detail.split(" ")[0] if detail else "",
                    "city":            "Uster",
                    "zip":             "8610",
                    "location": {
                        "lat": addr["lat"],
                        "lon": addr["lon"],
                    },
                    "source": "swisstopo",
                }
            }

    count, errors = helpers.bulk(es, generate_docs(), stats_only=True)
    print(f"Indexed {count} addresses  ({errors} errors)")
    es.indices.refresh(index=INDEX_NAME)


# ── 4. Fuzzy search function ──────────────────────────────────────────────────
def geocode_elasticsearch(
    query: str,
    fuzziness: str = "AUTO",
    min_score: float = 3.0,
) -> tuple[float, float] | None:
    """Search for an address using Elasticsearch fuzzy matching.

    Returns (lat, lon) of the best match, or None if no good match found.

    Parameters:
        query      : address string, e.g. "Chammerholzstr. 10 Uster"
        fuzziness  : "AUTO" lets ES decide based on string length
                     (0 edits for 1-2 chars, 1 for 3-5, 2 for 6+)
        min_score  : minimum relevance score to accept a result

    This is the function to plug into geocode_with_fallback() as step 5.
    """
    try:
        es = get_client()
    except ConnectionError:
        return None  # ES not running — silently skip

    body = {
        "query": {
            "match": {
                "address_text": {
                    "query":     query,
                    "fuzziness": fuzziness,
                    "operator":  "and",
                }
            }
        },
        "size": 1,
        "min_score": min_score,
    }

    resp = es.search(index=INDEX_NAME, body=body)
    hits = resp["hits"]["hits"]
    if not hits:
        return None

    best = hits[0]["_source"]
    loc  = best["location"]
    score = hits[0]["_score"]
    print(f"  ES match [{score:.1f}]: '{best['address_text']}' → ({loc['lat']}, {loc['lon']})")
    return loc["lat"], loc["lon"]


# ── 5. Autocomplete endpoint (FastAPI) ────────────────────────────────────────
def start_api():
    """Start a FastAPI server that exposes the address search as a REST API.

    Endpoints:
      GET /search?q=Seestrasse+26        → fuzzy full-text search
      GET /suggest?q=Seestra             → autocomplete suggestions
      GET /health                        → health check

    Example:
      curl "http://localhost:8000/search?q=Chammerholzstr+10"
    """
    try:
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
        import uvicorn
    except ImportError:
        raise ImportError("Run: pip install fastapi uvicorn")

    app = FastAPI(title="Swiss Address Search – Uster", version="1.0")
    es  = get_client()

    @app.get("/health")
    def health():
        return {"status": "ok", "index": INDEX_NAME,
                "doc_count": es.count(index=INDEX_NAME)["count"]}

    @app.get("/search")
    def search(q: str, size: int = 5):
        """Fuzzy full-text address search."""
        body = {
            "query": {
                "match": {
                    "address_text": {
                        "query": q, "fuzziness": "AUTO", "operator": "and"
                    }
                }
            },
            "size": size,
        }
        resp = es.search(index=INDEX_NAME, body=body)
        results = [
            {
                "address": hit["_source"]["address_text"],
                "lat":     hit["_source"]["location"]["lat"],
                "lon":     hit["_source"]["location"]["lon"],
                "score":   hit["_score"],
            }
            for hit in resp["hits"]["hits"]
        ]
        return JSONResponse({"query": q, "results": results})

    @app.get("/suggest")
    def suggest(q: str):
        """Autocomplete suggestions as user types."""
        body = {
            "suggest": {
                "address_suggest": {
                    "prefix": q,
                    "completion": {
                        "field": "address_suggest",
                        "size":  5,
                        "fuzzy": {"fuzziness": 1},
                    }
                }
            }
        }
        resp    = es.search(index=INDEX_NAME, body=body)
        options = resp["suggest"]["address_suggest"][0]["options"]
        return JSONResponse({
            "query":       q,
            "suggestions": [o["text"] for o in options],
        })

    print("Starting address search API at http://localhost:8000")
    print("  GET /search?q=Seestrasse+26")
    print("  GET /suggest?q=Seestra")
    print("  GET /health")
    uvicorn.run(app, host="0.0.0.0", port=8000)


# ── 6. Integration into existing pipeline ────────────────────────────────────
"""
To integrate into construction_permits.py / popety_dag.py,
add this as step 5 in geocode_with_fallback():

    from elasticsearch_addon import geocode_elasticsearch

    def geocode_with_fallback(addr):
        ...
        # 4. Street name only (existing)
        c = geocode_swisstopo(f"{street}, Uster, 8610, Switzerland")
        if c and _in_uster(*c):
            return c[0], c[1], "needs_review:street_only"

        # 5. NEW: Elasticsearch fuzzy search
        c = geocode_elasticsearch(addr)
        if c and _in_uster(*c):
            return c[0], c[1], "elasticsearch_fuzzy"

        return None, None, "failed"
"""


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Elasticsearch address search add-on")
    parser.add_argument("--build-index", action="store_true",
                        help="Fetch Uster addresses and index them in Elasticsearch")
    parser.add_argument("--serve", action="store_true",
                        help="Start the FastAPI search API on port 8000")
    parser.add_argument("--search", type=str,
                        help="Test a single fuzzy search query")
    args = parser.parse_args()

    if args.build_index:
        es = get_client()
        build_index(es)

    elif args.serve:
        start_api()

    elif args.search:
        result = geocode_elasticsearch(args.search)
        if result:
            print(f"Result: lat={result[0]}, lon={result[1]}")
        else:
            print("No match found")

    else:
        parser.print_help()
