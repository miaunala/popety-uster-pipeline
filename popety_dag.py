# -*- coding: utf-8 -*-
"""
Popety Technical Test – Airflow DAG
====================================
6 tasks in 3 independent pairs (run in parallel):

  download_plots   >>  insert_plots
  download_permits >>  insert_permits
  download_stops   >>  insert_stops

Download tasks: fetch source data, process, save as GeoJSON to DATA_DIR.
Insert tasks:   load the GeoJSON into a PostGIS table in the local `geodb` database.
"""

import os
import re
import sys
import time
import requests
from datetime import datetime
from pathlib import Path

from airflow.sdk import DAG
from airflow.decorators import task

# ── Paths ──────────────────────────────────────────────────────────────────────
# Folder where GeoJSON files are written by download tasks and read by insert tasks
DATA_DIR = Path("/Users/nathalieguibert/Downloads/PopetyTest")

# Path to the locally downloaded GeoPackage (plots source)
GPKG_PATH = DATA_DIR / "79cb1aec092f4642bcc0cdcdbeb499c2" / \
    "AV_MOpublic-_Liegenschaften_-OGD" / \
    "AV_MOpublic-_Liegenschaften_-OGD.gpkg"

# ── Database ───────────────────────────────────────────────────────────────────
DB_URL = "postgresql://nathalieguibert@localhost/geodb"

# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="popety_uster_pipeline",
    description="Download plots, permits, stops for Uster and insert into PostGIS",
    schedule=None,          # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["popety", "uster"],
) as dag:

    # ──────────────────────────────────────────────────────────────────────────
    # DOWNLOAD TASKS
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="download_plots")
    def download_plots():
        """Read GeoPackage, filter to Uster (bfsnr=198), reproject to WGS84,
        keep real-estate-relevant attributes, save as plots.geojson."""
        import geopandas as gpd

        print(f"Reading GeoPackage from {GPKG_PATH} ...")
        plots = gpd.read_file(
            str(GPKG_PATH),
            layer="avzh_liegenschaften_f",
            where="bfsnr=198",
        )
        plots = plots.to_crs(epsg=4326)

        keep = ["egris_egrid", "nummer", "flaechenmass", "geometry"]
        plots = plots[[c for c in keep if c in plots.columns]]

        out = DATA_DIR / "plots.geojson"
        plots.to_file(str(out), driver="GeoJSON")
        print(f"Saved {len(plots)} plots to {out}")
        return str(out)

    @task(task_id="download_permits")
    def download_permits(plots_geojson_path: str):
        """Fetch permits from amtsblattportal.ch, parse addresses, geocode,
        spatial-join with plots, save as construction_permits.geojson.
        If a pre-geocoded file already exists, skip geocoding and reuse it."""
        import re, time, requests
        from io import BytesIO

        import geopandas as gpd
        import pandas as pd
        from shapely.geometry import Point
        from geopy.geocoders import Nominatim
        from geopy.exc import GeocoderQuotaExceeded
        import lxml.etree as LET

        # ── 0. Geocoding cache (address → {lat, lon, status}) ────────────────
        # Avoids re-geocoding known addresses on subsequent runs (rate-limit safe)
        import json as _json
        CACHE_FILE = DATA_DIR / "geocode_cache.json"
        out = DATA_DIR / "construction_permits.geojson"
        _geocache: dict = {}
        if CACHE_FILE.exists():
            try:
                _geocache = _json.loads(CACHE_FILE.read_text())
                print(f"Loaded geocoding cache: {len(_geocache)} entries")
            except Exception:
                _geocache = {}

        def _save_cache():
            CACHE_FILE.write_text(_json.dumps(_geocache, ensure_ascii=False, indent=2))

        # If geocoded file already exists, seed cache from it and reuse
        if out.exists() and not CACHE_FILE.exists():
            try:
                _existing = gpd.read_file(str(out))
                for _, r in _existing.iterrows():
                    addr = r.get("project_address")
                    if addr and r.geometry is not None:
                        _geocache[addr] = {
                            "lat": r.geometry.y, "lon": r.geometry.x,
                            "status": r.get("geocode_status", "swisstopo"),
                        }
                _save_cache()
                print(f"Seeded cache with {len(_geocache)} entries from existing file")
            except Exception as e:
                print(f"Cache seeding failed: {e}")
        if out.exists():
            print(f"Pre-geocoded file found at {out} – skipping geocoding.")
            return str(out)

        # ── 1. Fetch XML ──────────────────────────────────────────────────────
        url = "https://amtsblattportal.ch/api/v1/publications/xml"
        params = {
            "publicationStates": "PUBLISHED",
            "subRubrics": "BP-ZH01",
            "municipalityId": "198",
            "pageRequest.page": 0,
            "pageRequest.size": 200,
        }
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        raw_xml = r.text
        print(f"Fetched XML: {len(raw_xml)} chars")

        # ── 2. Parse XML ──────────────────────────────────────────────────────
        data = []
        context = LET.iterparse(
            BytesIO(raw_xml.encode("utf-8")),
            events=("end",),
            tag="{*}publication",
        )
        for _, elem in context:
            meta = elem.find(".//{*}meta")
            reg = meta.find(".//{*}registrationOffice") if meta is not None else None
            data.append({
                "id":          meta.findtext("{*}id")               if meta else None,
                "pub_date":    meta.findtext("{*}publicationDate")   if meta else None,
                "title":       meta.findtext("{*}title/{*}de")       if meta else None,
                "office_name": reg.findtext("{*}displayName")        if reg  else None,
            })
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]

        import polars as pl
        permits_raw = pl.DataFrame(data).with_columns(
            pl.col("pub_date").str.to_date("%Y-%m-%d")
        )
        print(f"Parsed {len(permits_raw)} permits")

        # ── 3. Load plots for boundary + spatial join ─────────────────────────
        plots = gpd.read_file(plots_geojson_path)
        plots = plots.to_crs(epsg=4326)
        uster_boundary = plots.dissolve().geometry.iloc[0]

        # ── 4. Address parsing helpers ────────────────────────────────────────
        _STREET_RE = re.compile(
            r"(strasse|str\.?|weg|gasse|platz|allee|rain|steig|stieg|halde"
            r"|holz|riet|berg|bühl|bach|wies(?:en)?|roos|matt(?:en)?"
            r"|feld|graben|ring|bogen|park|gässli|ufer|dorf)",
            re.IGNORECASE,
        )

        def _looks_like_street(s):
            return bool(_STREET_RE.search(s))

        def parse_addresses(title):
            if not title:
                return []
            m = re.search(r"Bauprojekt:\s*(.+)", title)
            if not m:
                return []
            raw = m.group(1)
            raw = re.split(r",?\s*[Aa]ssek\.?", raw)[0].strip()
            raw = re.sub(r"^bei\s+", "", raw, flags=re.IGNORECASE)
            raw = re.sub(r"\s+bei\s+Nr\.?\s*", " ", raw, flags=re.IGNORECASE)
            raw = re.sub(r"\s+und\s+(?=\d)", "/", raw, flags=re.IGNORECASE)
            slash_parts = re.split(r"\s*/\s*", raw)
            merged, current = [], slash_parts[0]
            for part in slash_parts[1:]:
                if _looks_like_street(part):
                    dp = re.match(r'^(\d[\w]*)\s*,\s*(.+)', part)
                    if dp:
                        current = current + "/" + dp.group(1)
                        merged.append(current.strip())
                        current = dp.group(2).strip()
                    else:
                        merged.append(current.strip())
                        current = part.strip()
                else:
                    current = current + "/" + part
            merged.append(current.strip())
            street_parts = []
            for part in merged:
                sub = re.split(r",\s*", part)
                cur = sub[0]
                for s in sub[1:]:
                    if _looks_like_street(s):
                        street_parts.append(cur.strip())
                        cur = s.strip()
                street_parts.append(cur.strip())
            valid = []
            for addr in street_parts:
                addr = addr.strip()
                if not addr:
                    continue
                m_street = re.match(r'^(.+?)\s+(\d[\w/\-]*)$', addr)
                if m_street:
                    street_name, num_part = m_street.group(1), m_street.group(2)
                    if re.match(r'^\d+-\d+$', num_part):
                        start, end = map(int, num_part.split("-"))
                        step = 2 if (end - start) % 2 == 0 and end > start + 1 else 1
                        valid.extend(f"{street_name} {n}, Uster, 8610, Switzerland"
                                     for n in range(start, end + 1, step))
                        continue
                    slash_nums = num_part.split("/")
                    if len(slash_nums) > 1 and all(
                        re.match(r'^\d+[a-z]?$', p, re.IGNORECASE) for p in slash_nums
                    ):
                        valid.extend(f"{street_name} {n}, Uster, 8610, Switzerland"
                                     for n in slash_nums)
                        continue
                if re.search(r"\d", addr) or _looks_like_street(addr):
                    valid.append(f"{addr}, Uster, 8610, Switzerland")
            return valid

        # ── 5. Geocoding helpers ──────────────────────────────────────────────
        def _in_uster(lat, lon):
            return uster_boundary.contains(Point(lon, lat))

        def geocode_swisstopo(addr):
            addr = re.sub(r'(\d+)\.\d+', r'\1', addr)
            params = {"type": "locations", "searchText": addr,
                      "lang": "de", "limit": 1, "sr": "4326"}
            try:
                resp = requests.get(
                    "https://api3.geo.admin.ch/rest/services/api/SearchServer",
                    params=params, timeout=10)
                results = resp.json().get("results", [])
                if not results:
                    return None
                attrs = results[0].get("attrs", {})
                street_part = addr.split(",")[0]
                street_words = " ".join(
                    w for w in street_part.split()
                    if not re.match(r'^\d', w) and len(w) > 2
                ).lower()
                detail = attrs.get("detail", "").lower()
                if street_words and street_words not in detail:
                    return None
                lat, lon = attrs.get("lat"), attrs.get("lon")
                if lat is not None and lon is not None:
                    return float(lat), float(lon)
            except Exception:
                pass
            return None

        _nom = Nominatim(user_agent="popety_airflow_dag")

        def geocode_nominatim(addr):
            for attempt in range(3):
                try:
                    loc = _nom.geocode(addr, timeout=10, country_codes="ch")
                    if loc:
                        if loc.raw.get("type") not in ("house", "building"):
                            return None
                        return loc.latitude, loc.longitude
                    return None
                except GeocoderQuotaExceeded:
                    time.sleep(30)
                except Exception as e:
                    if "429" in str(e):
                        time.sleep(5 * (attempt + 1))
                    else:
                        return None
            return None

        def _try(addr):
            c = geocode_swisstopo(addr)
            if c and _in_uster(*c):
                return c
            time.sleep(1.5)  # respect rate limit (was 0.5s → too many 429s)
            c = geocode_nominatim(addr)
            if c and _in_uster(*c):
                return c
            return None

        def _strip_suffix(num):
            m = re.match(r'^(\d+)[a-z]+$', num, re.IGNORECASE)
            return m.group(1) if m else None

        def _decrement(street, num, steps=5):
            bm = re.match(r'^(\d+)', num)
            if not bm:
                return None, None
            base = int(bm.group(1))
            step = 2 if base > 1 else 1
            for i in range(1, steps + 1):
                cand = base - i * step
                if cand < 1:
                    break
                c = _try(f"{street} {cand}, Uster, 8610, Switzerland")
                if c:
                    return c, f"{street} {cand}"
            return None, None

        def geocode_with_fallback(addr):
            addr = re.sub(r'(\d+)\.\d+', r'\1', addr)
            c = geocode_swisstopo(addr)
            if c and _in_uster(*c):
                return c[0], c[1], "swisstopo"
            time.sleep(0.5)
            c = geocode_nominatim(addr)
            if c and _in_uster(*c):
                return c[0], c[1], "nominatim"
            san = addr.split(",")[0].strip()
            mp = re.match(r'^(.+?)\s+(\S+)$', san)
            if not mp:
                return None, None, "failed"
            street, num = mp.group(1), mp.group(2)
            base_num = _strip_suffix(num)
            if base_num:
                c = _try(f"{street} {base_num}, Uster, 8610, Switzerland")
                if c:
                    return c[0], c[1], "needs_review:no_suffix"
            c, _ = _decrement(street, base_num or num)
            if c:
                return c[0], c[1], "needs_review:lower_number"
            sc = geocode_swisstopo(f"{street}, Uster, 8610, Switzerland")
            if sc and _in_uster(*sc):
                return sc[0], sc[1], "needs_review:street_only"
            return None, None, "failed"

        # ── 6. Explode + geocode ──────────────────────────────────────────────
        rows = []
        for row in permits_raw.iter_rows(named=True):
            addresses = parse_addresses(row["title"])
            if not addresses:
                rows.append({**row, "project_address": None, "parse_status": "no_address"})
            else:
                for addr in addresses:
                    status = "street_only" if not re.search(r"\d", addr) else "ok"
                    rows.append({**row, "project_address": addr, "parse_status": status})
        permits_exploded = pd.DataFrame(rows)

        lats, lons, statuses = [], [], []
        new_cache_entries = 0
        for _, row in permits_exploded.iterrows():
            if row["parse_status"] == "no_address":
                lats.append(None); lons.append(None); statuses.append("skipped")
                continue
            addr = row["project_address"]
            if addr in _geocache:
                cached = _geocache[addr]
                lats.append(cached["lat"]); lons.append(cached["lon"]); statuses.append(cached["status"])
            else:
                lat, lon, status = geocode_with_fallback(addr)
                _geocache[addr] = {"lat": lat, "lon": lon, "status": status}
                new_cache_entries += 1
                if new_cache_entries % 10 == 0:
                    _save_cache()   # save progress every 10 new geocodes
                lats.append(lat); lons.append(lon); statuses.append(status)
        _save_cache()
        print(f"Geocoded {new_cache_entries} new addresses ({len(_geocache)} total in cache)")

        permits_exploded["lat"] = lats
        permits_exploded["lon"] = lons
        permits_exploded["geocode_status"] = statuses

        # ── 7. Spatial join ───────────────────────────────────────────────────
        mask = permits_exploded["lat"].notna()
        geocoded_gdf = gpd.GeoDataFrame(
            permits_exploded[mask].copy(),
            geometry=gpd.points_from_xy(
                permits_exploded.loc[mask, "lon"],
                permits_exploded.loc[mask, "lat"],
            ),
            crs="EPSG:4326",
        )
        plots_slim = plots[["egris_egrid", "nummer", "flaechenmass", "geometry"]].copy()
        joined = gpd.sjoin(geocoded_gdf, plots_slim, how="left", predicate="within")
        matched = joined["index_right"].notna()
        joined.loc[matched, "geometry"] = (
            plots_slim.loc[joined.loc[matched, "index_right"].astype(int), "geometry"].values
        )
        not_geocoded = gpd.GeoDataFrame(
            permits_exploded[~mask].copy(),
            geometry=[None] * (~mask).sum(),
            crs="EPSG:4326",
        )
        keep_cols = [c for c in joined.columns if c != "index_right"]
        permits_final = gpd.GeoDataFrame(
            pd.concat([joined[keep_cols], not_geocoded.reindex(columns=keep_cols)],
                      ignore_index=True),
            geometry="geometry", crs="EPSG:4326",
        )

        # ── 8. Save ───────────────────────────────────────────────────────────
        permits_final["pub_date"] = permits_final["pub_date"].astype(str)
        out = DATA_DIR / "construction_permits.geojson"
        permits_final[permits_final.geometry.notna()].to_file(str(out), driver="GeoJSON")
        print(f"Saved {permits_final.geometry.notna().sum()} permits to {out}")
        return str(out)

    @task(task_id="download_stops")
    def download_stops():
        """Fetch public transport stops for Uster from Overpass API,
        deduplicate, save as stops.geojson."""
        import geopandas as gpd
        from shapely.geometry import Point

        query = """
        [out:json][timeout:30];
        area["name"="Uster"]["boundary"="administrative"]["admin_level"="8"]->.uster;
        (
          node["public_transport"="stop_position"](area.uster);
          node["public_transport"="platform"](area.uster);
          node["highway"="bus_stop"](area.uster);
          node["railway"="tram_stop"](area.uster);
          node["railway"="station"](area.uster);
          node["railway"="halt"](area.uster);
        );
        out body;
        """
        import urllib.request, urllib.parse, json as _json
        mirrors = [
            "https://lz4.overpass-api.de/api/interpreter",
            "https://overpass.kumi.systems/api/interpreter",
            "https://overpass-api.de/api/interpreter",
        ]
        payload = urllib.parse.urlencode({"data": query}).encode()
        elements = None
        for url in mirrors:
            req = urllib.request.Request(
                url, data=payload,
                headers={"User-Agent": "PopetyUsterPipeline/1.0"},
            )
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    elements = _json.loads(resp.read()).get("elements", [])
                print(f"Overpass OK via {url}, {len(elements)} elements")
                break
            except Exception as e:
                print(f"Overpass mirror {url} failed: {e}")
        if elements is None:
            raise RuntimeError("All Overpass mirrors failed")

        rows = []
        for el in elements:
            tags = el.get("tags", {})
            rows.append({
                "osm_id":   el["id"],
                "name":     tags.get("name"),
                "type":     (tags.get("public_transport")
                             or tags.get("highway")
                             or tags.get("railway")),
                "network":  tags.get("network"),
                "operator": tags.get("operator"),
                "ref":      tags.get("ref"),
                "geometry": Point(el["lon"], el["lat"]),
            })

        stops = gpd.GeoDataFrame(rows, crs="EPSG:4326")
        stops = stops[stops["name"].notna()].reset_index(drop=True)
        stops = stops.drop_duplicates(subset=["name", "geometry"]).reset_index(drop=True)

        out = DATA_DIR / "stops.geojson"
        stops.to_file(str(out), driver="GeoJSON")
        print(f"Saved {len(stops)} stops to {out}")
        return str(out)

    # ──────────────────────────────────────────────────────────────────────────
    # INSERT TASKS
    # ──────────────────────────────────────────────────────────────────────────

    @task(task_id="insert_plots")
    def insert_plots(geojson_path: str):
        """Load plots.geojson into PostGIS table 'plots' (replace if exists)."""
        import geopandas as gpd
        from sqlalchemy import create_engine

        gdf = gpd.read_file(geojson_path)
        gdf = gdf.to_crs(epsg=4326)
        engine = create_engine(DB_URL)
        gdf.to_postgis("plots", engine, if_exists="replace", index=False)
        print(f"Inserted {len(gdf)} rows into table 'plots'")

    @task(task_id="insert_permits")
    def insert_permits(geojson_path: str):
        """Load construction_permits.geojson into PostGIS table 'permits'."""
        import geopandas as gpd
        from sqlalchemy import create_engine

        gdf = gpd.read_file(geojson_path)
        gdf = gdf.to_crs(epsg=4326)
        engine = create_engine(DB_URL)
        gdf.to_postgis("permits", engine, if_exists="replace", index=False)
        print(f"Inserted {len(gdf)} rows into table 'permits'")

    @task(task_id="insert_stops")
    def insert_stops(geojson_path: str):
        """Load stops.geojson into PostGIS table 'stops'."""
        import geopandas as gpd
        from sqlalchemy import create_engine

        gdf = gpd.read_file(geojson_path)
        gdf = gdf.to_crs(epsg=4326)
        engine = create_engine(DB_URL)
        gdf.to_postgis("stops", engine, if_exists="replace", index=False)
        print(f"Inserted {len(gdf)} rows into table 'stops'")

    # ──────────────────────────────────────────────────────────────────────────
    # DEPENDENCIES  (3 independent pairs, run in parallel)
    # ──────────────────────────────────────────────────────────────────────────
    plots_path   = download_plots()
    permits_path = download_permits(plots_path)   # waits for plots to finish
    stops_path   = download_stops()

    insert_plots(plots_path)
    insert_permits(permits_path)
    insert_stops(stops_path)
