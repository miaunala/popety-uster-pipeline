# Presentation Notes – Popety Technical Test

---

## Einstieg (1–2 Minuten)

**Was du sagst:**
> "I chose Uster in the canton of Zurich. All three data sources were available, the municipality is large enough to have meaningful permit activity, and I already knew the area slightly. The pipeline covers three separate datasets — cadastral plots, construction permits, and public transport stops — all brought together into a PostGIS database and visualised on an interactive map."

**Warum Uster:**
- Kanton Zürich → swisstopo-Daten gut verfügbar
- Aktive Bautätigkeit → echte Permits vorhanden
- Amtsblatt-API für Kanton Zürich funktioniert zuverlässig

---

## PART 1 – Notebook

### 1.1 Plots of land

**Was du zeigst:** Section 0 im Notebook / `plots.geojson`

**Was du sagst:**
> "The cadastral data comes from the canton of Zurich's AV_MOpublic GeoPackage — the official land survey. I load only the Uster parcels using the BFS municipality code 198 as a SQL filter directly on the GeoPackage layer. From the ~30 available columns, I kept four that are directly relevant to real estate: the federal parcel ID (EGRID), the local parcel number, the area in m², and the geometry."

**Warum diese Spalten:**
- `egris_egrid` — stabiler Bundesbezeichner, verknüpft mit GWR und anderen Bundesregistern
- `nummer` — für Anzeige und Querverweise mit Gemeindedaten
- `flaechenmass` — Kernattribut für Immobilienwert und Bebauungspotenzial
- `geometry` — benötigt für alle räumlichen Operationen

**CRS:**
> "The source data is in Swiss LV95 (EPSG:2056). I export everything in WGS84 (EPSG:4326) as the unified CRS for all three outputs — standard for GeoJSON and web maps. For the distance buffer in Part 3, I temporarily reproject back to LV95 where metric accuracy matters."

---

### 1.2 Construction permits

**Was du zeigst:** Sections 1–9 im Notebook, `construction_permits.geojson`, `permits_map.html`

**Datenquelle:**
> "Construction permits in Switzerland are published in the official gazette (Amtsblatt). There's a public REST API at amtsblattportal.ch. I filter by sub-rubric BP-ZH01 — Zurich building permits — and municipality 198. The API returns XML."

**Was der Code macht (Schritt für Schritt):**

1. **XML parsing** — `lxml.iterparse` für memory-effizientes Streaming (grosse XML-Dateien werden nicht komplett geladen)
2. **Address extraction** — Regex-Parser der Freitext-Titel wie *"Bauprojekt: Guldenenstrasse 14-18, Assek. Nrn. 123, Uster"* verarbeitet. Dabei: Assek.-Annotationen entfernen, Slash-Splits nur bei echten Strassennamen, Nummernsplits, Ranges expandieren
3. **Explode** — Ein Permit kann mehrere Adressen haben → eine Zeile pro Adresse
4. **Geocoding** — swisstopo primary, Nominatim fallback, mehrere Fallbacks
5. **Spatial join** — geocodierter Punkt → welche Parzelle enthält ihn → Permit bekommt Parzellenpolygon als Geometry
6. **Export** — GeoJSON + CSV für nicht-geocodierte Permits

**Was gut funktioniert:**
- swisstopo deckt fast alle offiziell registrierten Schweizer Adressen ab
- Boundary validation verhindert False Positives aus anderen Gemeinden
- Geocoding-Cache vermeidet Rate-Limit-Probleme bei Folgeruns

**Was schlecht funktioniert / ehrliche Einschränkungen:**
- 64% Geocoding-Rate — 36% nicht gefunden, meist neue Gebäude die noch nicht im swisstopo-Register sind
- Swisstopo 429-Errors bei schnellen Requests → 1.5s Sleep nötig
- Nominatim interpoliert Hausnummern → type-check nötig

**Auf der Karte zeigen:**
- Grün = exact match (swisstopo / Nominatim)
- Orange = needs review (Suffix gestripped, niedrigere Hausnummer, nur Strassenname)
- Layer ein/ausblenden demonstrieren

---

### 1.3 Public transport stops

**Was du zeigst:** Section 10, `stops.geojson`

**Was du sagst:**
> "For transit stops I use OpenStreetMap via the Overpass API. I query all node types relevant to public transport within the Uster administrative boundary — bus stops, tram stops, stations, platforms. The result is deduplicated because the same physical stop often appears twice in OSM: once as stop_position and once as platform."

**Technischer Hinweis falls gefragt:**
- POST statt GET — die Overpass API gibt 406 auf GET-Requests ohne korrekten User-Agent zurück
- Mehrere Mirror-Server als Fallback (lz4, kumi.systems, overpass-api.de)

---

## PART 2 – Apache Airflow DAG

> **Tipp:** Zeige den DAG im Browser auf `http://localhost:8080` während du erklärst. Starte Airflow vorher mit `airflow standalone` im Terminal.

**Was Airflow ist (falls erklärt werden muss):**
> "Airflow is a workflow orchestration tool. You define tasks and their dependencies as a DAG — a directed acyclic graph. Airflow then schedules and monitors the execution. It's used in production data pipelines to make them reliable, observable, and restartable."

**DAG-Struktur erklären:**

```
download_plots  ──►  insert_plots
download_permits ──►  insert_permits      ← diese drei Paare laufen parallel
download_stops  ──►  insert_stops
```

> "The DAG has six tasks in three pairs. Each pair is independent — they run in parallel. Within each pair, the insert task depends on the download task. So plots can be downloading at the same time as stops, but insert_plots waits until download_plots has finished and written its file."

**Im UI zeigen:**
1. DAG-Graph (Graphansicht) — zeigt die Abhängigkeiten visuell
2. Einen erfolgreichen Run zeigen — alle 6 Tasks grün
3. Log eines Tasks öffnen (z.B. `insert_plots`) — zeigt was wirklich passiert ist

**Herausforderungen die du erwähnen kannst:**

1. **Race condition** — `download_permits` liest `plots.geojson`, das von `download_plots` geschrieben wird. Wenn beide parallel starten, kann `download_permits` eine unfertige Datei lesen. Fix: `download_permits` bekommt den Rückgabewert von `download_plots` als Input → Airflow weiss dadurch, dass es warten muss.

2. **Missing packages** — Airflow läuft in einer eigenen Python-Umgebung. geopandas, shapely, psycopg2 mussten separat installiert werden.

3. **Geocoding in der DAG** — Der `download_permits` Task macht 137 Geocoding-Anfragen. Swisstopo blockt bei zu schnellen Requests. Lösung: Geocoding-Cache in `geocode_cache.json` — beim ersten Run alles geocodieren und cachen, bei Folgeruns nur neue Adressen.

4. **Overpass API 406** — Die Overpass API gibt 406 auf normale GET-Requests zurück. Fix: POST statt GET + User-Agent Header + mehrere Mirror-Server.

**Auf die Frage "Warum Airflow und nicht ein einfaches Python-Script?":**
> "Airflow adds observability — you can see exactly which task failed and why, retry individual tasks without rerunning everything, and schedule the pipeline to run automatically, e.g. monthly when new permits are published. For a production system that runs regularly, this matters a lot."

**Verbesserungsmöglichkeit die du proaktiv erwähnen kannst:**
> "One improvement I'd make: move the geocoding cache from a JSON file to the PostGIS database. That way it survives environment resets and is queryable — you could ask 'which addresses have we never been able to geocode?' directly in SQL."

---

## PART 3 – Interactive Map

**Was du zeigst:** `part3_map.html` im Browser öffnen

**Was du sagst:**
> "The Part 3 map brings all three datasets together. It shows three things: all cadastral plots as a gray background, plots with an active construction permit highlighted in blue, and plots within a 3-minute walk of a public transport stop in green."

**Methodisches vorgehen erklären:**

1. **Plots with permits** — Spatial join: permit point within plot polygon → plot gets highlighted
2. **3-minute walk** — Walking speed ≈ 5 km/h → 3 min ≈ 250 m. Buffer is computed in Swiss LV95 (metric CRS) for accurate distances, then reprojected to WGS84. Any plot that intersects a 250 m buffer is highlighted green.

**Auf der Karte demonstrieren:**
- Layer ein/ausschalten (Layer Control rechts oben)
- Auf einen blauen Plot klicken → EGRID, Parzellennummer anzeigen
- "250 m walk buffer" Layer einschalten → zeigt die Kreise um Haltestellen
- Rote Punkte = Haltestellen (Tooltip zeigt Name und Typ)

**Zahlen nennen:**
> "Of 8121 plots in Uster, 87 have an active construction permit — that's about 1%. 5357 plots, or 66%, are within a 3-minute walk of a transit stop — which makes sense for a well-connected Swiss municipality."

---

## Appendix – Falls Fragen kommen

### "Warum 64% Geocoding-Rate, was ist mit den anderen 36%?"

> "Most of the failed addresses are newly approved buildings whose addresses aren't yet registered in swisstopo or OSM. The Swiss federal address register is updated when a building is officially completed — but permits are issued earlier in the process. This is actually one motivation for also exploring the GWR/RegBL: it has a `gstat` field for construction status, including 'permitted' and 'under construction', which would give us earlier and more complete coverage."

### "Was würdest du anders machen?"

> "Three things: First, use the Swiss RegBL (GWR) directly for construction status — it's updated within 48 hours of cantonal entries and has richer building data. Second, store geocoding results in PostGIS instead of a JSON file. Third, add a monthly schedule to the Airflow DAG so it automatically picks up new permits."

### "Hast du AI-Tools verwendet?"

Das ist eine persönliche Entscheidung wie du das beantwortest. Ehrliche Antwort: ja, Claude hat beim Debugging geholfen (Airflow-Importfehler, Overpass 406, Race Condition). Den Geocoding-Ansatz, die Adressnormalisation und die Entscheidungen zu Datenquellen hast du aktiv mitgestaltet.

---

## Zwei Airflow-Dateien — kurze Erklärung

Falls gefragt warum es zwei gibt:

- **`popety_dag.py`** — der produktive DAG in `~/airflow/dags/`. Wird von Airflow automatisch geladen. Enthält den vollständigen Pipeline-Code direkt in den Task-Funktionen.
- **`construction_permits.py` / `construction_permits.ipynb`** — das explorative Notebook aus Part 1. Zeigt dieselbe Logik in lesbarer, interaktiver Form mit mehr Kommentaren und visuellen Checks.

> "The DAG is essentially a productionised version of the notebook — same logic, but wrapped in Airflow's task framework so it can be scheduled, monitored, and restarted independently per task."

---

## Elasticsearch Add-on (Section 12)

**Was du zeigst:** `elasticsearch_addon.py` + Section 12 im Notebook

**Was du sagst:**
> "After building the pipeline I identified that ~36% of addresses failed to geocode — mostly due to abbreviations like 'Str.' instead of 'Strasse', or small typos. I added Elasticsearch as an optional additional fallback that uses fuzzy matching to find the closest known address."

**Wie es funktioniert:**
1. `build_index` — lädt alle 1043 Uster-Adressen von swisstopo in einen ES-Index
2. `geocode_elasticsearch` — fuzzy sucht darin wenn alle anderen Fallbacks scheitern
3. Optional: REST API über FastAPI → andere User können über HTTP suchen

**Was `create_index` macht — falls gefragt:**
- `address_text` mit `standard` analyzer → lowercase + tokenisierung → fuzzy search
- `address_suggest` mit `completion` type → Autocomplete wenn User tippt
- `location` als `geo_point` → für spätere räumliche Queries
- `address_text.keyword` → exakter Match ohne Analyse

**Warum ES und nicht nur rapidfuzz:**
> "rapidfuzz works inside a single Python script. Elasticsearch runs as a service — multiple users can query it simultaneously via a REST API, which is what a production platform like Popety needs."

**Feature Flag:**
```python
USE_ELASTICSEARCH = True  # oben im Script — einfach an/ausschalten
```

---

## Zeitplan (Richtwert für 20–30 Minuten Präsentation)

| Teil | Zeit |
|---|---|
| Einstieg + Überblick | 2 min |
| Part 1: Plots | 3 min |
| Part 1: Permits (Geocoding erklären) | 8 min |
| Part 1: Stops | 2 min |
| Part 2: Airflow DAG | 7 min |
| Part 3: Karte demonstrieren | 3 min |
| Appendix / Fragen | rest |
