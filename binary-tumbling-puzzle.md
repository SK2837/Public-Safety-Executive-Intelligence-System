# PeregrineOps — Public Safety Executive Intelligence System

## Context

Building a greenfield operational intelligence platform that simulates what Peregrine sells to city governments: ingesting multi-source public safety data, running anomaly detection, and delivering AI-generated executive briefings. The project demonstrates a full modern data stack (dbt + DuckDB + LangChain + Streamlit) in a single day.

Working directory: `/Users/adarshkasula/Documents/Public Safety Executive Intelligence System/`

---

## Project Structure

```
├── .env                              # ANTHROPIC_API_KEY
├── requirements.txt
├── run_pipeline.py                   # One-command orchestrator
│
├── data/
│   └── peregrine.duckdb             # Git-ignored database file
│
├── scripts/
│   └── generate_synthetic_data.py   # Layer 1: synthetic data generation
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml                  # DuckDB path: ../data/peregrine.duckdb
│   ├── models/
│   │   ├── raw/                      # Type-cast pass-through models
│   │   │   ├── raw_incidents.sql
│   │   │   ├── raw_dispatch_logs.sql
│   │   │   └── raw_sensor_feeds.sql
│   │   ├── staging/                  # Cleaning + enrichment views
│   │   │   ├── stg_incidents.sql
│   │   │   ├── stg_dispatch_logs.sql
│   │   │   └── stg_sensor_feeds.sql
│   │   └── marts/                    # Analytical aggregates (tables)
│   │       ├── mart_response_times.sql
│   │       ├── mart_incident_clusters.sql
│   │       └── mart_resource_gaps.sql
│   └── tests/
│       └── assert_response_time_positive.sql
│
├── anomaly_detection/
│   ├── __init__.py
│   ├── models.py                     # Pydantic: Anomaly, AnomalySeverity, AnomalyType
│   └── detector.py                   # Layer 2: AnomalyDetector class
│
├── briefing_engine/
│   ├── __init__.py
│   ├── prompts.py                    # LangChain ChatPromptTemplate
│   └── generator.py                  # Layer 3: BriefingGenerator class
│
└── dashboard/
    ├── app.py                        # Layer 4: Streamlit entry point
    ├── data_loader.py                # Cached DuckDB query helpers
    └── components/
        ├── __init__.py
        ├── kpi_cards.py
        ├── anomaly_timeline.py
        ├── hotspot_map.py
        └── briefing_panel.py
```

---

## Layer 1: Synthetic Data Generation

**File:** `scripts/generate_synthetic_data.py`

Three agencies: POLICE, FIRE, EMS. 90 days of data. City centered at lat 37.77, lon -122.41.

### Tables Written to `raw.*` Schema

**`raw.incidents`** (5,000 rows)
| Column | Type | Notes |
|---|---|---|
| incident_id | VARCHAR | UUID |
| agency | VARCHAR | POLICE, FIRE, EMS |
| incident_type | VARCHAR | Agency-specific enums |
| reported_at | TIMESTAMP | |
| latitude/longitude | DOUBLE | City bounding box |
| priority | INTEGER | 1=critical, 4=low |
| district | VARCHAR | NORTH/SOUTH/EAST/WEST/CENTRAL |
| status | VARCHAR | CLOSED/OPEN/ESCALATED |

Deliberate anomaly injection:
- 3 volume spikes: days 15, 45, 72 → 3x POLICE incident volume
- 1 geographic hotspot cluster: 15% of incidents within 0.5km radius

**`raw.dispatch_logs`** (~8,700 rows, 1-3 per incident)
| Column | Type | Notes |
|---|---|---|
| log_id | VARCHAR | UUID |
| incident_id | VARCHAR | FK |
| unit_id | VARCHAR | e.g. P-14, F-03 |
| dispatched_at/arrived_at/cleared_at | TIMESTAMP | |
| response_time_seconds | INTEGER | Log-normal, mean 420s |
| units_available_at_dispatch | INTEGER | 1-12 |

Spike days: response time × 1.8 to simulate resource strain.

**`raw.sensor_feeds`** (2,000 rows)
| Column | Type |
|---|---|
| feed_id, sensor_id, sensor_type | VARCHAR |
| CAMERA/GUNSHOT_DETECTOR/TRAFFIC/WEATHER | enum |
| recorded_at, latitude, longitude | |
| value, unit, anomaly_flag | |

---

## Layer 1b: dbt Models

### Staging (views)
- `stg_incidents`: filters invalid coords, adds `reported_date`, `reported_hour`, `priority_label`, `is_escalated`
- `stg_dispatch_logs`: adds `service_duration_seconds`, `response_tier` (FAST/NORMAL/SLOW/CRITICAL_DELAY), filters invalid timestamps
- `stg_sensor_feeds`: adds `rolling_24h_avg` via window function, filters nulls

### Marts (tables)
- `mart_response_times`: daily avg/median/p90 response time by agency + district + priority
- `mart_incident_clusters`: ~111m grid cells, incident counts + escalation rates, density rank per day
- `mart_resource_gaps`: hourly demand vs. capacity, flags `resource_gap_flag` when avg_units_available < 3 AND incident_volume > 5, or avg_response_time_s > 900

**dbt execution order (DAG):**
1. raw_incidents, raw_dispatch_logs, raw_sensor_feeds (parallel)
2. stg_* (parallel, depend on raw)
3. mart_* (parallel, depend on staging)

---

## Layer 2: Anomaly Detection

**File:** `anomaly_detection/detector.py` — class `AnomalyDetector`

### Detection Methods

**`detect_volume_spikes()`**
- Query mart_response_times for daily incident counts by agency
- Pandas rolling(7) mean + std per agency
- Z-score threshold: 2.5 → flag; severity: MEDIUM(<3), HIGH(<4), CRITICAL(≥4)
- Last 7 days only

**`detect_response_time_spikes()`**
- Rolling 14-day z-score per (agency, district)
- Z-score threshold: 2.0 (lower because ops-critical)
- Hard threshold: p90 > 1800s (30 min) → at least HIGH regardless of z

**`detect_geographic_hotspots()`**
- Query mart_incident_clusters last 7 days
- Global mean/std of incident_count across grid cells
- Z-score > 2.0 → hotspot
- Merge nearby cells within 0.005° (~500m) into single anomaly with centroid

**`detect_resource_gaps()`**
- Query mart_resource_gaps where resource_gap_flag = TRUE
- Last 24h: >2 gap hours → HIGH, >4 gap hours → CRITICAL

**`compute_kpi_snapshot() -> dict`**
Returns: total_incidents_24h/7d, avg/p90 response time 24h, active/critical anomaly counts, busiest_district, agency breakdown dict

---

## Layer 3: LLM Briefing Engine

**Files:** `briefing_engine/prompts.py`, `briefing_engine/generator.py`

### Prompt Design
- System: "You are PeregrineOps, an executive intelligence analyst... authoritative, data-driven, direct. Use specific numbers."
- Human template vars: `{kpi_snapshot}`, `{anomaly_summary}`, `{current_datetime}`
- Output: 3 plain paragraphs — Current Situation | Anomalies & Risk | Recommended Actions

### `BriefingGenerator` Class
```python
self.llm = ChatAnthropic(model="claude-sonnet-4-6", api_key=api_key, max_tokens=1024)
self.chain = prompt_template | self.llm | StrOutputParser()
```

- `format_kpi_snapshot(kpi)` → readable string block with agency breakdown
- `format_anomaly_summary(anomalies)` → top 8, sorted by severity, numbered list with z-scores
- `generate(kpi, anomalies) -> str` → invokes chain, try/except fallback

---

## Layer 4: Streamlit Dashboard

**Entry:** `dashboard/app.py` — `st.set_page_config(layout="wide")`

### Layout
```
HEADER: PeregrineOps | Last Updated | [Refresh]
────────────────────────────────────────────────
[KPI: Incidents 24h] [Avg RT] [P90 RT] [Anomalies] [Critical]
────────────────────────────────────────────────
[Tab: Overview] [Tab: Anomalies] [Tab: Map] [Tab: AI Briefing]
────────────────────────────────────────────────
Tab content
```

### Components
- **`kpi_cards.py`**: `st.columns(5)` + `st.metric()` with deltas; critical anomalies always red
- **`anomaly_timeline.py`**: Plotly scatter — x=detected_at, y=z_score, color=severity, symbol=type; styled dataframe below
- **`hotspot_map.py`**: `px.scatter_mapbox` with `carto-positron` (no token needed); overlay anomaly centroids as translucent red circles; fallback to folium if needed
- **`briefing_panel.py`**: `st.session_state["briefing_text"]` persists between rerenders; "Regenerate Briefing" button + `st.spinner`; styled container via `unsafe_allow_html=True`

**`data_loader.py`**: `@st.cache_data(ttl=300)` on all query functions to prevent re-queries on rerender.

---

## Requirements

```
dbt-duckdb==1.9.1
duckdb==1.2.1
pandas==2.2.3
numpy==2.0.2
pydantic==2.10.6
langchain==0.3.19
langchain-anthropic==0.3.10
anthropic==0.49.0
streamlit==1.43.2
plotly==5.24.1
folium==0.19.3
streamlit-folium==0.23.2
python-dotenv==1.0.1
scipy==1.15.2
```

---

## Verification / End-to-End Run

```bash
# 1. Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
echo "ANTHROPIC_API_KEY=sk-ant-..." > .env

# 2. Generate data
python scripts/generate_synthetic_data.py
# Expect: 5000 incidents, ~8700 dispatch logs, 2000 sensor feeds written to peregrine.duckdb

# 3. Run dbt
cd dbt_project && dbt run && dbt test && cd ..
# Expect: 9 models created (3 raw + 3 staging + 3 mart), all tests pass

# 4. Launch dashboard
streamlit run dashboard/app.py
# Opens http://localhost:8501
# Verify: KPI cards show real numbers, anomaly timeline has plotted points,
#         map shows incident scatter, AI briefing generates on "Regenerate" click

# Or run all steps at once:
python run_pipeline.py
```

---

## Key Design Decisions

- **DuckDB**: In-process, no server, single-file persistence — zero infrastructure overhead
- **Raw dbt models**: Wrap Python-written tables so full dbt lineage graph is captured; provides typed casting layer
- **pandas rolling() for z-scores**: More control than scipy.stats.zscore; handles window edge cases naturally
- **session_state for briefings**: Prevents re-triggering expensive LLM API call on every Streamlit rerender
- **carto-positron map style**: No Mapbox token required; renders cleanly in any color scheme
