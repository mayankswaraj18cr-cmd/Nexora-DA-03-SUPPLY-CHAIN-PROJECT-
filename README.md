# DA-03 Retail and Supply Chain Control Tower

A monitoring system that watches inventory, shipments and suppliers, computes service KPIs, detects anomalies, forecasts demand, and raises alerts **before** a stock-out costs sales. It ships as a Python pipeline, an Airflow DAG, a PostgreSQL schema, a Docker Compose stack, and a self-contained HTML dashboard with a supplier-delay what-if tool.

Nexvia Technologies capstone, Data Analytics track. Author: Mayank Swaraj.

> **Data note:** all data is simulated (hourly feeds, 45 days from 2026-09-01 to 2026-10-15, 12 SKUs, 6 suppliers). Two incidents are injected on purpose so detection can be verified: a **demand spike on SKU03** (last 72 hours) and a **delivery-delay incident at supplier S4, Delta Traders** (orders placed in the last 480 hours).

## Contents
1. [Results at a glance](#results-at-a-glance)
2. [Quick start (offline)](#quick-start-offline)
3. [Dashboard tour](#dashboard-tour)
4. [Architecture](#architecture)
5. [How it works](#how-it-works)
6. [Full stack: Postgres, Airflow, Superset](#full-stack-postgres-airflow-superset)
7. [Configuration](#configuration)
8. [Project structure](#project-structure)
9. [Data dictionary](#data-dictionary)
10. [Testing](#testing)
11. [Verification status](#verification-status)
12. [Limitations](#limitations)
13. [Troubleshooting](#troubleshooting)
14. [Roadmap](#roadmap)
15. [Requirement map](#requirement-map)

## Results at a glance
Output of `python pipeline.py` with the default seed:

| Metric | Value |
|---|---|
| Stock-out rate, last 7 days | 7.79% (whole period 1.45%) |
| On-time delivery, all delivered shipments | 82.4% (orders in last 20 days: 85.2%) |
| Average supplier lead time | 4.14 days |
| Average days of inventory | 5.3 days |
| Mean forecast MAPE (7-day backtest) | 11.3% |
| SKU status | 2 Critical, 3 Watch, 7 OK |
| Anomalies flagged | 5 demand-spike days, 4 supplier-delay shipments |
| Alerts raised | 6 |

Both injected incidents are detected: SKU03's demand spike, and S4's late deliveries (S4 on-time rate 60%, average lead 5.12 days against 4 promised). The two Critical SKUs, SKU04 and SKU10, both belong to S4.

## Quick start (offline)
No Docker or database needed.

```bash
pip install -r requirements.txt
python pipeline.py            # writes data/*.csv, data/alerts.json, docs/metrics.json, dashboard.html
python -m pytest tests        # 8 tests
open dashboard.html           # or double-click; works offline in any browser
```

Requirements: Python 3.10+. `prophet` installs its own Stan backend (a few minutes the first time). `matplotlib` and `reportlab` are only needed to rebuild the paper (`python build_paper.py`); `psycopg2-binary` is only needed for PostgreSQL loading.

## Dashboard tour
`dashboard.html` is one file with the data embedded, so it needs no server or internet. It follows your system light/dark mode.

| Tab | What it shows |
|---|---|
| Overview | KPI cards (stock-out 7d, on-time, recent on-time, lead time, days of inventory, open alerts), daily stock-out chart, daily demand chart |
| Inventory | Per-SKU stock, forecast per day, cover days, days of inventory, replenishment wait, in-transit flag and status (Critical / Watch / OK) |
| Suppliers | Promised vs actual lead time chart, on-time % and delivered count per supplier |
| Forecast | Pick a SKU: history, 14-day Prophet forecast, interval, and hold-out MAPE |
| Anomalies | Demand-spike days with z-scores; delayed shipments with the detection method that fired |
| What-if | Choose a supplier and an extra delay of 0 to 14 days; see which SKUs turn Critical and the estimated units at risk |
| Alerts | Alert list with severity and the delivery channel used |

## Architecture
```
 simulate() hourly feeds ──> data/*.csv ──> load_to_postgres.py ──> PostgreSQL ──> Superset
        (inventory, shipments, SKU master)         views: v_stockout_daily, v_supplier_perf
        │
 Airflow DAG control_tower_hourly (@hourly)
   generate_feed ─> load_postgres ─> analyze_alert_publish
                                       ├─ kpis()          stock-out, on-time, lead time, days of inventory
                                       ├─ anomalies()     rolling z-score, z-score + Isolation Forest
                                       ├─ forecast()      Prophet 14 days, cover vs replenishment wait
                                       ├─ build_alerts() / dispatch()  -> Slack / email / log
                                       └─ dashboard.html  (self-contained, with what-if)
```
More detail in `docs/architecture.md`.

## How it works

### 1. Simulation (`simulate()`)
- 12 SKUs, each with a base daily demand drawn from 40 to 199 units, assigned round-robin to 6 suppliers with nominal lead times of 3, 5, 4, 4, 2 and 6 days.
- Hourly demand is Poisson with mean `base/24 × hourly profile × 1.25 on weekends`, where the profile is low overnight and peaks in the day.
- Each SKU follows a reorder-point policy: order eight days of demand when stock reaches (lead time + 2) days of demand and no order is open. Realised lead time is the nominal value plus Gaussian noise (sd 0.5 day).
- Incidents: SKU03 demand is multiplied by 2.5 in the last 72 hours; S4 orders in the last 480 hours take 5 extra days.
- Everything is seeded, so runs are reproducible.

### 2. KPIs (`kpis()`)
| KPI | Definition |
|---|---|
| Stock-out rate | Share of SKU-hours with zero stock (7-day and whole period) |
| On-time delivery | Delivered shipments with delay of at most 0.5 day versus promised lead time (overall and orders in last 20 days) |
| Supplier lead time | Mean realised lead time per supplier versus promised |
| Days of inventory | Current stock divided by mean daily demand over the last 14 days |

### 3. Anomaly detection (`anomalies()`)
- **Demand spikes:** rolling z-score of each day against the previous 14 days; flagged above 3.
- **Supplier delays:** z-score of delay days across delivered shipments, flagged above 2.5; plus an Isolation Forest (contamination 0.08) on delay and order size, flagging shipments with delay above 1 day. The method that fired is recorded.

### 4. Forecast and risk status (`forecast()`)
- One Prophet model per SKU (weekly seasonality on; daily and yearly off), 14-day horizon.
- Backtest: fit on the first 38 days, predict the last 7, report MAPE (denominator floored at 1 unit).
- **Cover days:** days until cumulative forecast demand exhausts current stock.
- **Replenishment wait:** ETA of the open order if one is in transit, otherwise the supplier's lead time.
- **Status:** `Critical` if cover < wait; `Watch` if cover < wait + 2; otherwise `OK`.

### 5. What-if (`whatif()` and the dashboard)
Adds the chosen delay to the wait of every SKU of one supplier, recomputes status, and estimates `units at risk = max(0, wait - cover) × forecast per day`. The same logic exists in Python (unit-tested) and in the dashboard's JavaScript.

### 6. Alerts (`build_alerts()`, `dispatch()`)
| Rule | Trigger | Severity |
|---|---|---|
| `stockout_rate` | 7-day stock-out rate above 2% | warning |
| `on_time_delivery` | on-time rate for recent orders below 85% | warning |
| `stock_cover` | any SKU with status Critical | critical |
| `supplier_delay` | any supplier with abnormal delay flags | warning |
| `demand_spike` | spike in the last 3 days (up to 3 listed) | info |

Delivery: Slack webhook and/or SMTP email when configured; otherwise a dry run that writes `data/alerts.log`.

## Full stack: Postgres, Airflow, Superset
```bash
docker compose up -d
```
Starts PostgreSQL 15 (`tower/tower`, database `tower`, port 5432), Airflow 2.9.3 in standalone mode (port 8080) with this project mounted at `/opt/airflow/project`, and Superset 3.1.0 (port 8088).

1. **Airflow:** open http://localhost:8080 and enable the DAG `control_tower_hourly`. Standalone mode prints the admin credentials in the container logs (`docker compose logs airflow`). The DAG runs `generate_feed`, `load_postgres`, then `analyze_alert_publish` every hour.
2. **Load manually (optional):** `DATABASE_URL=postgresql://tower:tower@localhost:5432/tower python load_to_postgres.py` applies `schema.sql` and bulk-loads the CSVs with `COPY`.
3. **Superset:** follow `superset/superset_setup.md` (create admin, connect to `postgresql://tower:tower@postgres:5432/tower`, add the views and tables as datasets, build the charts and the supplier filter for the what-if panel).

> The simulator regenerates the same seeded 45-day window on every run, so the hourly DAG refreshes identical data. To use real data, replace `generate_feed` with an extraction from your ERP or warehouse system and keep the downstream tasks unchanged.

## Configuration
Environment variables (all optional):

| Variable | Purpose |
|---|---|
| `SLACK_WEBHOOK_URL` | Post alerts to Slack |
| `SMTP_HOST`, `SMTP_PORT` (587), `SMTP_USER`, `SMTP_PASS` | SMTP server for email alerts |
| `ALERT_TO`, `ALERT_FROM` | Email recipient and sender (email is sent only if `SMTP_HOST` and `ALERT_TO` are set) |
| `DATABASE_URL` | PostgreSQL connection for `load_to_postgres.py` (default `postgresql://tower:tower@localhost:5432/tower`) |

Code-level settings in `pipeline.py`:

| Setting | Where | Default |
|---|---|---|
| Alert and anomaly thresholds | `TH` dict | stock-out 2%, on-time 85%, demand z 3, delay z 2.5 |
| Suppliers and lead times | `SUP` dict | 6 suppliers, 2 to 6 days |
| Simulation window | `START`, `DAYS` | 2026-09-01, 45 days |
| Forecast horizon | `HORIZON` | 14 days |
| Random seed | `simulate(seed=11)` | 11 |

## Project structure
```
DA-03/
├── pipeline.py                  simulator, KPIs, anomalies, forecast, alerts, dashboard build
├── dashboard_template.html      UI template (data injected at build)
├── dashboard.html               generated, self-contained dashboard
├── dags/control_tower_dag.py    hourly Airflow DAG
├── load_to_postgres.py          CSV -> PostgreSQL loader
├── schema.sql                   tables, index, Superset views
├── docker-compose.yml           Postgres + Airflow + Superset
├── superset/superset_setup.md   Superset setup and what-if filter
├── tests/test_pipeline.py       8 pytest tests
├── data/                        generated CSVs, alerts.json, alerts.log
├── docs/                        architecture, metrics, test results, presentation outline, reflection
├── build_paper.py, paperkit.py  rebuild the research paper PDF
├── DA-03_Research_Paper_and_Source.pdf
└── requirements.txt
```

## Data dictionary
| Table | Columns |
|---|---|
| `sku_master` | sku, name, supplier, base_daily |
| `inventory_hourly` | ts, sku, stock (after demand), demand, unmet (lost demand) |
| `shipments` | shipment_id, sku, supplier, order_ts, promised_days, actual_days, qty, delivered (0 = in transit), eta_days, delay_days, on_time |
| `sku_status` | sku, name, supplier, stock, fc_daily, cover_days, lead_days, wait_days, in_transit, mape, status |

SQL views: `v_stockout_daily` (daily stock-out % and demand) and `v_supplier_perf` (delivered count, average lead time, on-time % per supplier).

## Testing
```bash
python -m pytest tests -v
```
| Test | Checks |
|---|---|
| determinism and shape | 12 × 45 × 24 rows, identical output for the same seed |
| stock integrity | stock never negative; unmet demand never above demand |
| on-time definition | `on_time` equals delay of at most 0.5 day |
| KPI ranges | percentages within 0 to 100, lead time positive |
| injected demand spike | SKU03 is flagged |
| injected supplier delay | S4 is flagged |
| status and what-if rules | hand-computed cases |
| forecast and alerts | mean MAPE below 25%, 14-day horizon, a stock-cover alert is raised |

Result: 8 passed. The dashboard JavaScript was also executed against a stubbed browser environment to catch runtime errors.

## Verification status
| Component | Status |
|---|---|
| Pipeline, KPIs, anomalies, Prophet forecast, alerts (dry run) | Run and tested |
| Dashboard | JavaScript smoke-tested; open it in a browser to confirm the visuals |
| Research paper PDF | Built and visually checked |
| `docker-compose.yml`, Airflow DAG execution, `load_to_postgres.py`, Superset setup | **Written but not executed** (DAG and loader syntax-checked only) |
| Live Slack and SMTP delivery | **Not executed** (dry-run path tested) |

Please run the full stack once and record the outcome before relying on it.

## Limitations
- **Simulated data.** Detection results show the logic works, not how it performs on real noise.
- **Short Prophet history.** With 45 days there is no yearly seasonality and the model reacts strongly to spikes; SKU03's forecast is inflated by the injected spike and has the worst backtest error (28.6%).
- **Illustrative thresholds.** The z-score limits, 0.5-day on-time tolerance, 2-day Watch margin and alert limits need tuning against business cost.
- **Simplifications.** One warehouse, no SKU substitution, no capacity limits, no promotions, and lost sales are not backordered.
- **Z-score false positives.** Some spike flags are natural variation at the chosen threshold.

## Troubleshooting
| Problem | Fix |
|---|---|
| `pip install prophet` is slow or fails | Use Python 3.10 to 3.12 and upgrade pip; Prophet bundles its Stan backend |
| Forecast logs are noisy | Expected cmdstanpy output; it is silenced at ERROR level in the code but may still print |
| `docker compose` Airflow cannot import `prophet` | First start installs packages via `_PIP_ADDITIONAL_REQUIREMENTS`; wait for the container to finish installing |
| Superset cannot connect to Postgres | The stock Superset image may lack the PostgreSQL driver; install `psycopg2-binary` in the container (unverified) |
| No Slack or email received | Variables are not set, so alerts were written to `data/alerts.log` (dry run) |

## Roadmap
Connect real warehouse and ERP feeds; add promotions and holidays as Prophet regressors; remove flagged anomalies before fitting; schedule model retraining separately from the hourly refresh; add safety-stock optimisation and supplier scorecards; validate the container stack end to end.

## Requirement map
| Project requirement | Where |
|---|---|
| Data in PostgreSQL (Kaggle or simulated hourly feeds) | `simulate()`, `schema.sql`, `load_to_postgres.py` |
| Airflow hourly refresh | `dags/control_tower_dag.py` (`@hourly`) |
| KPIs: stock-out rate, on-time delivery, supplier lead time, days of inventory | `kpis()` |
| Anomaly detection (z-score or Isolation Forest) | `anomalies()` |
| Prophet forecast compared with current stock | `forecast()` |
| Superset dashboards with what-if panel | `superset/superset_setup.md`, `schema.sql` views, What-if tab in `dashboard.html` |
| Slack or email alerts on threshold breach | `build_alerts()`, `dispatch()` |
| Research and documentation | `DA-03_Research_Paper_and_Source.pdf`, `docs/` |
