# B100 Intelligence — Nifty 100 Financial Intelligence Platform

A full financial intelligence system for India's top 100 publicly listed companies (Nifty 100), covering data engineering, ML analytics, Power BI dashboards, and a Django REST API.

## Live Links

| | Link |
|---|---|
| 🌐 **Web App** | [nifty100-financial-intelligence.vercel.app](https://nifty100-financial-intelligence.vercel.app) |
| 📊 **Power BI Dashboard** | [View on Power BI Service](https://app.powerbi.com/groups/me/dashboards/c734a1ae-1d3e-47a9-838c-51eed14a88ae?ctid=5cd8bc41-efe1-4078-87ce-6b6701eb6973&pbi_source=linkShare) |
| 📖 **API Docs** | [nifty100-financial-intelligence.vercel.app/api/docs/](https://nifty100-financial-intelligence.vercel.app/api/docs/) |

---

## Dashboard Screenshots

### Section 1 — Executive Overview

| Page 1: Market Snapshot | Page 2: Sector Performance | Page 3: YoY Growth Tracker |
|---|---|---|
| ![Market Snapshot](screenshots/Sec1_Page1_Market_Snapshot.png) | ![Sector Performance](screenshots/Sec1_Page2_Sector_Performance.png) | ![YoY Growth Tracker](screenshots/Sec1_Page3_YoY_Growth_Tracker.png) |

### Section 2 — Company Deep Dive

| Page 4: Financial Summary | Page 5: Balance Sheet Health |
|---|---|
| ![Financial Summary](screenshots/Sec2_Page4_Financial_Summary.png) | ![Balance Sheet Health](screenshots/Sec2_Page5_Balance_Sheet_Health.png) |

| Page 6: Cash Flow Analysis | Page 7: Growth and Returns |
|---|---|
| ![Cash Flow Analysis](screenshots/Sec2_Page6_Cash_Flow_Analysis.png) | ![Growth and Returns](screenshots/Sec2_Page7_Growth_and_Returns.png) |

### Section 3 — Sector Comparison

| Page 8: Sector vs Sector | Page 9: Companies Within Sector | Page 10: Sector Trends |
|---|---|---|
| ![Sector vs Sector](screenshots/Sec3_Page8_Sector_vs_Sector.png) | ![Companies Within Sector](screenshots/Sec3_Page9_Companies_Within_Sector.png) | ![Sector Trends](screenshots/Sec3_Page10_Sector_Trends.png) |

### Section 4 — Health Scorecard

| Page 11: Health Leaderboard | Page 12: Company Score Breakdown |
|---|---|
| ![Health Leaderboard](screenshots/Sec4_Page11_Health_Leaderboard.png) | ![Company Score Breakdown](screenshots/Sec4_Page12_Company_Score_Breakdown.png) |

### Section 5 — Growth and Valuation

| Page 13: Growth Analytics |
|---|
| ![Growth Analytics](screenshots/Sec5_Page13_Growth_Analytics.png) |

### Section 6 — Debt and Leverage

| Page 14: Leverage Monitor |
|---|
| ![Leverage Monitor](screenshots/Sec6_Page14_Leverage_Monitor.png) |

### Section 7 — Dividends and Shareholder Returns

| Page 15: Shareholder Returns |
|---|
| ![Shareholder Returns](screenshots/Sec7_Page15_Shareholder_Returns.png) |

---

## Project Structure

```
nifty100_project/
├── etl/                            # ETL pipeline scripts
│   ├── 02_clean_and_transform.py   # Data cleaning (9 issues fixed)
│   └── 03_load_to_warehouse.py     # PostgreSQL star schema loader
├── ml/
│   └── health_scorer.py            # ML financial health scoring engine
├── dashboards/
│   └── B100 Intelligence.pbix      # Power BI report (15 pages, 7 sections)
├── screenshots/                    # Dashboard page screenshots
├── docker-compose.yml              # PostgreSQL + Redis setup
├── requirements.txt                # Python dependencies
├── Procfile                        # Railway deployment config
└── .env.example                    # Environment variables template
```

---

## Tech Stack

| Component | Technology |
|---|---|
| BI Dashboards | Microsoft Power BI (15 pages, 7 sections) |
| Data Warehouse | PostgreSQL 18 (star schema) |
| ETL | Python 3.11, pandas, SQLAlchemy |
| ML Analytics | scikit-learn, scipy, statsmodels |
| Web Framework | Django 4.2 + Django REST Framework |
| Background Tasks | Celery + Redis |
| Containerization | Docker + Docker Compose |

---

## Data Coverage

- **101 companies** across 19 sectors
- **12+ years** of financial history (2012–2024)
- **7 data tables**: Companies, P&L, Balance Sheet, Cash Flow, Analysis, Documents, Pros/Cons
- **ML Health Scores**: 0–100 score across 6 dimensions for every company

---

## Setup

```bash
# 1. Clone the repo
git clone https://github.com/Pikallery/nifty100-financial-intelligence.git
cd nifty100-financial-intelligence

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your PostgreSQL credentials

# 4. Start database
docker-compose up db redis -d

# 5. Run ETL pipeline
python etl/02_clean_and_transform.py
python etl/03_load_to_warehouse.py

# 6. Run ML scoring
python ml/health_scorer.py
```

---

## Power BI Connection

Connect Power BI Desktop to PostgreSQL:
- **Server**: `localhost`
- **Database**: `nifty100_warehouse`
- **Username**: `postgres`

Import all `dim_*` and `fact_*` tables. Set all relationships as Many-to-One from fact tables to dimension tables.
