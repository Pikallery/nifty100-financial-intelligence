# B100 Intelligence — Nifty 100 Financial Intelligence Platform

A full financial intelligence system for India's top 100 publicly listed companies (Nifty 100), covering data engineering, ML analytics, Power BI dashboards, and a Django REST API.

## Project Structure

```
nifty100_project/
├── etl/                        # ETL pipeline scripts
│   ├── 02_clean_and_transform.py   # Data cleaning
│   └── 03_load_to_warehouse.py     # PostgreSQL warehouse loader
├── ml/
│   └── health_scorer.py        # ML financial health scoring engine
├── notebooks/                  # Jupyter analysis notebooks
├── dashboards/                 # Power BI report (.pbix)
├── data/
│   └── clean/                  # Clean CSVs (git-ignored)
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

## Tech Stack

| Component | Technology |
|---|---|
| BI Dashboards | Microsoft Power BI |
| Data Warehouse | PostgreSQL 15 |
| ETL | Python 3.11, pandas, SQLAlchemy |
| ML Analytics | scikit-learn, scipy, statsmodels |
| Web Framework | Django 4.2 + Django REST Framework |
| Background Tasks | Celery + Redis |
| Containerization | Docker + Docker Compose |

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

# 7. Run Django
python manage.py migrate
python manage.py runserver
```

## Dashboards

The Power BI report (`dashboards/B100 Intelligence.pbix`) contains 16 pages across 7 sections:

1. Executive Market Overview
2. Company Deep Dive
3. Sector Comparison
4. Financial Health Scorecard
5. Growth and Valuation Analytics
6. Debt and Leverage Monitor
7. Dividend and Shareholder Returns

Connect Power BI to PostgreSQL:
- Server: `localhost`
- Database: `nifty100_warehouse`
- Username: `postgres`
