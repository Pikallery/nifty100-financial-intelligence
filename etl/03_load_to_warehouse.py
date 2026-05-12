"""
etl/03_load_to_warehouse.py
Creates the star schema in PostgreSQL and loads all clean CSVs.
Safe to run multiple times — uses ON CONFLICT DO UPDATE (upsert).
"""

import os, sys, re
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

CLEAN_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "clean")

DB_URL = (
    f"postgresql://{os.getenv('DB_USER','postgres')}:"
    f"{os.getenv('DB_PASSWORD','Pradyumna123')}@"
    f"{os.getenv('DB_HOST','localhost')}:"
    f"{os.getenv('DB_PORT','5432')}/"
    f"{os.getenv('DB_NAME','nifty100_warehouse')}"
)

# ── DDL ───────────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS dim_sector (
    sector_id   SERIAL PRIMARY KEY,
    sector_name VARCHAR(100) UNIQUE NOT NULL,
    sector_code VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS dim_health_label (
    label_id   SERIAL PRIMARY KEY,
    label_name VARCHAR(20) UNIQUE NOT NULL,
    min_score  NUMERIC(5,2),
    max_score  NUMERIC(5,2),
    color_hex  VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_company (
    symbol          VARCHAR(20) PRIMARY KEY,
    company_name    VARCHAR(200),
    sector_id       INT REFERENCES dim_sector(sector_id),
    company_logo    TEXT,
    website         TEXT,
    nse_profile     TEXT,
    bse_profile     TEXT,
    face_value      NUMERIC(10,2),
    book_value      NUMERIC(12,2),
    roce_percentage NUMERIC(8,2),
    roe_percentage  NUMERIC(8,2),
    about_company   TEXT,
    is_banking      BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_year (
    year_id     SERIAL PRIMARY KEY,
    year_label  VARCHAR(20) UNIQUE NOT NULL,
    fiscal_year INT,
    is_ttm      BOOLEAN DEFAULT FALSE,
    sort_order  INT
);

CREATE TABLE IF NOT EXISTS fact_profit_loss (
    id                    SERIAL PRIMARY KEY,
    symbol                VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    year_id               INT NOT NULL REFERENCES dim_year(year_id),
    sales                 NUMERIC(18,2),
    expenses              NUMERIC(18,2),
    operating_profit      NUMERIC(18,2),
    opm_percentage        NUMERIC(8,2),
    other_income          NUMERIC(18,2),
    interest              NUMERIC(18,2),
    depreciation          NUMERIC(18,2),
    profit_before_tax     NUMERIC(18,2),
    tax_percentage        NUMERIC(8,2),
    net_profit            NUMERIC(18,2),
    eps                   NUMERIC(12,2),
    dividend_payout       NUMERIC(8,2),
    net_profit_margin_pct NUMERIC(8,2),
    expense_ratio_pct     NUMERIC(8,2),
    interest_coverage     NUMERIC(12,2),
    asset_turnover        NUMERIC(12,4),
    return_on_assets_pct  NUMERIC(8,2),
    is_banking            BOOLEAN DEFAULT FALSE,
    UNIQUE (symbol, year_id)
);

CREATE TABLE IF NOT EXISTS fact_balance_sheet (
    id                SERIAL PRIMARY KEY,
    symbol            VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    year_id           INT NOT NULL REFERENCES dim_year(year_id),
    equity_capital    NUMERIC(18,2),
    reserves          NUMERIC(18,2),
    borrowings        NUMERIC(18,2),
    other_liabilities NUMERIC(18,2),
    total_liabilities NUMERIC(18,2),
    fixed_assets      NUMERIC(18,2),
    cwip              NUMERIC(18,2),
    investments       NUMERIC(18,2),
    other_assets      NUMERIC(18,2),
    total_assets      NUMERIC(18,2),
    debt_to_equity    NUMERIC(12,4),
    equity_ratio      NUMERIC(8,4),
    UNIQUE (symbol, year_id)
);

CREATE TABLE IF NOT EXISTS fact_cash_flow (
    id                    SERIAL PRIMARY KEY,
    symbol                VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    year_id               INT NOT NULL REFERENCES dim_year(year_id),
    operating_activity    NUMERIC(18,2),
    investing_activity    NUMERIC(18,2),
    financing_activity    NUMERIC(18,2),
    net_cash_flow         NUMERIC(18,2),
    free_cash_flow        NUMERIC(18,2),
    cash_conversion_ratio NUMERIC(12,4),
    UNIQUE (symbol, year_id)
);

CREATE TABLE IF NOT EXISTS fact_analysis (
    id        SERIAL PRIMARY KEY,
    symbol    VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    period    VARCHAR(10) NOT NULL,
    metric    VARCHAR(60) NOT NULL,
    value_pct NUMERIC(8,2),
    UNIQUE (symbol, period, metric)
);

CREATE TABLE IF NOT EXISTS fact_ml_scores (
    id                  SERIAL PRIMARY KEY,
    symbol              VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    computed_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    overall_score       NUMERIC(5,2),
    profitability_score NUMERIC(5,2),
    growth_score        NUMERIC(5,2),
    leverage_score      NUMERIC(5,2),
    cashflow_score      NUMERIC(5,2),
    dividend_score      NUMERIC(5,2),
    trend_score         NUMERIC(5,2),
    health_label        VARCHAR(20),
    UNIQUE (symbol, computed_at)
);

CREATE TABLE IF NOT EXISTS fact_pros_cons (
    id           SERIAL PRIMARY KEY,
    symbol       VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    is_pro       BOOLEAN NOT NULL,
    text         TEXT NOT NULL,
    source       VARCHAR(10) DEFAULT 'MANUAL',
    generated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS documents (
    id                SERIAL PRIMARY KEY,
    symbol            VARCHAR(20) NOT NULL,
    year              INT,
    annual_report_url TEXT,
    UNIQUE (symbol, year)
);

CREATE TABLE IF NOT EXISTS fact_forecasts (
    id              SERIAL PRIMARY KEY,
    symbol          VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    forecast_year   INT NOT NULL,
    predicted_sales NUMERIC(18,2),
    lower_bound     NUMERIC(18,2),
    upper_bound     NUMERIC(18,2),
    trend_direction VARCHAR(10),
    computed_at     TIMESTAMP DEFAULT NOW(),
    UNIQUE (symbol, forecast_year)
);

CREATE TABLE IF NOT EXISTS fact_peers (
    symbol      VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    peer_symbol VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    similarity  NUMERIC(8,6),
    rank        INT,
    PRIMARY KEY (symbol, peer_symbol)
);

CREATE TABLE IF NOT EXISTS fact_anomalies (
    id         SERIAL PRIMARY KEY,
    symbol     VARCHAR(20) NOT NULL REFERENCES dim_company(symbol),
    year_id    INT REFERENCES dim_year(year_id),
    metric     VARCHAR(60),
    value      NUMERIC(18,2),
    z_score    NUMERIC(8,4),
    method     VARCHAR(20),
    severity   VARCHAR(10),
    reviewed   BOOLEAN DEFAULT FALSE,
    notes      TEXT,
    flagged_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_clusters (
    symbol        VARCHAR(20) PRIMARY KEY REFERENCES dim_company(symbol),
    cluster_id    INT,
    cluster_label VARCHAR(100),
    pca_x         NUMERIC(12,6),
    pca_y         NUMERIC(12,6),
    computed_at   TIMESTAMP DEFAULT NOW()
);
"""

SECTORS = [
    ("IT","IT"),("Banking","BANK"),("NBFC","NBFC"),("Insurance","INS"),
    ("Energy","ENRG"),("Power","PWR"),("Infrastructure","INFRA"),
    ("Conglomerate","CONG"),("Cement","CEM"),("Healthcare","HC"),
    ("Pharma","PHA"),("FMCG","FMCG"),("Auto","AUTO"),("Paint","PAINT"),
    ("Telecom","TEL"),("Hospitality","HOSP"),("Metals","MET"),
    ("Real Estate","RE"),("Diversified","DIV"),
]

HEALTH_LABELS = [
    ("EXCELLENT", 85, 100, "#22c55e"),
    ("GOOD",      70,  84, "#84cc16"),
    ("AVERAGE",   50,  69, "#eab308"),
    ("WEAK",      35,  49, "#f97316"),
    ("POOR",       0,  34, "#ef4444"),
]

BANKING_SYMBOLS = {
    "AXISBANK","BANKBARODA","CANBK","HDFCBANK","ICICIBANK",
    "INDUSINDBK","IDFCFIRSTB","KOTAKBANK","PNB","SBIN",
    "UNIONBANK","BANDHANBNK","FEDERALBNK",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def nan_to_none(df):
    # astype(object) is required so that NaN becomes Python None (not float NaN)
    # when .to_dict() serialises the rows for psycopg2
    return df.astype(object).where(pd.notna(df), other=None)

def upsert(conn, table, df, conflict_cols, update_cols):
    if df.empty:
        return
    cols = list(df.columns)
    sql = (
        f"INSERT INTO {table} ({', '.join(cols)}) "
        f"VALUES ({', '.join(':'+c for c in cols)}) "
        f"ON CONFLICT ({', '.join(conflict_cols)}) DO UPDATE SET "
        f"{', '.join(c+' = EXCLUDED.'+c for c in update_cols)}"
    )
    conn.execute(text(sql), nan_to_none(df).to_dict(orient="records"))

def read_clean(name):
    path = os.path.join(CLEAN_DIR, f"{name}.csv")
    if not os.path.exists(path):
        print(f"  WARN: {path} not found")
        return pd.DataFrame()
    return pd.read_csv(path)

# ── Dimension loaders ─────────────────────────────────────────────────────────

def load_dim_sector(conn):
    for name, code in SECTORS:
        conn.execute(text(
            "INSERT INTO dim_sector (sector_name, sector_code) VALUES (:n,:c) "
            "ON CONFLICT (sector_name) DO NOTHING"
        ), {"n": name, "c": code})
    print(f"  dim_sector: {conn.execute(text('SELECT COUNT(*) FROM dim_sector')).scalar()} rows")

def load_dim_health_label(conn):
    for name, mn, mx, color in HEALTH_LABELS:
        conn.execute(text(
            "INSERT INTO dim_health_label (label_name,min_score,max_score,color_hex) "
            "VALUES (:n,:mn,:mx,:c) ON CONFLICT (label_name) DO NOTHING"
        ), {"n": name, "mn": mn, "mx": mx, "c": color})
    print(f"  dim_health_label: {conn.execute(text('SELECT COUNT(*) FROM dim_health_label')).scalar()} rows")

def load_dim_company(conn):
    df = read_clean("companies")
    if df.empty:
        return
    sectors = pd.read_sql("SELECT sector_id, sector_name FROM dim_sector", conn)
    df = df.merge(sectors, left_on="sector", right_on="sector_name", how="left")
    df["is_banking"] = df["id"].isin(BANKING_SYMBOLS)
    cols = ["id","company_name","sector_id","company_logo","website",
            "nse_profile","bse_profile","face_value","book_value",
            "roce_percentage","roe_percentage","about_company","is_banking"]
    df = df[cols].rename(columns={"id":"symbol"})
    upsert(conn, "dim_company", df, ["symbol"],
           ["company_name","sector_id","company_logo","website","nse_profile",
            "bse_profile","face_value","book_value","roce_percentage",
            "roe_percentage","about_company","is_banking"])
    print(f"  dim_company: {conn.execute(text('SELECT COUNT(*) FROM dim_company')).scalar()} rows")

def load_dim_year(conn):
    years = set()
    for sheet in ["profit_loss","balance_sheet","cash_flow"]:
        df = read_clean(sheet)
        if not df.empty and "year" in df.columns:
            years.update(df["year"].dropna().unique())
    mo = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
          "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    rows = []
    for label in years:
        label = str(label).strip()
        # Only accept 'TTM' or 'Mon YYYY' — skip bare integers like '2015'
        is_ttm = label.upper() == "TTM"
        m = re.match(r"^([A-Za-z]{3})\s+(\d{4})$", label)
        if not is_ttm and not m:
            continue
        fiscal_year = int(m.group(2)) if m else None
        # Use a simple incrementing sort key (YYYY*100 + MM) stored as BIGINT-safe value
        # Max possible: 2099 * 100 + 12 = 209912 — well within INT4 range (2.1B)
        sort_order  = 99999 if is_ttm else int(m.group(2)) * 100 + mo.get(m.group(1), 0)
        rows.append({"year_label": label, "fiscal_year": fiscal_year,
                     "is_ttm": bool(is_ttm), "sort_order": int(sort_order)})
    df = pd.DataFrame(rows).drop_duplicates("year_label")
    upsert(conn, "dim_year", df, ["year_label"], ["fiscal_year","is_ttm","sort_order"])
    print(f"  dim_year: {conn.execute(text('SELECT COUNT(*) FROM dim_year')).scalar()} rows")

# ── Fact loaders ──────────────────────────────────────────────────────────────

def add_year_id(df, conn):
    years = pd.read_sql("SELECT year_id, year_label FROM dim_year", conn)
    df = df.merge(years, left_on="year", right_on="year_label", how="left")
    df = df.dropna(subset=["year_id"])
    df["year_id"] = df["year_id"].astype(int)
    # Drop any symbols not present in dim_company (orphan FK guard)
    known = pd.read_sql("SELECT symbol FROM dim_company", conn)["symbol"].tolist()
    before = len(df)
    df = df[df["company_id"].isin(known)] if "company_id" in df.columns else df[df["symbol"].isin(known)]
    dropped = before - len(df)
    if dropped:
        print(f"    (skipped {dropped} rows with unknown symbol)")
    return df

def load_fact_profit_loss(conn):
    df = read_clean("profit_loss")
    if df.empty:
        return
    df = add_year_id(df, conn)
    df["is_banking"] = df["company_id"].isin(BANKING_SYMBOLS)
    cols = ["company_id","year_id","sales","expenses","operating_profit",
            "opm_percentage","other_income","interest","depreciation",
            "profit_before_tax","tax_percentage","net_profit","eps",
            "dividend_payout","net_profit_margin_pct","expense_ratio_pct",
            "interest_coverage","asset_turnover","return_on_assets_pct","is_banking"]
    df = df[cols].rename(columns={"company_id":"symbol"})
    upsert(conn, "fact_profit_loss", df, ["symbol","year_id"],
           [c for c in cols if c != "company_id"])
    print(f"  fact_profit_loss: {conn.execute(text('SELECT COUNT(*) FROM fact_profit_loss')).scalar()} rows")

def load_fact_balance_sheet(conn):
    df = read_clean("balance_sheet")
    if df.empty:
        return
    df = add_year_id(df, conn)
    cols = ["company_id","year_id","equity_capital","reserves","borrowings",
            "other_liabilities","total_liabilities","fixed_assets","cwip",
            "investments","other_assets","total_assets","debt_to_equity","equity_ratio"]
    df = df[cols].rename(columns={"company_id":"symbol"})
    upsert(conn, "fact_balance_sheet", df, ["symbol","year_id"],
           [c for c in cols if c != "company_id"])
    print(f"  fact_balance_sheet: {conn.execute(text('SELECT COUNT(*) FROM fact_balance_sheet')).scalar()} rows")

def load_fact_cash_flow(conn):
    df = read_clean("cash_flow")
    if df.empty:
        return
    df = add_year_id(df, conn)
    cols = ["company_id","year_id","operating_activity","investing_activity",
            "financing_activity","net_cash_flow","free_cash_flow","cash_conversion_ratio"]
    df = df[cols].rename(columns={"company_id":"symbol"})
    upsert(conn, "fact_cash_flow", df, ["symbol","year_id"],
           [c for c in cols if c != "company_id"])
    print(f"  fact_cash_flow: {conn.execute(text('SELECT COUNT(*) FROM fact_cash_flow')).scalar()} rows")

def load_fact_analysis(conn):
    df = read_clean("analysis")
    if df.empty:
        return
    cols = ["company_id","period","metric","value_pct"]
    df = df[cols].rename(columns={"company_id":"symbol"})
    upsert(conn, "fact_analysis", df, ["symbol","period","metric"], ["value_pct"])
    print(f"  fact_analysis: {conn.execute(text('SELECT COUNT(*) FROM fact_analysis')).scalar()} rows")

def load_fact_pros_cons(conn):
    df = read_clean("pros_cons")
    if df.empty:
        return
    records = []
    for _, row in df.iterrows():
        if pd.notna(row.get("pros")):
            records.append({"symbol":row["company_id"],"is_pro":True,
                            "text":str(row["pros"]),"source":"MANUAL"})
        if pd.notna(row.get("cons")):
            records.append({"symbol":row["company_id"],"is_pro":False,
                            "text":str(row["cons"]),"source":"MANUAL"})
    if records:
        conn.execute(text("DELETE FROM fact_pros_cons WHERE source='MANUAL'"))
        conn.execute(text(
            "INSERT INTO fact_pros_cons (symbol,is_pro,text,source) "
            "VALUES (:symbol,:is_pro,:text,:source)"
        ), records)
    print(f"  fact_pros_cons: {conn.execute(text('SELECT COUNT(*) FROM fact_pros_cons')).scalar()} rows")

def load_documents(conn):
    df = read_clean("documents")
    if df.empty:
        return
    cols = ["company_id","year","annual_report_url"]
    df = df[cols].rename(columns={"company_id":"symbol"})
    upsert(conn, "documents", df, ["symbol","year"], ["annual_report_url"])
    print(f"  documents: {conn.execute(text('SELECT COUNT(*) FROM documents')).scalar()} rows")

# ── Quality checks ────────────────────────────────────────────────────────────

CHECKS = [
    ("Companies loaded",       "SELECT COUNT(*) FROM dim_company",        lambda n: n >= 100),
    ("Years loaded",           "SELECT COUNT(*) FROM dim_year",           lambda n: n >= 20),
    ("P&L rows",               "SELECT COUNT(*) FROM fact_profit_loss",   lambda n: n >= 1200),
    ("Balance Sheet rows",     "SELECT COUNT(*) FROM fact_balance_sheet", lambda n: n >= 1100),
    ("Cash Flow rows",         "SELECT COUNT(*) FROM fact_cash_flow",     lambda n: n >= 1100),
    ("No null symbol in P&L",  "SELECT COUNT(*) FROM fact_profit_loss WHERE symbol IS NULL", lambda n: n == 0),
    ("No null year in BS",     "SELECT COUNT(*) FROM fact_balance_sheet WHERE year_id IS NULL", lambda n: n == 0),
    ("Health labels seeded",   "SELECT COUNT(*) FROM dim_health_label",   lambda n: n == 5),
    ("Banking OPM% all NULL",  "SELECT COUNT(*) FROM fact_profit_loss WHERE is_banking=TRUE AND opm_percentage IS NOT NULL", lambda n: n == 0),
    ("No duplicate P&L",       "SELECT COUNT(*) FROM (SELECT symbol,year_id,COUNT(*) FROM fact_profit_loss GROUP BY symbol,year_id HAVING COUNT(*)>1) x", lambda n: n == 0),
]

def run_checks(conn):
    print("\n" + "="*55)
    print("DATA QUALITY CHECKS")
    print("="*55)
    all_pass = True
    for label, sql, check in CHECKS:
        result = conn.execute(text(sql)).scalar()
        passed = check(result)
        print(f"  [{'PASS' if passed else 'FAIL'}] {label}: {result}")
        if not passed:
            all_pass = False
    print()
    if not all_pass:
        print("  FAILED — fix issues before proceeding.")
        sys.exit(1)
    print("  All checks passed.")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"Connecting to PostgreSQL...")
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        print("Creating schema...")
        conn.execute(text(DDL))

        print("\nLoading dimension tables...")
        load_dim_sector(conn)
        load_dim_health_label(conn)
        load_dim_company(conn)
        load_dim_year(conn)

        print("\nLoading fact tables...")
        load_fact_profit_loss(conn)
        load_fact_balance_sheet(conn)
        load_fact_cash_flow(conn)
        load_fact_analysis(conn)
        load_fact_pros_cons(conn)
        load_documents(conn)

        run_checks(conn)

    print("\nWarehouse load complete.")

if __name__ == "__main__":
    main()
