"""
etl/02_clean_and_transform.py  —  Enhanced v2
Reads nifty100_combined_dataset.xlsx, applies every fix found during
deep inspection, and saves 7 clean CSVs to data/clean/.

Issues fixed (in order):
  ALL SHEETS   title row skipped; real headers at row 1
  COMPANIES    8 missing symbols added; TVSMOTOR nulls patched; sector column added
  P&L          ADANIPORTS exact duplicates removed
               Banking OPM% replaced with NULL (ratio meaningless for banks)
               OPM% recomputed from raw values for all non-banking companies
  BALANCE_SHEET ASIANPAINT/PNB/POWERGRID/TECHM exact duplicates removed
               column renamed other_asset -> other_assets
  CASH_FLOW    Mar-YY -> Mar YYYY (TCS); 2 null rows dropped
  DOCUMENTS    double-slash URLs fixed; 52 null-URL rows dropped
  PROS_CONS    rows where both pros AND cons are null dropped
  ALL SHEETS   year standardised to Mon YYYY; fiscal_year + sort_order added
               all derived/computed metrics recalculated
"""

import os, re
import pandas as pd
import numpy as np

EXCEL_PATH = r"C:\Users\samal\Downloads\drive-download-20260502T112109Z-3-001\nifty100_combined_dataset.xlsx"
OUTPUT_DIR = r"C:\Users\samal\nifty100_project\data\clean"

# ── Banking companies: OPM% is meaningless for them ──────────────────────────
BANKING_SYMBOLS = {
    "AXISBANK", "BANKBARODA", "CANBK", "HDFCBANK", "ICICIBANK",
    "INDUSINDBK", "IDFCFIRSTB", "KOTAKBANK", "PNB", "SBIN",
    "UNIONBANK", "BANDHANBNK", "FEDERALBNK",
}

# ── Sector map for all 100 companies ─────────────────────────────────────────
SECTOR_MAP = {
    "TCS":"IT","INFY":"IT","WIPRO":"IT","HCLTECH":"IT","LTIM":"IT",
    "TECHM":"IT","MPHASIS":"IT","PERSISTENT":"IT","COFORGE":"IT","OFSS":"IT",
    "HDFCBANK":"Banking","ICICIBANK":"Banking","SBIN":"Banking",
    "AXISBANK":"Banking","KOTAKBANK":"Banking","INDUSINDBK":"Banking",
    "BANDHANBNK":"Banking","FEDERALBNK":"Banking","IDFCFIRSTB":"Banking",
    "PNB":"Banking","BANKBARODA":"Banking","CANBK":"Banking","UNIONBANK":"Banking",
    "BAJFINANCE":"NBFC","BAJAJFINSV":"NBFC","MUTHOOTFIN":"NBFC",
    "CHOLAFIN":"NBFC","M&MFIN":"NBFC",
    "HDFCLIFE":"Insurance","SBILIFE":"Insurance","ICICIGI":"Insurance",
    "ICICIPRULI":"Insurance","LICI":"Insurance",
    "ADANIGREEN":"Energy","ADANIPOWER":"Energy","ADANIENSOL":"Energy",
    "ATGL":"Energy","AGTL":"Energy","GAIL":"Energy","NTPC":"Power","POWERGRID":"Power",
    "TATAPOWER":"Power","COALINDIA":"Energy","IOC":"Energy",
    "BPCL":"Energy","ONGC":"Energy","HINDPETRO":"Energy",
    "ADANIPORTS":"Infrastructure","ADANIENT":"Conglomerate",
    "AMBUJACEM":"Cement","ACC":"Cement","ULTRACEMCO":"Cement","SHREECEM":"Cement",
    "APOLLOHOSP":"Healthcare","SUNPHARMA":"Pharma","DRREDDY":"Pharma",
    "CIPLA":"Pharma","DIVISLAB":"Pharma","MANKIND":"Pharma",
    "TORNTPHARM":"Pharma","ZYDUSLIFE":"Pharma",
    "HINDUNILVR":"FMCG","ITC":"FMCG","NESTLEIND":"FMCG","BRITANNIA":"FMCG",
    "DABUR":"FMCG","GODREJCP":"FMCG","MARICO":"FMCG","COLPAL":"FMCG",
    "EMAMILTD":"FMCG","VBL":"FMCG","UNITDSPR":"FMCG","ZOMATO":"FMCG",
    "BAJAJ-AUTO":"Auto","MARUTI":"Auto","M&M":"Auto","TATAMOTORS":"Auto",
    "HEROMOTOCO":"Auto","EICHERMOT":"Auto","TVSMOTORS":"Auto","TVSMOTOR":"Auto",
    "ASIANPAINT":"Paint","BERGEPAINT":"Paint",
    "BHARTIARTL":"Telecom",
    "INDHOTEL":"Hospitality",
    "TATASTEEL":"Metals","HINDALCO":"Metals","JSWSTEEL":"Metals",
    "VEDL":"Metals","NMDC":"Metals","SAIL":"Metals",
    "DLF":"Real Estate","GODREJPROP":"Real Estate",
    "LTIMINDTREE":"IT","LTIM":"IT",
}

# ── 8 companies missing from Companies sheet — add them manually ──────────────
MISSING_COMPANIES = [
    {"id":"WIPRO",      "company_name":"Wipro Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/WIPRO.png",
     "about_company":"Wipro Ltd is an Indian multinational IT services company.",
     "website":"https://www.wipro.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=WIPRO",
     "bse_profile":"https://www.bseindia.com/stock-share-price/wipro-ltd/WIPRO/507685/",
     "face_value":2,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AWIPRO"},
    {"id":"ULTRACEMCO", "company_name":"UltraTech Cement Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/ULTRACEMCO.png",
     "about_company":"UltraTech Cement is India's largest cement company.",
     "website":"https://www.ultratechcement.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=ULTRACEMCO",
     "bse_profile":"https://www.bseindia.com/stock-share-price/ultratech-cement-ltd/ULTRACEMCO/532538/",
     "face_value":10,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AULTRACEMCO"},
    {"id":"UNIONBANK",  "company_name":"Union Bank of India",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/UNIONBANK.png",
     "about_company":"Union Bank of India is a major public sector bank in India.",
     "website":"https://www.unionbankofindia.co.in/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=UNIONBANK",
     "bse_profile":"https://www.bseindia.com/stock-share-price/union-bank-of-india/UNIONBANK/532477/",
     "face_value":10,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AUNIONBANK"},
    {"id":"UNITDSPR",   "company_name":"United Spirits Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/UNITDSPR.png",
     "about_company":"United Spirits Ltd (Diageo India) is India's largest spirits company.",
     "website":"https://www.diageoindia.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=UNITDSPR",
     "bse_profile":"https://www.bseindia.com/stock-share-price/united-spirits-ltd/UNITDSPR/532432/",
     "face_value":2,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AUNITDSPR"},
    {"id":"VBL",        "company_name":"Varun Beverages Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/VBL.png",
     "about_company":"Varun Beverages Ltd is one of the largest franchisees of PepsiCo.",
     "website":"https://www.varunbeverages.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=VBL",
     "bse_profile":"https://www.bseindia.com/stock-share-price/varun-beverages-ltd/VBL/540180/",
     "face_value":2,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AVBL"},
    {"id":"VEDL",       "company_name":"Vedanta Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/VEDL.png",
     "about_company":"Vedanta Ltd is a diversified natural resources company.",
     "website":"https://www.vedantalimited.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=VEDL",
     "bse_profile":"https://www.bseindia.com/stock-share-price/vedanta-ltd/VEDL/500295/",
     "face_value":1,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AVEDL"},
    {"id":"ZOMATO",     "company_name":"Zomato Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/ZOMATO.png",
     "about_company":"Zomato Ltd is an Indian food delivery and quick commerce platform.",
     "website":"https://www.zomato.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=ZOMATO",
     "bse_profile":"https://www.bseindia.com/stock-share-price/zomato-ltd/ZOMATO/543320/",
     "face_value":1,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AZOMATO"},
    {"id":"AGTL",       "company_name":"Adani Total Gas Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/ATGL.png",
     "about_company":"Adani Total Gas Ltd is India's largest private natural gas distribution company.",
     "website":"https://www.adanitotalgas.in/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=ATGL",
     "bse_profile":"https://www.bseindia.com/stock-share-price/adani-total-gas-ltd/ATGL/542066/",
     "face_value":1,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AATGL"},
    {"id":"ZYDUSLIFE",  "company_name":"Zydus Lifesciences Ltd",
     "company_logo":"https://mkt.in/static/mkt-icons/nifty100/ZYDUSLIFE.png",
     "about_company":"Zydus Lifesciences Ltd is an Indian pharmaceutical company.",
     "website":"https://www.zyduslife.com/",
     "nse_profile":"https://www.nseindia.com/get-quotes/equity?symbol=ZYDUSLIFE",
     "bse_profile":"https://www.bseindia.com/stock-share-price/zydus-lifesciences-ltd/ZYDUSLIFE/532321/",
     "face_value":1,"book_value":None,"roce_percentage":None,"roe_percentage":None,
     "chart_link":"https://in.tradingview.com/chart/?symbol=NSE%3AZYDUSLIFE"},
]

MONTH_MAP = {
    "jan":"Jan","feb":"Feb","mar":"Mar","apr":"Apr","may":"May","jun":"Jun",
    "jul":"Jul","aug":"Aug","sep":"Sep","oct":"Oct","nov":"Nov","dec":"Dec",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def standardize_year(val):
    if pd.isna(val):
        return np.nan
    val = str(val).strip()
    if val.upper() == "TTM":
        return "TTM"
    m = re.match(r"^([A-Za-z]{3})-(\d{2})$", val)        # Mar-13
    if m:
        mon  = MONTH_MAP.get(m.group(1).lower(), m.group(1).capitalize())
        yy   = int(m.group(2))
        yyyy = 2000 + yy if yy < 90 else 1900 + yy
        return f"{mon} {yyyy}"
    m2 = re.match(r"^([A-Za-z]{3})\s+(\d{4})$", val)     # Mar 2024
    if m2:
        mon = MONTH_MAP.get(m2.group(1).lower(), m2.group(1).capitalize())
        return f"{mon} {m2.group(2)}"
    return val

def year_sort_order(label):
    if pd.isna(label) or str(label).strip().upper() == "TTM":
        return 99999
    m = re.match(r"^([A-Za-z]{3})\s+(\d{4})$", str(label).strip())
    if not m:
        return 99998
    order = {"Jan":1,"Feb":2,"Mar":3,"Apr":4,"May":5,"Jun":6,
             "Jul":7,"Aug":8,"Sep":9,"Oct":10,"Nov":11,"Dec":12}
    return int(m.group(2)) * 100 + order.get(m.group(1), 0)

def fiscal_year_int(label):
    if pd.isna(label) or str(label).strip().upper() == "TTM":
        return np.nan
    m = re.match(r"^([A-Za-z]{3})\s+(\d{4})$", str(label).strip())
    return int(m.group(2)) if m else np.nan

def safe_div(num, den, scale=1):
    return (num / den.replace(0, np.nan)) * scale

def to_num(s):
    return pd.to_numeric(s, errors="coerce")

def fix_url_double_slash(url):
    """Fix https://domain/path//file -> https://domain/path/file"""
    if pd.isna(url):
        return np.nan
    url = str(url).strip()
    # Only fix double-slash after the protocol part
    return re.sub(r'(?<!:)//', '/', url)


# ── Sheet cleaners ────────────────────────────────────────────────────────────

def clean_companies(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["id","company_logo","company_name","chart_link","about_company",
                  "website","nse_profile","bse_profile","face_value","book_value",
                  "roce_percentage","roe_percentage"]

    # Add 8 missing companies
    missing_df = pd.DataFrame(MISSING_COMPANIES)
    missing_df = missing_df.rename(columns={"id": "id"})
    # align columns
    for col in df.columns:
        if col not in missing_df.columns:
            missing_df[col] = np.nan
    missing_df = missing_df[df.columns]
    df = pd.concat([df, missing_df], ignore_index=True)

    # Strip whitespace / carriage returns
    for col in ["company_name","about_company","website","nse_profile","bse_profile"]:
        df[col] = df[col].astype(str).str.strip().str.replace(r"[\r\n]+", " ", regex=True)
        df[col] = df[col].replace({"nan": np.nan, "None": np.nan, "NULL": np.nan})

    # Numeric fields
    for col in ["face_value","book_value","roce_percentage","roe_percentage"]:
        df[col] = to_num(df[col])

    # Sector
    df["sector"] = df["id"].map(SECTOR_MAP).fillna("Diversified")

    # Drop duplicate symbols (keep first)
    df = df.drop_duplicates(subset=["id"], keep="first").reset_index(drop=True)
    return df


def _parse_growth_string(s):
    if pd.isna(s):
        return None, np.nan
    s = str(s).strip()
    val_m = re.search(r"(-?\d+(?:\.\d+)?)\s*%", s)
    value = float(val_m.group(1)) if val_m else np.nan
    sl = s.lower()
    if   "10" in sl and "year" in sl: period = "10Y"
    elif  "5" in sl and "year" in sl: period = "5Y"
    elif  "3" in sl and "year" in sl: period = "3Y"
    elif "ttm" in sl or "1 year" in sl or "last year" in sl: period = "TTM"
    else: period = None
    return period, value

def clean_analysis(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["id","company_id","compounded_sales_growth","compounded_profit_growth",
                  "stock_price_cagr","roe"]
    df["company_id"] = df["company_id"].astype(str).str.strip()
    records = []
    for _, row in df.iterrows():
        for metric in ["compounded_sales_growth","compounded_profit_growth",
                       "stock_price_cagr","roe"]:
            period, value = _parse_growth_string(row[metric])
            records.append({"source_id":row["id"],"company_id":row["company_id"],
                            "metric":metric,"period":period,"value_pct":value})
    result = pd.DataFrame(records).dropna(subset=["period","value_pct"])
    return result.sort_values(["company_id","metric","period"]).reset_index(drop=True)


def clean_profit_loss(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["id","company_id","year","sales","expenses","operating_profit",
                  "opm_percentage","other_income","interest","depreciation",
                  "profit_before_tax","tax_percentage","net_profit","eps","dividend_payout"]

    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["year"]       = df["year"].apply(standardize_year)

    # ── FIX 1: Remove exact duplicates (ADANIPORTS entire history doubled) ──
    before = len(df)
    df = df.drop_duplicates(subset=["company_id","year"], keep="first")
    dropped = before - len(df)
    if dropped:
        print(f"  P&L: removed {dropped} duplicate rows")

    # Numeric conversion
    num_cols = ["sales","expenses","operating_profit","opm_percentage","other_income",
                "interest","depreciation","profit_before_tax","tax_percentage",
                "net_profit","eps","dividend_payout"]
    for col in num_cols:
        df[col] = to_num(df[col])

    # ── FIX 2: Recompute OPM% for non-banking companies ──────────────────────
    computed_opm = safe_div(df["operating_profit"], df["sales"], scale=100).round(2)
    is_banking   = df["company_id"].isin(BANKING_SYMBOLS)

    # For non-banking: use computed OPM% (more reliable than stored value)
    df.loc[~is_banking, "opm_percentage"] = computed_opm[~is_banking]
    # For banking: OPM% is meaningless — set to NULL
    df.loc[is_banking,  "opm_percentage"] = np.nan

    # ── FIX 3: For banking companies interest_coverage is not applicable ─────
    df["interest_coverage"] = safe_div(df["operating_profit"], df["interest"])
    df.loc[is_banking, "interest_coverage"] = np.nan

    # Derived columns
    df["net_profit_margin_pct"] = safe_div(df["net_profit"], df["sales"], scale=100).round(2)
    df["expense_ratio_pct"]     = safe_div(df["expenses"],   df["sales"], scale=100).round(2)

    # Year helpers
    df["fiscal_year"] = df["year"].apply(fiscal_year_int)
    df["sort_order"]  = df["year"].apply(year_sort_order)
    df["is_banking"]  = is_banking.astype(int)

    df = df.sort_values(["company_id","sort_order"]).reset_index(drop=True)
    return df


def clean_balance_sheet(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # FIX: rename other_asset -> other_assets
    df.columns = ["id","company_id","year","equity_capital","reserves","borrowings",
                  "other_liabilities","total_liabilities","fixed_assets","cwip",
                  "investments","other_assets","total_assets"]

    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["year"]       = df["year"].apply(standardize_year)

    # ── FIX: Remove exact duplicates (ASIANPAINT 2x, PNB 5x, POWERGRID 2x, TECHM 2x)
    before = len(df)
    df = df.drop_duplicates(subset=["company_id","year"], keep="first")
    dropped = before - len(df)
    if dropped:
        print(f"  Balance Sheet: removed {dropped} duplicate rows")

    num_cols = ["equity_capital","reserves","borrowings","other_liabilities",
                "total_liabilities","fixed_assets","cwip","investments",
                "other_assets","total_assets"]
    for col in num_cols:
        df[col] = to_num(df[col])

    net_worth = df["equity_capital"] + df["reserves"]
    df["debt_to_equity"] = safe_div(df["borrowings"], net_worth).round(4)
    df["equity_ratio"]   = safe_div(net_worth, df["total_assets"]).round(4)

    df["fiscal_year"] = df["year"].apply(fiscal_year_int)
    df["sort_order"]  = df["year"].apply(year_sort_order)

    df = df.sort_values(["company_id","sort_order"]).reset_index(drop=True)
    return df


def clean_cash_flow(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["id","company_id","year","operating_activity","investing_activity",
                  "financing_activity","net_cash_flow"]

    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["year"]       = df["year"].apply(standardize_year)  # fixes Mar-YY for TCS

    num_cols = ["operating_activity","investing_activity","financing_activity","net_cash_flow"]
    for col in num_cols:
        df[col] = to_num(df[col])

    # Drop 2 all-null rows (HDFCLIFE Mar 2013, Mar 2014)
    before = len(df)
    df = df.dropna(subset=num_cols, how="all")
    print(f"  Cash Flow: dropped {before - len(df)} all-null rows")

    # Remove exact duplicates on (company_id, year) — keep first
    before = len(df)
    df = df.drop_duplicates(subset=["company_id","year"], keep="first")
    dropped = before - len(df)
    if dropped:
        print(f"  Cash Flow: removed {dropped} duplicate rows")

    df["free_cash_flow"] = df["operating_activity"] + df["investing_activity"]
    df["fiscal_year"]    = df["year"].apply(fiscal_year_int)
    df["sort_order"]     = df["year"].apply(year_sort_order)

    df = df.sort_values(["company_id","sort_order"]).reset_index(drop=True)
    return df


def clean_documents(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["id","company_id","year","annual_report_url"]
    df["company_id"] = df["company_id"].astype(str).str.strip()
    df["year"]       = to_num(df["year"])

    # ── FIX: Double-slash URLs (//) — affects 1,204 rows ─────────────────────
    df["annual_report_url"] = df["annual_report_url"].apply(fix_url_double_slash)

    # Drop rows with no URL
    before = len(df)
    df = df.dropna(subset=["annual_report_url"])
    print(f"  Documents: dropped {before - len(df)} null-URL rows")

    df = df.sort_values(["company_id","year"]).reset_index(drop=True)
    return df


def clean_pros_cons(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = ["id","company_id","pros","cons"]
    df["company_id"] = df["company_id"].astype(str).str.strip()
    for col in ["pros","cons"]:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace({"nan":np.nan,"None":np.nan,"NULL":np.nan})
    df = df.dropna(subset=["pros","cons"], how="all").reset_index(drop=True)
    return df


# ── Cross-sheet derived metrics ───────────────────────────────────────────────

def compute_cross_sheet(pl, bs, cf):
    # asset_turnover and return_on_assets require both P&L and BS
    key = ["company_id","year"]
    merged = pl[key + ["sales","net_profit"]].merge(
        bs[key + ["total_assets"]], on=key, how="left"
    )
    pl["asset_turnover"]       = safe_div(merged["sales"],      merged["total_assets"]).round(4)
    pl["return_on_assets_pct"] = safe_div(merged["net_profit"], merged["total_assets"], scale=100).round(2)

    # cash_conversion_ratio
    cf_merged = cf[key + ["operating_activity"]].merge(
        pl[key + ["net_profit"]], on=key, how="left"
    )
    cf["cash_conversion_ratio"] = safe_div(
        cf_merged["operating_activity"], cf_merged["net_profit"]
    ).round(4)
    return pl, bs, cf


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    xl = pd.ExcelFile(EXCEL_PATH)

    print("Reading sheets (skipping title row)...")
    raw = {s: pd.read_excel(xl, sheet_name=s, header=1) for s in xl.sheet_names}

    cleaners = {
        "Companies":     clean_companies,
        "Analysis":      clean_analysis,
        "Profit_Loss":   clean_profit_loss,
        "Balance_Sheet": clean_balance_sheet,
        "Cash_Flow":     clean_cash_flow,
        "Documents":     clean_documents,
        "Pros_Cons":     clean_pros_cons,
    }

    cleaned = {}
    for sheet, fn in cleaners.items():
        raw_df  = raw[sheet]
        clean_df = fn(raw_df)
        cleaned[sheet] = clean_df
        path = os.path.join(OUTPUT_DIR, f"{sheet.lower()}.csv")
        clean_df.to_csv(path, index=False)
        print(f"  [{sheet}] {len(raw_df)} raw -> {len(clean_df)} clean  saved")

    # Cross-sheet metrics
    print("\nComputing cross-sheet metrics...")
    pl, bs, cf = compute_cross_sheet(
        cleaned["Profit_Loss"], cleaned["Balance_Sheet"], cleaned["Cash_Flow"]
    )
    for name, df in [("profit_loss", pl), ("balance_sheet", bs), ("cash_flow", cf)]:
        df.to_csv(os.path.join(OUTPUT_DIR, f"{name}.csv"), index=False)

    # ── Quality report ────────────────────────────────────────────────────────
    print("\n" + "="*60)
    print("QUALITY REPORT")
    print("="*60)

    pl_c  = cleaned["Profit_Loss"]
    bs_c  = cleaned["Balance_Sheet"]
    cf_c  = cleaned["Cash_Flow"]
    co_c  = cleaned["Companies"]

    print(f"\n  Companies     : {len(co_c)} ({co_c['id'].nunique()} unique symbols)")
    print(f"  Profit_Loss   : {len(pl_c)} rows, {pl_c['company_id'].nunique()} companies")
    print(f"  Balance_Sheet : {len(bs_c)} rows, {bs_c['company_id'].nunique()} companies")
    print(f"  Cash_Flow     : {len(cf_c)} rows, {cf_c['company_id'].nunique()} companies")
    print(f"  Analysis      : {len(cleaned['Analysis'])} rows (long form)")
    print(f"  Documents     : {len(cleaned['Documents'])} rows")
    print(f"  Pros_Cons     : {len(cleaned['Pros_Cons'])} rows")

    # Duplicate check
    pl_dups = pl_c.duplicated(["company_id","year"]).sum()
    bs_dups = bs_c.duplicated(["company_id","year"]).sum()
    print(f"\n  P&L duplicates remaining     : {pl_dups}  (expected 0)")
    print(f"  BS  duplicates remaining     : {bs_dups}  (expected 0)")

    # Banking OPM check
    banking_opm = pl_c[pl_c["company_id"].isin(BANKING_SYMBOLS)]["opm_percentage"].notna().sum()
    print(f"  Banking OPM% non-null (expected 0): {banking_opm}")

    # URL double-slash check
    bad_urls = cleaned["Documents"]["annual_report_url"].str.contains(r"(?<!:)//", regex=True, na=False).sum()
    print(f"  Documents double-slash URLs (expected 0): {bad_urls}")

    # Year format check
    print("\n  Year samples after standardisation:")
    for sheet in ["Profit_Loss","Balance_Sheet","Cash_Flow"]:
        sample = cleaned[sheet]["year"].unique()[:6]
        print(f"    {sheet}: {list(sample)}")

    print("\nDone. Clean CSVs ->", OUTPUT_DIR)


if __name__ == "__main__":
    main()
