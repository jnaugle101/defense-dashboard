
# data_sources.py
import io, json, zipfile, pandas as pd, requests
from datetime import datetime
from functools import lru_cache

# Streamlit cache if you’re inside Streamlit; otherwise comment out.
try:
    import streamlit as st
    cache = st.cache_data(ttl=60*60)  # 1 hour
except Exception:
    def cache(func=None, **_):
        return func

# ---------- World Bank: Military expenditure % of GDP (code: MS.MIL.XPND.GD.ZS) ----------
@cache
def wb_milex_percent_gdp(countries="all", start=1990, end=datetime.now().year):
    # Docs: https://api.worldbank.org/v2/country/{id}/indicator/{code}?downloadformat=excel
    code = "MS.MIL.XPND.GD.ZS"
    url = f"https://api.worldbank.org/v2/country/{countries}/indicator/{code}?format=json&per_page=20000&date={start}:{end}"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or len(data) < 2:
        return pd.DataFrame()
    rows = []
    for rec in data[1]:
        if rec["value"] is None:
            continue
        rows.append({
            "source": "World Bank",
            "country": rec["country"]["value"],
            "iso2": rec["countryiso3code"],
            "year": int(rec["date"]),
            "metric": "Military Expenditure (% GDP)",
            "value": float(rec["value"]),
            "unit": "percent",
        })
    return pd.DataFrame(rows)

# ---------- NATO: Defence expenditure (you can swap this URL to latest XLSX each year) ----------
@cache
def nato_defence_spending():
    # Example public XLS (change to current year if NATO updates the path)
    xls_url = "https://www.nato.int/nato_static_fl2014/assets/pdf/2024/6/pdf/240603-pr-2024-068-en.pdf"  # placeholder; replace with XLS/CSV when available
    # If only a PDF is posted, prefer manually downloading/hosting the companion XLS,
    # or parse PDF with tabula/camelot (heavier). For now, return empty if we can’t get structured data.
    try:
        # If you have a CSV/XLS path, use pd.read_excel/read_csv here:
        return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

# ---------- UN Peacekeeping: Troop contributions ----------
@cache
def unpk_troop_contributors():
    # A common CSV endpoint (the UN site occasionally changes):
    # Use a maintained file or a simple mirror in your repo/S3 for stability.
    csv_url = "https://peacekeeping.un.org/sites/default/files/mission_contributions_by_country_month_0.csv"
    try:
        df = pd.read_csv(csv_url)
    except Exception:
        return pd.DataFrame()

    # Normalize columns (these change—adjust as needed)
    # Expect columns like: 'Country', 'Year', 'Month', 'Troops', 'Police', ...
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if "country" in cl: rename[c] = "country"
        if cl in ("year",): rename[c] = "year"
        if "troop" in cl and "police" not in cl: rename[c] = "troops"
    df = df.rename(columns=rename)
    req = {"country","year"}
    if not req.issubset(df.columns):
        return pd.DataFrame()

    # Aggregate per year/country
    out = (df.groupby(["country","year"])["troops"]
             .sum(min_count=1)
             .reset_index())
    out["source"] = "UN Peacekeeping"
    out["metric"] = "Troops contributed to UN PKO"
    out["value"] = out["troops"]
    out["unit"] = "personnel"
    return out[["source","country","year","metric","value","unit"]]

# ---------- USAspending: DoD obligations by FY ----------
@cache
def usaspending_dod_obligations(start_fy=2016, end_fy=datetime.now().year):
    # Docs: https://api.usaspending.gov
    url = "https://api.usaspending.gov/api/v2/financial_balances/agencies/"
    # DoD toptier_code is "097"
    rows = []
    for fy in range(start_fy, end_fy+1):
        try:
            r = requests.get(f"{url}?fiscal_year={fy}", timeout=30)
            r.raise_for_status()
            data = r.json().get("results", [])
            dod = next((x for x in data if x.get("toptier_code") == "097"), None)
            if not dod:
                continue
            rows.append({
                "source": "USAspending",
                "country": "United States",
                "year": fy,
                "metric": "DoD Obligations",
                "value": float(dod.get("obligations", 0.0)),
                "unit": "USD",
            })
        except Exception:
            continue
    return pd.DataFrame(rows)

# ---------- Registry ----------
REGISTRY = {
    "World Bank: mil exp %GDP": wb_milex_percent_gdp,
    "UN Peacekeeping: contributors": unpk_troop_contributors,
    "USAspending: DoD obligations": usaspending_dod_obligations,
    # "NATO: defence spending": nato_defence_spending,  # enable when you wire a structured XLS/CSV
}

def load_selected(sources: list[str]) -> pd.DataFrame:
    frames = []
    for name in sources:
        fn = REGISTRY.get(name)
        if not fn:
            continue
        try:
            frames.append(fn())
        except Exception:
            pass
    if not frames:
        return pd.DataFrame(columns=["source","country","year","metric","value","unit"])
    df = pd.concat(frames, ignore_index=True).dropna(subset=["value"])
    # basic cleaning
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df
