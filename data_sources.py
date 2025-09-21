# data_sources.py
import io
from datetime import datetime

import pandas as pd
import requests

# Optional Streamlit cache (safe no-op outside Streamlit)
try:
    import streamlit as st
    cache = st.cache_data(ttl=60 * 60)  # 1 hour
except Exception:
    def cache(func=None, **_):
        return func

UA = {"User-Agent": "ReadinessDashboard/1.0 (+contact)"}
TIMEOUT = 30

# ---------- World Bank: Military expenditure % of GDP ----------
@cache
def wb_milex_percent_gdp(countries="all", start=1990, end=datetime.now().year) -> pd.DataFrame:
    """
    https://api.worldbank.org/v2/country/{id}/indicator/MS.MIL.XPND.GD.ZS?format=json&per_page=20000&date=YYYY:YYYY
    """
    code = "MS.MIL.XPND.GD.ZS"
    url = (
        f"https://api.worldbank.org/v2/country/{countries}/indicator/{code}"
        f"?format=json&per_page=20000&date={start}:{end}"
    )
    try:
        r = requests.get(url, headers=UA, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
    except Exception:
        return pd.DataFrame()

    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
        return pd.DataFrame()

    rows = []
    for rec in data[1]:
        if rec.get("value") is None:
            continue
        rows.append(
            {
                "source": "World Bank",
                "country": rec.get("country", {}).get("value"),
                "iso3": rec.get("countryiso3code"),
                "year": int(rec.get("date", 0) or 0),
                "metric": "Military Expenditure (% GDP)",
                "value": float(rec["value"]),
                "unit": "percent",
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.dropna(subset=["value"])
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


# ---------- UN Peacekeeping: Troop contributors (yearly aggregate) ----------
@cache
def unpk_troop_contributors() -> pd.DataFrame:
    """
    The UN rotates CSV endpoints and headers. This tries to normalize common shapes
    like: Country, Year, Month, Troops (or 'Troop_', 'Troops_Total', etc).
    """
    url = "https://peacekeeping.un.org/sites/default/files/mission_contributions_by_country_month_0.csv"
    try:
        df = pd.read_csv(url)
    except Exception:
        return pd.DataFrame()

    # Normalize column names
    lower_map = {c: c.lower() for c in df.columns}
    df = df.rename(columns=lower_map)

    # Country column
    country_col = next((c for c in df.columns if "country" in c), None)
    # Year column
    year_col = next((c for c in df.columns if c == "year"), None)

    # Troops column (lots of variants)
    troop_cols = [c for c in df.columns if "troop" in c and "police" not in c]
    troop_col = troop_cols[0] if troop_cols else None

    if not (country_col and year_col and troop_col):
        return pd.DataFrame()

    out = (
        df[[country_col, year_col, troop_col]]
        .rename(columns={country_col: "country", year_col: "year", troop_col: "troops"})
    )
    out["troops"] = pd.to_numeric(out["troops"], errors="coerce")
    out = (
        out.dropna(subset=["year"])
        .groupby(["country", "year"], as_index=False)["troops"]
        .sum(min_count=1)
    )

    out["source"] = "UN Peacekeeping"
    out["metric"] = "Troops contributed to UN PKO"
    out["value"] = out["troops"]
    out["unit"] = "personnel"
    return out[["source", "country", "year", "metric", "value", "unit"]]


# ---------- USAspending: DoD obligations (by FY) ----------
@cache
def usaspending_dod_obligations(start_fy=2016, end_fy=datetime.now().year) -> pd.DataFrame:
    """
    https://api.usaspending.gov/api/v2/financial_balances/agencies/?fiscal_year=YYYY
    DoD toptier_code = "097"
    """
    base = "https://api.usaspending.gov/api/v2/financial_balances/agencies/"
    rows = []
    for fy in range(start_fy, end_fy + 1):
        try:
            r = requests.get(f"{base}?fiscal_year={fy}", headers=UA, timeout=TIMEOUT)
            r.raise_for_status()
            results = r.json().get("results", []) or []
            dod = next((x for x in results if x.get("toptier_code") == "097"), None)
            if not dod:
                continue
            rows.append(
                {
                    "source": "USAspending",
                    "country": "United States",
                    "year": fy,
                    "metric": "DoD Obligations",
                    "value": float(dod.get("obligations") or 0.0),
                    "unit": "USD",
                }
            )
        except Exception:
            continue

    df = pd.DataFrame(rows)
    if not df.empty:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


# ---------- (Optional) NATO: scaffold only; wire an XLS/CSV when available ----------
@cache
def nato_defence_spending() -> pd.DataFrame:
    """
    Placeholder — return an empty frame until a structured CSV/XLS is available.
    """
    return pd.DataFrame(columns=["source", "country", "year", "metric", "value", "unit"])


# ---------- Registry + loader ----------
REGISTRY = {
    "World Bank: mil exp %GDP": wb_milex_percent_gdp,
    "UN Peacekeeping: contributors": unpk_troop_contributors,
    "USAspending: DoD obligations": usaspending_dod_obligations,
    # "NATO: defence spending": nato_defence_spending,
}


def load_selected(sources: list[str]) -> pd.DataFrame:
    frames = []
    for name in sources or []:
        fn = REGISTRY.get(name)
        if not fn:
            continue
        try:
            frames.append(fn())
        except Exception:
            # keep loading other sources
            pass

    if not frames:
        return pd.DataFrame(columns=["source", "country", "year", "metric", "value", "unit"])

    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["value"])
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df
