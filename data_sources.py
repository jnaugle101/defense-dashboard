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


# ---------- Helpers ----------
def _wb_indicator_to_df(code: str, countries="all", start=1990, end=None,
                        metric_name: str = "", unit: str = "") -> pd.DataFrame:
    """
    Generic World Bank indicator fetcher -> tidy DF.
    """
    if end is None:
        end = datetime.now().year
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
        val = rec.get("value")
        if val is None:
            continue
        rows.append(
            {
                "source": "World Bank",
                "country": rec.get("country", {}).get("value"),
                "year": int(rec.get("date", 0) or 0),
                "metric": metric_name or code,
                "value": float(val),
                "unit": unit,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.dropna(subset=["value"])
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    return df


def _owid_grapher_csv_to_df(url: str, value_col: str, metric_name: str, unit: str) -> pd.DataFrame:
    """
    Our World in Data 'grapher' CSVs usually have columns: Entity, Code, Year, <value_col>.
    We reshape that into our standard schema.
    """
    try:
        df = pd.read_csv(url)
    except Exception:
        return pd.DataFrame()

    # Normalize common OWID casing
    # Some files use 'Entity'/'entity', 'Year'/'year'
    cols = {c.lower(): c for c in df.columns}
    ent = cols.get("entity") or cols.get("country") or "Entity"
    yr = cols.get("year") or "Year"
    val = value_col if value_col in df.columns else cols.get(value_col.lower())

    if ent not in df.columns or yr not in df.columns or val not in df.columns:
        return pd.DataFrame()

    out = df[[ent, yr, val]].rename(columns={ent: "country", yr: "year", val: "value"})
    out["source"] = "Our World in Data"
    out["metric"] = metric_name
    out["unit"] = unit
    # tidy types
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["value"])
    return out[["source", "country", "year", "metric", "value", "unit"]]


# ---------- World Bank: existing ----------
@cache
def wb_milex_percent_gdp(countries="all", start=1990, end=datetime.now().year) -> pd.DataFrame:
    # Military expenditure (% of GDP)
    return _wb_indicator_to_df(
        code="MS.MIL.XPND.GD.ZS",
        countries=countries,
        start=start,
        end=end,
        metric_name="Military Expenditure (% GDP)",
        unit="percent",
    )


# ---------- World Bank: NEW additions ----------
@cache
def wb_milex_current_usd(countries="all", start=1990, end=datetime.now().year) -> pd.DataFrame:
    # Military expenditure (current US$)
    # Code verified: MS.MIL.XPND.CD
    return _wb_indicator_to_df(
        code="MS.MIL.XPND.CD",
        countries=countries,
        start=start,
        end=end,
        metric_name="Military Expenditure (current US$)",
        unit="USD",
    )


@cache
def wb_armed_forces_total(countries="all", start=1990, end=datetime.now().year) -> pd.DataFrame:
    # Armed forces personnel, total
    # Code verified: MS.MIL.TOTL.P1
    return _wb_indicator_to_df(
        code="MS.MIL.TOTL.P1",
        countries=countries,
        start=start,
        end=end,
        metric_name="Armed Forces Personnel (total)",
        unit="persons",
    )


@cache
def wb_armed_forces_pct_labor(countries="all", start=1990, end=datetime.now().year) -> pd.DataFrame:
    # Armed forces personnel (% of total labor force)
    # Code verified: MS.MIL.TOTL.TF.ZS
    return _wb_indicator_to_df(
        code="MS.MIL.TOTL.TF.ZS",
        countries=countries,
        start=start,
        end=end,
        metric_name="Armed Forces Personnel (% labor force)",
        unit="percent",
    )


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


# ---------- Our World in Data (NEW) ----------
@cache
def owid_military_personnel() -> pd.DataFrame:
    """
    Military personnel (OWID grapher). If the endpoint changes, this will just return empty.
    """
    url = "https://ourworldindata.org/grapher/military-personnel.csv"
    # value column name inside CSV is also "military-personnel"
    return _owid_grapher_csv_to_df(
        url=url,
        value_col="military-personnel",
        metric_name="Military Personnel (OWID)",
        unit="persons",
    )


@cache
def owid_mil_exp_share_gdp() -> pd.DataFrame:
    """
    Military expenditure as share of GDP (OWID grapher). This file name occasionally changes.
    Wrapped in try/except to fail gracefully if not available.
    """
    url = "https://ourworldindata.org/grapher/military-expenditure-share-gdp.csv"
    return _owid_grapher_csv_to_df(
        url=url,
        value_col="military-expenditure-share-gdp",
        metric_name="Military Expenditure (% GDP, OWID)",
        unit="percent",
    )


# ---------- Registry + loader ----------
REGISTRY = {
    # World Bank (existing + new)
    "World Bank: mil exp %GDP": wb_milex_percent_gdp,
    "World Bank: mil exp (USD)": wb_milex_current_usd,
    "World Bank: armed forces total": wb_armed_forces_total,
    "World Bank: armed forces % labor": wb_armed_forces_pct_labor,

    # UN & USAspending (existing)
    "UN Peacekeeping: contributors": unpk_troop_contributors,
    "USAspending: DoD obligations": usaspending_dod_obligations,

    # OWID (new, optional)
    "OWID: military personnel": owid_military_personnel,
    "OWID: mil exp %GDP": owid_mil_exp_share_gdp,
    # You can add a NATO CSV/XLS source later when you find a stable structured link.
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
