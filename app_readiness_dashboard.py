

#!/usr/bin/env python3
import io
import re
import json
import time
import zipfile
from pathlib import Path

import requests
import pandas as pd
import plotly.express as px
import streamlit as st

try:
    import pycountry
    HAVE_PYCOUNTRY = True
except Exception:
    HAVE_PYCOUNTRY = False

APP_TITLE = "🛡️ Military Readiness Dashboard (Budget • Deployments • Installations)"

# -------------------------
# Data fetchers / loaders
# -------------------------

OMB_TABLE_32_XLS = (
    # OMB Historical Tables, Table 3.2 “Outlays by Function and Subfunction” (archived)
    # Contains function 050 (National Defense) and subfunction lines (e.g., 051 DoD-Military)
    "https://obamawhitehouse.archives.gov/sites/default/files/omb/budget/fy2016/assets/hist03z2.xls"
)

@st.cache_data(ttl=24*3600, show_spinner=False)
def fetch_omb_table_32() -> pd.DataFrame:
    """
    Download and parse OMB Historical Tables 3.2.
    Returns a tidy df with columns: ['line','label','year','outlays'].
    """
    resp = requests.get(OMB_TABLE_32_XLS, timeout=30)
    resp.raise_for_status()
    with io.BytesIO(resp.content) as bio:
        # The OMB sheet has a few header rows. We'll infer header row by scanning for the "Function and subfunction" column.
        xls = pd.ExcelFile(bio)
        # Historically the first sheet contains Table 3.2
        sheet = xls.sheet_names[0]
        raw = pd.read_excel(bio, sheet_name=sheet, header=None)

    # Find header row (col 0 == 'Function and subfunction')
    header_row_idx = None
    for i in range(min(20, len(raw))):
        if str(raw.iloc[i, 0]).strip().lower().startswith("function and subfunction"):
            header_row_idx = i
            break
    if header_row_idx is None:
        # fallback: assume row 2
        header_row_idx = 2

    cols = raw.iloc[header_row_idx].tolist()
    df = raw.iloc[header_row_idx+1:].copy()
    df.columns = cols
    df = df.rename(columns={df.columns[0]: "Function and subfunction"})

    # Keep numeric year columns and the label column
    year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c))]
    keep = ["Function and subfunction"] + year_cols
    df = df[keep].copy()

    # Drop rows that are completely NaN across years
    df = df.dropna(axis=0, how="all", subset=year_cols)

    # Tidy
    tidy = df.melt(id_vars=["Function and subfunction"], var_name="year", value_name="outlays")
    tidy["year"] = tidy["year"].astype(int)
    # Coerce to numeric
    tidy["outlays"] = pd.to_numeric(tidy["outlays"], errors="coerce")

    # Identify National Defense lines
    # Function code 050 line includes "050 National defense" at start;
    # subfunction 051 (DoD–Military) often appears as "051 Department of Defense–Military"
    tidy["line"] = tidy["Function and subfunction"].fillna("").astype(str)
    tidy["label"] = tidy["line"].str.replace(r"^\s*\d+\s+", "", regex=True)

    return tidy[["line", "label", "year", "outlays"]].reset_index(drop=True)

def pick_defense_series(tidy: pd.DataFrame, which: str = "050") -> pd.DataFrame:
    """
    which: "050" (total National Defense) or "051" (DoD–Military)
    """
    assert which in {"050", "051"}
    m = tidy["line"].str.strip().str.startswith(which + " ")
    sel = tidy[m].groupby("year", as_index=False)["outlays"].sum().sort_values("year")
    sel["series"] = "National defense ({}*)".format(which)
    return sel

# -------------------------
# DMDC / deployments upload helpers
# -------------------------

COUNTRY_FIXES = {
    # quick name fixes for ISO mapping
    "korea, south": "South Korea",
    "korea, north": "North Korea",
    "cote d'ivoire": "Côte d'Ivoire",
    "ivory coast": "Côte d'Ivoire",
    "bahamas, the": "Bahamas",
    "gambia, the": "Gambia",
    "congo, democratic republic of the": "Democratic Republic of the Congo",
    "congo, republic of the": "Republic of the Congo",
    "russia": "Russian Federation",
    "bolivia": "Bolivia, Plurinational State of",
    "iran": "Iran, Islamic Republic of",
    "moldova": "Moldova, Republic of",
    "syria": "Syrian Arab Republic",
    "viet nam": "Viet Nam",
    "lao pdr": "Lao People's Democratic Republic",
    "macedonia": "North Macedonia",
    "eswatini": "Eswatini",
}

def to_iso3(name: str) -> str | None:
    if not HAVE_PYCOUNTRY:
        return None
    n = (name or "").strip()
    if not n:
        return None
    fixed = COUNTRY_FIXES.get(n.lower(), n)
    try:
        c = pycountry.countries.lookup(fixed)
        return c.alpha_3
    except Exception:
        return None

def load_deployments_table(file) -> pd.DataFrame:
    """
    Accepts an uploaded DMDC Excel/CSV with 'Country' (or 'Location') and a personnel column.
    Returns df with columns: country, iso3 (if pycountry present), personnel.
    """
    name = getattr(file, "name", "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        df = pd.read_excel(file)
    else:
        df = pd.read_csv(file)

    # Try to locate key columns (case-insensitive)
    cols = {c.lower(): c for c in df.columns}
    country_col = None
    for cand in ("country", "location", "country/territory", "duty location"):
        if cand in cols:
            country_col = cols[cand]; break
    if not country_col:
        # fallback: first text-like column
        country_col = df.columns[0]

    # personnel column: pick the first numeric-ish column that isn't the country
    personnel_col = None
    for c in df.columns:
        if c == country_col: continue
        if pd.api.types.is_numeric_dtype(df[c]):
            personnel_col = c; break
    if not personnel_col:
        # try to coerce last column to numeric
        c = df.columns[-1]
        df[c] = pd.to_numeric(df[c], errors="coerce")
        personnel_col = c

    out = df[[country_col, personnel_col]].rename(columns={country_col:"country", personnel_col:"personnel"})
    out = out.dropna(subset=["country", "personnel"])
    out["personnel"] = pd.to_numeric(out["personnel"], errors="coerce").fillna(0).astype(int)
    if HAVE_PYCOUNTRY:
        out["iso3"] = out["country"].apply(to_iso3)
    return out

# -------------------------
# Installations (user CSV)
# -------------------------

def load_installations_csv(file) -> pd.DataFrame:
    df = pd.read_csv(file)
    # Expect: name, lat, lon, service (optional)
    # minimal cleaning
    rename_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in {"name", "installation", "base"}: rename_map[c] = "name"
        elif lc in {"lat","latitude"}: rename_map[c] = "lat"
        elif lc in {"lon","lng","longitude"}: rename_map[c] = "lon"
        elif lc in {"service","branch"}: rename_map[c] = "service"
    df = df.rename(columns=rename_map)
    needed = {"name","lat","lon"}
    if not needed.issubset(set(df.columns)):
        raise ValueError("Installations CSV must have at least: name, lat, lon")
    return df

# -------------------------
# UI
# -------------------------

st.set_page_config(page_title="Military Readiness Dashboard", page_icon="🛡️", layout="wide")
st.title(APP_TITLE)
st.caption("Prototype • Upload DMDC deployments & base lists • Plot budgets from OMB")

with st.sidebar:
    st.markdown("### Data Sources")
    st.write("• OMB Historical Tables (Table 3.2) — National defense outlays.")  # OMB source
    st.write("• DMDC “By Country” Excel — upload below for deployments.")
    st.write("• Optional: base CSV with name/lat/lon for installations map.")
    st.divider()
    st.markdown("**Options**")
    which_series = st.radio("Budget series", ["050 National defense (total)", "051 DoD–Military (subset)"], index=0)

# --- Budget block ---
st.header("1) Budget — Outlays over time (OMB)")

err = None
with st.spinner("Downloading OMB Historical Table 3.2 …"):
    try:
        tidy = fetch_omb_table_32()
    except Exception as e:
        err = e

if err:
    st.error(f"Could not fetch OMB table: {err}")
else:
    series_code = "050" if which_series.startswith("050") else "051"
    s = pick_defense_series(tidy, which=series_code)
    fig = px.line(
        s, x="year", y="outlays",
        title=f"OMB Outlays — National Defense ({series_code}) — Current $ (billions)",
        markers=True
    )
    fig.update_layout(height=420, yaxis_title="Outlays (current $)")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("See parsed rows (debug)"):
        st.dataframe(s, use_container_width=True)

st.divider()

# --- Deployments block ---
st.header("2) Deployments — personnel by country (upload)")

up = st.file_uploader("Upload DMDC (or similar) Excel/CSV", type=["xlsx","xls","csv"], help="Must include Country and a personnel count column")
if up is not None:
    try:
        dep = load_deployments_table(up)
        st.success(f"Parsed {len(dep)} rows.")
        if HAVE_PYCOUNTRY and dep["iso3"].notna().any():
            fig = px.choropleth(
                dep, locations="iso3", color="personnel",
                color_continuous_scale="Blues",
                hover_name="country",
                title="Active duty (or total) personnel by country"
            )
            fig.update_layout(height=520, geo=dict(showframe=False))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Install `pycountry` to enable ISO-code mapping and world map. Showing table instead.")
            st.dataframe(dep.sort_values("personnel", ascending=False), use_container_width=True)

        # Top-N breakdown
        topn = st.slider("Top N countries", 5, 30, 10)
        st.bar_chart(dep.sort_values("personnel", ascending=False).head(topn).set_index("country")["personnel"])

        # Download cleaned CSV
        st.download_button(
            "Download cleaned deployments CSV",
            dep.to_csv(index=False).encode("utf-8"),
            file_name="deployments_clean.csv",
            mime="text/csv",
        )
    except Exception as e:
        st.error(f"Could not parse the uploaded file: {e}")
else:
    st.caption("Tip: Get the latest *“Military and Civilian Personnel by State/Country”* Excel from DMDC and upload it here.")

st.divider()

# --- Installations block ---
st.header("3) Installations — points CSV (optional upload)")

ins = st.file_uploader("Upload installations CSV (name, lat, lon, service?)", type=["csv"], key="installations")
if ins is not None:
    try:
        bases = load_installations_csv(ins)
        st.success(f"Parsed {len(bases)} installations.")
        color_col = "service" if "service" in bases.columns else None
        fig = px.scatter_geo(
            bases, lat="lat", lon="lon", hover_name="name",
            color=color_col, title="Installations map (uploaded CSV)"
        )
        fig.update_layout(height=520, geo=dict(showframe=False))
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(bases.head(50), use_container_width=True)
    except Exception as e:
        st.error(f"Could not parse installations CSV: {e}")
else:
    st.caption("Need a quick start? Build a sheet with columns: name, lat, lon, service (optional).")

st.divider()
st.markdown(
    "#### Notes\n"
    "- OMB outlays are **current dollars**; for constant-dollar analysis you’ll want deflators.\n"
    "- DMDC files change layout over time; the uploader tries to auto-detect the country and personnel columns.\n"
    "- Chaplaincy analytics: upload a roster CSV with an `occupation` or `AFSC/MOS` column and filter for *chaplain* to trend billets vs. population.\n"
)

