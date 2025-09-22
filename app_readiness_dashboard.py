#!/usr/bin/env python3
# app_readiness_dashboard.py

import io
import re
from pathlib import Path

import pandas as pd
import plotly.express as px
import requests
import streamlit as st

# ---- Page config (ONLY once) ----
APP_TITLE = "🛡️ Military Readiness Dashboard (Budget • Deployments • Installations)"
st.set_page_config(page_title="Military Readiness Dashboard", page_icon="🛡️", layout="wide")
st.title(APP_TITLE)
st.caption("Prototype • Upload DMDC deployments & base lists • Plot budgets from OMB and public APIs")

# ---- Optional ISO mapping for uploads ----
try:
    import pycountry
    HAVE_PYCOUNTRY = True
except Exception:
    HAVE_PYCOUNTRY = False

# ---- Data sources (World Bank / UNPK / USAspending) ----
from data_sources import REGISTRY, load_selected

with st.sidebar:
    st.header("Data sources")
    choices = st.multiselect(
        "Choose sources to load",
        options=list(REGISTRY.keys()),
        default=[
            "World Bank: mil exp %GDP",
            "UN Peacekeeping: contributors",
            "USAspending: DoD obligations",
        ],
        key="source_picker",
        help="This loads multiple public datasets together for quick exploration."
    )

# Load the combined dataset from data_sources.py
# Load the combined dataset from data_sources.py
df = load_selected(choices)

# --- Combined dataset (preview) with filter + unique widget keys ---
st.subheader("Combined dataset (preview)")
st.caption("This table mixes rows from all selected sources. Use the filter to focus it.")

src_options = ["All"] + sorted(df["source"].dropna().unique().tolist())
src_pick = st.selectbox(
    "Filter preview by source",
    src_options,
    index=0,
    key="preview_source",   # unique key avoids duplicate-ID errors
)
preview_df = df if src_pick == "All" else df[df["source"] == src_pick]

show_cols = [c for c in ["source", "country", "year", "metric", "value", "unit"] if c in preview_df.columns]
rows_to_show = min(50, len(preview_df))
if rows_to_show:
    st.dataframe(
        preview_df.sample(rows_to_show, random_state=42)[show_cols],
        use_container_width=True
    )
else:
    st.info("No rows for the current selection.")


# ---------- Example charts from the combined dataset ----------
# World Bank: Military Expenditure (% GDP)
wb = df[df["metric"] == "Military Expenditure (% GDP)"]
if not wb.empty:
    st.markdown("### World Bank — Military Expenditure as % of GDP")
    countries = st.multiselect(
        "Countries to plot (WB)",
        sorted(wb["country"].dropna().unique()),
        default=["United States"],
        key="wb_countries",
    )
    if countries:
        m = wb[wb["country"].isin(countries)]
        fig = px.line(
            m.sort_values(["country", "year"]),
            x="year", y="value", color="country",
            labels={"value": "% of GDP", "year": "Year"},
            title="World Bank: Military Expenditure as % of GDP"
        )
        st.plotly_chart(fig, use_container_width=True)
    st.caption("Source: World Bank MS.MIL.XPND.GD.ZS — country selection applies only to this chart.")

# UN Peacekeeping (Top N latest year)
unpk = df[df["metric"] == "Troops contributed to UN PKO"]
if not unpk.empty:
    latest = unpk["year"].dropna().max()
    st.markdown(f"### UN Peacekeeping — Top contributors in {int(latest)}")
    top_n = st.slider("Show top N countries (UNPK)", 5, 30, 15, 1, key="unpk_topn")
    topn = (
        unpk[unpk["year"] == latest]
        .nlargest(top_n, "value")
        .sort_values("value", ascending=True)
    )
    fig2 = px.bar(
        topn, x="value", y="country", orientation="h",
        labels={"value": "Troops", "country": ""},
        title=f"Top UN PKO Troop Contributors — {int(latest)}"
    )
    st.plotly_chart(fig2, use_container_width=True)
    st.caption("UN PKO data is global by nature, so you’ll see major contributors (often African/Asian countries).")

# USAspending DoD obligations
dod = df[df["metric"] == "DoD Obligations"]
if not dod.empty:
    st.markdown("### USAspending — DoD Obligations by Fiscal Year (United States)")
    fig3 = px.line(
        dod.sort_values("year"),
        x="year", y="value",
        title="USAspending: DoD Obligations by Fiscal Year",
        labels={"value": "USD", "year": "FY"}
    )
    st.plotly_chart(fig3, use_container_width=True)
    st.caption("Source: USAspending.gov API; this series is for the United States only.")

st.divider()

# ------------------------------------------------------------------------------------
# OMB Historical Tables — Table 3.2 (“Outlays by Function and Subfunction”)
# ------------------------------------------------------------------------------------
OMB_TABLE_32_XLS = (
    "https://obamawhitehouse.archives.gov/sites/default/files/omb/budget/fy2016/assets/hist03z2.xls"
)
UA = {"User-Agent": "ReadinessDashboard/1.0 (+contact)"}

@st.cache_data(ttl=24*3600, show_spinner=False)
def fetch_omb_table_32() -> pd.DataFrame:
    """
    Download and parse OMB Historical Table 3.2.
    Returns tidy df: ['line','label','year','outlays'] in current $.
    """
    resp = requests.get(OMB_TABLE_32_XLS, headers=UA, timeout=30)
    resp.raise_for_status()
    bio = io.BytesIO(resp.content)

    # Read sheet (older .xls requires xlrd)
    try:
        xls = pd.ExcelFile(bio)
        sheet = xls.sheet_names[0]
        raw = pd.read_excel(bio, sheet_name=sheet, header=None, engine="xlrd")
    except Exception:
        # fallback: let pandas pick an engine if available
        bio.seek(0)
        raw = pd.read_excel(bio, header=None)

    # Find header row (col 0 == 'Function and subfunction')
    header_row_idx = None
    for i in range(min(20, len(raw))):
        if str(raw.iloc[i, 0]).strip().lower().startswith("function and subfunction"):
            header_row_idx = i
            break
    if header_row_idx is None:
        header_row_idx = 2

    cols = raw.iloc[header_row_idx].tolist()
    df_omb = raw.iloc[header_row_idx + 1:].copy()
    df_omb.columns = cols
    df_omb = df_omb.rename(columns={df_omb.columns[0]: "Function and subfunction"})

    # Keep numeric year columns and the label column
    year_cols = [c for c in df_omb.columns if re.fullmatch(r"\d{4}", str(c))]
    keep = ["Function and subfunction"] + year_cols
    df_omb = df_omb[keep].copy()

    # Drop rows that are completely NaN across years
    df_omb = df_omb.dropna(axis=0, how="all", subset=year_cols)

    # Tidy
    tidy = df_omb.melt(id_vars=["Function and subfunction"], var_name="year", value_name="outlays")
    tidy["year"] = pd.to_numeric(tidy["year"], errors="coerce").astype("Int64")
    tidy["outlays"] = pd.to_numeric(tidy["outlays"], errors="coerce")

    # Label extraction
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
    sel["series"] = f"National defense ({which}*)"
    return sel

st.header("1) Budget — OMB Outlays over time")
series_choice = st.radio(
    "Budget series",
    ["050 National defense (total)", "051 DoD–Military (subset)"],
    index=0,
    key="omb_series_choice",
)
err = None
with st.spinner("Downloading OMB Historical Table 3.2 …"):
    try:
        tidy = fetch_omb_table_32()
    except Exception as e:
        err = e

if err:
    st.error(f"Could not fetch OMB table: {err}")
else:
    series_code = "050" if series_choice.startswith("050") else "051"
    s = pick_defense_series(tidy, which=series_code)
    fig = px.line(
        s, x="year", y="outlays",
        title=f"OMB Outlays — National Defense ({series_code}) — Current $",
        markers=True
    )
    fig.update_layout(height=420, yaxis_title="Outlays (current $)")
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("See parsed rows"):
        st.dataframe(s, use_container_width=True)

st.divider()

# ------------------------------------------------------------------------------------
# 2) Deployments — personnel by country (user upload)
# ------------------------------------------------------------------------------------
st.header("2) Deployments — personnel by country (upload)")

COUNTRY_FIXES = {
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
    Returns df with columns: country, iso3 (if available), personnel.
    """
    name = getattr(file, "name", "").lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        # engine auto-pick; older xls needs xlrd (included in requirements)
        df = pd.read_excel(file)
    else:
        df = pd.read_csv(file)

    cols = {c.lower(): c for c in df.columns}
    country_col = next((cols[c] for c in ("country", "location", "country/territory", "duty location") if c in cols), None)
    if not country_col:
        country_col = df.columns[0]

    personnel_col = None
    for c in df.columns:
        if c == country_col:
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            personnel_col = c
            break
    if not personnel_col:
        c = df.columns[-1]
        df[c] = pd.to_numeric(df[c], errors="coerce")
        personnel_col = c

    out = df[[country_col, personnel_col]].rename(columns={country_col: "country", personnel_col: "personnel"})
    out = out.dropna(subset=["country", "personnel"])
    out["personnel"] = pd.to_numeric(out["personnel"], errors="coerce").fillna(0).astype(int)
    if HAVE_PYCOUNTRY:
        out["iso3"] = out["country"].apply(to_iso3)
    return out

up = st.file_uploader(
    "Upload DMDC (or similar) Excel/CSV",
    type=["xlsx", "xls", "csv"],
    help="Must include Country and a personnel count column",
    key="dmdc_uploader",
)
if up is not None:
    try:
        dep = load_deployments_table(up)
        st.success(f"Parsed {len(dep)} rows.")
        if HAVE_PYCOUNTRY and dep.get("iso3") is not None and dep["iso3"].notna().any():
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

        topn = st.slider("Top N countries (deployments)", 5, 30, 10, key="dep_topn")
        st.bar_chart(
            dep.sort_values("personnel", ascending=False)
               .head(topn)
               .set_index("country")["personnel"]
        )

        st.download_button(
            "Download cleaned deployments CSV",
            dep.to_csv(index=False).encode("utf-8"),
            file_name="deployments_clean.csv",
            mime="text/csv",
            key="dep_dl",
        )
    except Exception as e:
        st.error(f"Could not parse the uploaded file: {e}")
else:
    st.caption("Tip: Get the latest *“Military and Civilian Personnel by State/Country”* Excel from DMDC and upload it here.")

st.divider()

# ------------------------------------------------------------------------------------
# 3) Installations — points CSV (optional upload)
# ------------------------------------------------------------------------------------
st.header("3) Installations — points CSV (optional upload)")

def load_installations_csv(file) -> pd.DataFrame:
    df = pd.read_csv(file)
    rename_map = {}
    for c in df.columns:
        lc = c.strip().lower()
        if lc in {"name", "installation", "base"}:
            rename_map[c] = "name"
        elif lc in {"lat", "latitude"}:
            rename_map[c] = "lat"
        elif lc in {"lon", "lng", "longitude"}:
            rename_map[c] = "lon"
        elif lc in {"service", "branch"}:
            rename_map[c] = "service"
    df = df.rename(columns=rename_map)
    needed = {"name", "lat", "lon"}
    if not needed.issubset(set(df.columns)):
        raise ValueError("Installations CSV must have at least: name, lat, lon")
    return df

ins = st.file_uploader(
    "Upload installations CSV (name, lat, lon, service?)",
    type=["csv"],
    key="installations_uploader"
)
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
    st.caption("Need a quick start? Create a CSV with columns: name, lat, lon, service (optional).")

st.divider()
st.markdown(
    "#### Notes\n"
    "- OMB outlays are **current dollars**; for constant-dollar analysis you’ll want deflators.\n"
    "- DMDC files change layout over time; the uploader tries to auto-detect the country and personnel columns.\n"
    "- Chaplaincy analytics idea: upload a roster CSV with an `occupation`/`AFSC`/`MOS` column and filter for *chaplain* to trend billets vs. population.\n"
)
