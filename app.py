"""
app.py
Vendor / Category Sales, Inward & Inventory Report — single-file app.

This combines what used to be two separate Streamlit apps
(main_app.py + drillthrough_app.py) into ONE app/ONE process.

Why this is faster:
- Only one Streamlit server/process to boot. Drilling through no longer
  means loading a second app on a second port (which, on most hosts,
  means a cold start: re-importing pyodbc/pandas, re-detecting the ODBC
  driver, and opening a brand-new AAD service-principal SQL connection).
- The SQL connection (st.cache_resource) and the query result caches
  (st.cache_data) are process-level in Streamlit, not per-session. With
  two separate apps you effectively had two separate connection pools
  and two separate caches that never shared a hit. Merged into one
  process, a drill-through click can reuse the exact same live
  connection and, if the same query/params were already fetched, the
  same cached DataFrame — no round trip at all.
- No more DRILL_APP_URL / MAIN_APP_URL secrets or cross-port navigation.
  Drill-through links are just `?drill=...` on the same app.

DATE-FIRST FETCH + IN-MEMORY FILTERING (this revision)
--------------------------------------------------------------------
The report is now split into two stages so that changing Vendor /
Category / Company never re-hits SQL Server:

  1. FETCH (SQL, slow-ish, only runs on "Fetch Data"):
     `get_aggregate_data(conn_str, start, end)` pulls the report
     aggregated by (vendor, category, company) for the selected date
     range ONLY — no vendor/category/company filtering in the WHERE
     clause. This result is cached with `st.cache_data(ttl=21600)`
     (6 hours), keyed on the connection string + date range. As long
     as nobody clicks "Fetch Data" with a *different* date range (or
     the 6 hours expire), this never re-queries the Lakehouse — every
     user/session sharing that date range gets the cached DataFrame.

  2. FILTER (pandas, instant, runs on every rerun):
     Vendor / Category / Company are now applied with
     `filter_report_df(...)` directly on the cached DataFrame that's
     sitting in memory. Toggling these filters, paging, sorting, and
     the "Top vendors" chart all operate on that in-memory frame —
     no network round trip, no spinner.

The filter *options* themselves (which vendors/categories/companies
show up in the multiselects) are derived from the fetched DataFrame
rather than a separate SQL round trip, which removes one more query
from the critical path. They naturally scope themselves to "things
that actually appear in this date range."

Set these in Streamlit secrets:
    SQL_ENDPOINT, DATABASE, TENANT_ID, CLIENT_ID, CLIENT_SECRET

Image loading uses the same service principal to read OneLake via the
Azure Data Lake SDK. The service principal must have Fabric workspace
read access plus OneLake data access to the workspace holding the
Bronze lakehouse. Requires: azure-identity, azure-storage-file-datalake,
Pillow.
"""

import base64
import hashlib
from itertools import permutations
import io
import json
import time
import posixpath
import re
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO
from xml.sax.saxutils import escape
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode, urlparse

import pandas as pd
import pyodbc
import streamlit as st
from PIL import Image

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Report transformations and exports live in this file.
METRICS = ["sale_qty", "sale_value", "available_qty", "available_value", "inward_qty", "inward_value", "intransit_qty", "intransit_value", "outgoing_qty", "outgoing_value", "returns_qty", "returns_value"]
DATE_PRESETS = ["Today", "Yesterday", "Last 7 days", "Last 30 days", "Week to date", "Month to date", "Quarter to date", "Year to date", "Custom range"]


def display_store_name(value):
    """Presentation only: database identities remain unchanged."""
    if not isinstance(value, str):
        return value
    return re.sub(r"Wedtree eStore Private Limited\s*-\s*", "", value).strip()


def display_store_frame(frame):
    result = frame.copy()
    for column in ("company", "company_id_name", "Store"):
        if column in result:
            result[column] = result[column].map(display_store_name)
    result.columns = [display_store_name(column) for column in result.columns]
    return result


def date_bounds(preset, today):
    if preset == "Yesterday":
        return today - timedelta(days=1), today - timedelta(days=1)
    starts = {
        "Today": today,
        "Last 7 days": today - timedelta(days=6),
        "Last 30 days": today - timedelta(days=29),
        "Week to date": today - timedelta(days=today.weekday()),
        "Month to date": today.replace(day=1),
        "Quarter to date": date(today.year, ((today.month - 1) // 3) * 3 + 1, 1),
        "Year to date": date(today.year, 1, 1),
    }
    return starts[preset], today


def category_key(value):
    return re.sub(r"\s*/\s*", " / ", re.sub(r"\s+", " ", str(value).strip())).casefold()


def read_category_mapping(path):
    """Read the two mapping columns from OOXML without an Excel dependency.

    Text and cached cell values only; workbook contents are treated as data.
    Conflicting parent mappings fail instead of multiplying report totals.
    """
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(Path(path)) as workbook:
        strings = []
        if "xl/sharedStrings.xml" in workbook.namelist():
            root = ET.fromstring(workbook.read("xl/sharedStrings.xml"))
            strings = ["".join(t.text or "" for t in item.findall(".//s:t", ns)) for item in root.findall("s:si", ns)]
        sheet = ET.fromstring(workbook.read("xl/workbook.xml")).find("s:sheets/s:sheet", ns)
        rel_id = sheet.get("{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id")
        relationships = ET.fromstring(workbook.read("xl/_rels/workbook.xml.rels"))
        target = next(rel.get("Target") for rel in relationships if rel.get("Id") == rel_id)
        sheet_path = target.lstrip("/") if target.startswith("/") else posixpath.normpath("xl/" + target)
        rows = ET.fromstring(workbook.read(sheet_path)).findall(".//s:sheetData/s:row", ns)
        records = []
        for row in rows:
            values = {}
            for cell in row:
                column = re.sub(r"\d", "", cell.get("r", ""))
                value = cell.find("s:v", ns)
                text = value.text if value is not None else ""
                if cell.get("t") == "s":
                    text = strings[int(text)] if text else ""
                elif cell.get("t") == "inlineStr":
                    text = "".join(t.text or "" for t in cell.findall(".//s:t", ns))
                values[column] = (text or "").strip()
            records.append(values)
    if not records:
        raise ValueError("The category workbook is empty.")
    headers = {value.casefold(): column for column, value in records[0].items()}
    if not {"category", "display name"}.issubset(headers):
        raise ValueError("The workbook must contain CATEGORY and Display Name columns.")
    mapping = {}
    for row in records[1:]:
        parent = row.get(headers["category"], "")
        child = row.get(headers["display name"], "")
        if not parent or not child:
            continue
        key = category_key(child)
        if key in mapping and mapping[key] != parent:
            raise ValueError(f"Conflicting master categories for {child}: {mapping[key]} and {parent}")
        mapping[key] = parent
    return mapping


def add_master_category(df, mapping, column):
    result = df.copy()
    result["master_category"] = result[column].map(lambda value: mapping.get(category_key(value), "Unmatched category") if pd.notna(value) and str(value).strip() else "Unmatched category")
    return result


def aggregate_report(df, keys):
    return df.groupby(keys, dropna=False, as_index=False)[METRICS].sum()


def consolidate_products(df, kind):
    """Sum facts, retain dimension descriptions, never deduplicate by picture."""
    if df.empty:
        return df.copy()
    quantities = ["sale_qty", "sale_value"] if kind == "sales" else ["available_inventory", "available_selling_price"]
    def labels(series):
        return ", ".join(sorted(set(series.dropna().astype(str))))
    rules = {column: "sum" for column in quantities}
    for column in df.columns:
        if column == "product_id" or column in rules:
            continue
        if column in {"company", "location", "lot_number"}:
            rules[column] = labels
        elif column in {"overall_age", "sale_age"}:
            rules[column] = "max"
        elif column == "sold_date":
            rules[column] = "max"
        elif column == "stock_move_date":
            rules[column] = "min"
        else:
            rules[column] = "first"
    result = df.groupby("product_id", dropna=False, as_index=False).agg(rules)
    # The consolidated sale age must correspond to its latest sale date.
    if kind == "sales" and "sold_date" in result:
        reference = df.attrs.get("age_reference")
        if reference is not None:
            result["sale_age"] = (pd.Timestamp(reference) - pd.to_datetime(result["sold_date"])).dt.days
    return result


def scope_predicate(column, values, null_label=None):
    """Build a parameterized scope; empty selection means all values."""
    if not values:
        return "1 = 1", []
    nonnull = [value for value in values if value != null_label]
    clauses = []
    if nonnull:
        clauses.append(f"{column} IN ({', '.join('?' for _ in nonnull)})")
    if null_label is not None and null_label in values:
        clauses.append(f"{column} IS NULL")
    return "(" + " OR ".join(clauses) + ")", nonnull


LABELS = {"master_category": "Master category", "categ_id_name": "Category",
          "Vendor": "Vendor", "company_id_name": "Store", "sale_qty": "Sold quantity",
          "sale_value": "Sales value (INR)", "available_qty": "Available quantity",
          "available_value": "Available value (INR)", "inward_qty": "Inward quantity",
          "inward_value": "Inward value (INR)",
          "intransit_qty": "Intransit quantity", "intransit_value": "Intransit value (INR)",
          "outgoing_qty": "Outgoing quantity", "outgoing_value": "Outgoing value (INR)",
          "returns_qty": "Returns quantity", "returns_value": "Returns value (INR)"}


def report_export_frame(frame, view, sort_field, ascending, pivot_metric="sale_qty", pivot_order=None):
    if view == "Store pivot":
        result = frame.groupby(["master_category", "categ_id_name", "company_id_name"], dropna=False)[pivot_metric].sum().unstack("company_id_name", fill_value=0)
        result.columns = result.columns.fillna("(No store)")
        result["Total"] = result.sum(axis=1)
        result = result.sort_values("Total", ascending=ascending) if sort_field in METRICS else result.sort_index(ascending=ascending)
        return display_store_frame(result.reset_index().rename(columns=LABELS))
    keys = ["master_category", "categ_id_name", "Vendor"]
    if pivot_order and view == "Category hierarchy":
        dimension_keys = {"Store": ["company_id_name"], "Vendor": ["Vendor"], "Category": ["master_category", "categ_id_name"]}
        keys = [key for name in pivot_order for key in dimension_keys[name]]
    if view == "Detailed rows":
        keys = ["categ_id_name", "Vendor", "master_category", "company_id_name"]
    result = aggregate_report(frame, keys)
    primary = sort_field if sort_field in METRICS or sort_field in keys else keys[0]
    order = [primary] + [key for key in keys if key != primary]
    result = result.sort_values(order, ascending=[ascending] + [True] * (len(order) - 1), na_position="last", kind="stable")
    return display_store_frame(result.rename(columns=LABELS))


def report_pdf(frame, start, end, view, filters, measure=None, table_only=False, logo_path=None):
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_RIGHT
    from reportlab.lib.pagesizes import A3, landscape
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.utils import ImageReader
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, LongTable, TableStyle

    output = BytesIO()
    pagesize = landscape(A3)
    logo_file = Path(logo_path) if logo_path is not None else Path(__file__).resolve().parent / "assets" / "prashanti-logo.png"
    logo = ImageReader(BytesIO(logo_file.read_bytes())) if logo_file.is_file() else None
    page_width = pagesize[0] - 48
    styles = getSampleStyleSheet()
    text = ParagraphStyle("Report cell", fontName="Helvetica", fontSize=7, leading=9, splitLongWords=True)
    heading = ParagraphStyle("Report heading", parent=text, textColor=colors.white, fontName="Helvetica-Bold")
    numeric = ParagraphStyle("Report amount", parent=text, alignment=TA_RIGHT)
    content = [Paragraph("Inventory purchase report", styles["Title"]),
               Paragraph(escape(f"{start:%d %b %Y} - {end:%d %b %Y} | {view}"), styles["Normal"])]
    if measure:
        content.append(Paragraph(escape(f"Measure: {measure}"), styles["Normal"]))
    for label, values in filters.items():
        content.append(Paragraph(escape(f"{label}: {', '.join(display_store_name(value) for value in values) if values else 'All'}"), styles["Normal"]))
    content += [Paragraph("Sales, inward, outgoing and returns use the selected period. Available and intransit are current snapshots. Currency: INR.", styles["Normal"]), Spacer(1, 12)]
    if table_only:
        content = []
    dimension_columns = [column for column in frame if not pd.api.types.is_numeric_dtype(frame[column])]
    weights = [2.8 if column in {"Category", "Store"} else 1.8 if column in dimension_columns else 1 for column in frame]
    widths = [page_width * weight / sum(weights) for weight in weights]
    data = [[Paragraph(escape(str(column)), heading) for column in frame]]
    for _, record in frame.iterrows():
        cells = []
        for column, value in record.items():
            numeric_cell = column not in dimension_columns and pd.api.types.is_numeric_dtype(frame[column])
            value_text = "(Not recorded)" if pd.isna(value) else f"{value:,.2f}" if numeric_cell else str(value)
            escaped_text = escape(value_text)
            if table_only and not numeric_cell:
                padding = len(value_text) - len(value_text.lstrip())
                escaped_text = "&#160;" * padding + escape(value_text.lstrip())
            cells.append(Paragraph(escaped_text, numeric if numeric_cell else text))
        data.append(cells)
    # One total row, calculated only from the fact rows, not hierarchy subtotals.
    totals = []
    for index, column in enumerate(frame):
        value = "Grand total" if index == 0 else f"{frame[column].sum():,.2f}" if pd.api.types.is_numeric_dtype(frame[column]) else ""
        totals.append(Paragraph(escape(value), numeric if pd.api.types.is_numeric_dtype(frame[column]) else text))
    if not table_only:
        data.insert(1, totals)
    table = LongTable(data, colWidths=widths, repeatRows=1, hAlign="LEFT")
    table.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#560835")),
                               ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#dddddd")),
                               ("VALIGN", (0, 0), (-1, -1), "TOP"),
                               ("LEFTPADDING", (0, 0), (-1, -1), 5),
                               ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                               ("BACKGROUND", (0, 1), (-1, 1), colors.HexColor("#f2eeee"))]))
    content.append(table)
    def footer(canvas, doc):
        canvas.saveState()
        if logo is not None:
            # The website's white logo needs its burgundy background for contrast.
            logo_width, logo_height = logo.getSize()
            width = 132
            height = width * logo_height / logo_width
            x = pagesize[0] - 24 - width - 12
            y = pagesize[1] - 18 - height - 12
            canvas.setFillColor(colors.HexColor("#560835"))
            canvas.roundRect(x, y, width + 12, height + 12, 3, fill=1, stroke=0)
            canvas.drawImage(logo, x + 6, y + 6, width=width, height=height, mask="auto")
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(pagesize[0] - 24, 14, f"Page {doc.page}")
        canvas.restoreState()
    SimpleDocTemplate(output, pagesize=pagesize, leftMargin=24, rightMargin=24,
                      topMargin=78 if logo is not None else 24, bottomMargin=30).build(content, onFirstPage=footer, onLaterPages=footer)
    return output.getvalue()


st.set_page_config(
    page_title="Vendor / Category Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================================================
# Secrets
# =====================================================================

def read_allowed_emails(path):
    """Read the administrator-owned allowlist afresh on every app rerun."""
    frame = pd.read_excel(path, dtype=str, keep_default_na=False, engine="openpyxl")
    columns = {str(column).strip().casefold(): column for column in frame.columns}
    if "email" not in columns:
        raise ValueError("The access workbook needs an email column.")
    return {
        value.strip().casefold()
        for value in frame[columns["email"]]
        if value.strip()
    }


def require_google_access():
    """Stop execution before any report data is loaded unless access is granted."""
    try:
        auth = st.secrets.get("auth", {})
        google = auth.get("google", {})
        configured = all(auth.get(key) for key in ("redirect_uri", "cookie_secret")) and all(
            google.get(key) for key in ("client_id", "client_secret", "server_metadata_url")
        )
        access = st.secrets.get("access", {})
    except Exception:
        configured, access = False, {}

    if not configured or not st.user.get("is_logged_in", False):
        _, center, _ = st.columns([1, 2, 1])
        with center:
            st.space("large")
            with st.container(border=True):
                st.caption("PRASHANTI · INVENTORY & PURCHASE")
                st.title("Welcome back")
                st.write("Sign in with your approved Google account to view your reports.")
                if st.button("Sign in with Google", type="primary", width="stretch"):
                    if configured:
                        st.login("google")
                    else:
                        st.error("Google sign-in is unavailable because the app's authentication settings are incomplete. Please contact the administrator.")
                st.caption("Access is available to approved users only.")
        st.stop()

    email = str(st.user.get("email", "")).strip().casefold()
    verified = st.user.get("email_verified") is True
    google_issuer = st.user.get("iss") in ("https://accounts.google.com", "accounts.google.com")
    workbook = Path(access.get("allowed_users_file", "allowed_users.xlsx"))
    if not workbook.is_absolute():
        workbook = Path(__file__).resolve().parent / workbook
    try:
        allowed = read_allowed_emails(workbook)
    except (OSError, ValueError, ImportError, zipfile.BadZipFile):
        st.error("User access could not be checked. Please contact the administrator.")
        if st.button("Sign out", key="access_config_logout"):
            st.logout()
        st.stop()
    if not email or not verified or not google_issuer or email not in allowed:
        st.title("Access restricted")
        st.error("Your account is not authorized. Please contact the administrator.")
        if st.button("Sign out", key="access_denied_logout"):
            st.logout()
        st.stop()
    with st.sidebar:
        st.caption(f"Signed in as {email}")
        if st.button("Sign out", key="account_logout"):
            st.session_state.clear()
            st.logout()


require_google_access()


def _get_secret(key):
    try:
        return st.secrets[key]
    except Exception:
        return ""


SQL_ENDPOINT = _get_secret("SQL_ENDPOINT")
DATABASE = _get_secret("DATABASE")
TENANT_ID = _get_secret("TENANT_ID")
CLIENT_ID = _get_secret("CLIENT_ID")
CLIENT_SECRET = _get_secret("CLIENT_SECRET")

EXCLUDED_COMPANIES = [
    "Saree Trails",
    "Wedtree eStore Private Limited - HO",
]
COMPANIES_HIDDEN_FROM_FILTER = [
    "Wedtree eStore Private Limited - Online",
]
NONE_VENDOR_LABEL = "None"

# Image pipeline tuning
DATA_TTL_SECONDS = 3600          # image bytes cache lifetime
MAX_WORKERS = 8                  # parallel OneLake image fetches
ONELAKE_ACCOUNT_URL = "https://onelake.dfs.fabric.microsoft.com"

# How long a date-range fetch stays valid in memory before a fresh
# "Fetch Data" click is forced to re-query the Lakehouse.
AGGREGATE_CACHE_TTL_SECONDS = 6 * 60 * 60  # 6 hours

# =====================================================================
# Styling (merged: hero/report-table theme + drill-through card theme)
# =====================================================================

st.markdown(
    """
    <style>

    :root {
        --st-bg: #ffffff;
        --st-bg2: #ffffff;
        --st-text: #202124;
        --muted: rgba(32,33,36,.65);
        --gold: #c9a24b;
        --on-accent: #ffffff;
        --accent-chip: #e5dbed;
        --wine: #560835;
        --wine2: #850c52;
        --card-border: #dddddd;
        --card-bg: #ffffff;
        --card-shadow: rgba(32,33,36,.08);
        --sidebar-border: #d4c3e1;
        --select-border: #dddddd;
        --pill-border: #d4c3e1;
        --notice-bg-1: #ffffff;
        --notice-bg-2: #ffffff;
        --notice-border: #dddddd;
        --notice-title: #202124;
        --notice-text: #202124;
        --hero-shadow: rgba(32,33,36,.28);
        --stock-bg: #ffffff;
        --stock-fg: #202124;
        --border: #dddddd;
    }

    html, body, .stApp {
        font-family: sans-serif;
    }

    .stApp {
        background: linear-gradient(180deg, var(--st-bg) 0%, var(--st-bg2) 100%);
        color: var(--st-text);
    }

    #MainMenu, footer { visibility: hidden; height: 0; }

    header[data-testid="stHeader"] { background: transparent; }

    section[data-testid="stSidebar"],
    section[data-testid="stSidebar"] > div {
        visibility: visible !important;
        display: block !important;
        background: linear-gradient(180deg, var(--st-bg) 0%, var(--st-bg2) 100%);
        border-right: 1px solid var(--sidebar-border);
        color: var(--st-text);
    }

    [data-testid="collapsedControl"] {
        visibility: visible !important;
        display: flex !important;
        color: #202124 !important;
        z-index: 999999 !important;
    }

    .block-container {
        padding-top: 4.5rem;
        padding-bottom: 3rem;
        max-width: 1450px;
    }

    .hero, .drill-hero {
        background: linear-gradient(120deg,#ffffff 0%,#ffffff 55%,#ffffff 100%);
        border-radius: 20px;
        padding: 26px 32px;
        margin-bottom: 22px;
        box-shadow: 0 10px 30px var(--hero-shadow);
        position: relative;
        overflow: hidden;
        color: #202124;
    }

    .hero h1, .drill-hero h1 {
        font-family: inherit;
        color: #202124;
        font-size: 2.1rem;
        font-weight: 800;
        margin: 0 0 4px 0;
    }

    .hero p, .drill-hero p {
        color: #202124;
        font-size: 14px;
        margin: 0;
        font-weight: 500;
    }

    .hero .rule {
        width: 64px;
        height: 3px;
        border-radius: 3px;
        background: linear-gradient(90deg,#ffffff,#ffffff);
        margin-top: 12px;
    }

    [data-baseweb="tag"] {
        background: var(--accent-chip) !important;
        border-radius: 8px !important;
    }

    [data-baseweb="tag"] span {
        color: var(--on-accent) !important;
    }

    div[data-baseweb="select"] > div {
        border-radius: 10px !important;
        border-color: var(--select-border) !important;
        background: var(--st-bg2) !important;
        color: var(--st-text) !important;
    }

    .stButton > button {
        border-radius: 12px !important;
        font-weight: 600 !important;
        transition: transform .15s ease, box-shadow .15s ease !important;
        border: 1px solid var(--select-border) !important;
        color: var(--st-text) !important;
        background: var(--st-bg2) !important;
    }

    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 14px var(--card-shadow);
    }

    button[kind="primary"] {
        background: linear-gradient(120deg,#ffffff,#ffffff) !important;
        border: 1px solid #ffffff !important;
        color: #202124 !important;
    }

    [data-testid="stDownloadButton"] button {
        border-radius: 12px !important;
        background: linear-gradient(120deg,#ffffff,#ffffff) !important;
        color: #202124 !important;
        border: 1px solid #dddddd !important;
        font-weight: 700 !important;
    }

    [data-testid="stMetric"] {
        background: var(--st-bg2);
        border: 1px solid var(--card-border);
        border-left: 4px solid var(--gold);
        border-radius: 14px;
        padding: 16px 18px;
        box-shadow: 0 3px 10px var(--card-shadow);
    }

    [data-testid="stMetricLabel"] {
        font-weight: 600;
        color: var(--st-text);
        opacity: .62;
        text-transform: uppercase;
        font-size: 14px;
        letter-spacing: .06em;
    }

    [data-testid="stMetricValue"] {
        color: var(--st-text);
        font-family: inherit;
        font-weight: 700;
    }

    .filter-pill-row span.pill,
    .context-row .context-chip {
        display: inline-block;
        background: var(--st-bg2);
        color: var(--st-text);
        border: 1px solid var(--pill-border);
        border-radius: 999px;
        padding: 3px 12px;
        font-size: 14px;
        margin: 2px 6px 2px 0;
        font-weight: 600;
    }

    .context-row { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0 20px; }
    .context-row .context-chip { margin: 0; font-size: 14px; padding: 7px 12px; }

    .notice-card, .summary-box {
        display:flex;
        align-items:flex-start;
        gap:14px;
        background:linear-gradient(135deg,var(--notice-bg-1),var(--notice-bg-2));
        border:1px solid var(--notice-border);
        border-left:5px solid var(--gold);
        border-radius:14px;
        padding:14px 18px;
        margin:6px 0 20px 0;
        box-shadow:0 2px 8px var(--card-shadow);
    }

    .summary-box { display: block; }
    .summary-box strong { color: var(--wine); }

    .notice-icon { font-size:22px; }
    .notice-title {
        font-weight:700;
        color:var(--notice-title);
        font-size: 14px;
    }
    .notice-body {
        color:var(--notice-text);
        font-size: 14px;
        margin-top:2px;
    }

    .cache-badge {
        display:inline-flex;
        align-items:center;
        gap:6px;
        background: var(--st-bg2);
        border: 1px solid var(--card-border);
        border-radius: 999px;
        padding: 4px 12px;
        font-size: 14px;
        font-weight: 600;
        color: var(--st-text);
        opacity: .85;
        margin: 4px 0 14px 0;
    }

    .report-table-wrap {
        border: 1px solid var(--card-border);
        border-radius: 14px;
        overflow: auto;
        box-shadow: 0 4px 14px var(--card-shadow);
        margin-top: 4px;
    }

    table.report-table {
        width: 100%;
        border-collapse: collapse;
        min-width: 1100px;
        background: var(--st-bg2);
    }

    .report-table th {
        position: sticky;
        top: 0;
        z-index: 1;
        background: #ffffff;
        color: #202124;
        padding: 12px 13px;
        text-align: left;
        font-size: 14px;
        white-space: nowrap;
    }

    .report-table td {
        padding: 11px 13px;
        border-bottom: 1px solid var(--card-border);
        color: var(--st-text);
        white-space: nowrap;
        font-size: 14px;
    }

    .report-table tr:hover td {
        background: #ffffff;
    }

    .qty-link {
        display:inline-block;
        min-width:34px;
        text-align:center;
        padding:4px 9px;
        border-radius:8px;
        background:#ffffff;
        color:#202124 !important;
        text-decoration:none !important;
        font-weight:700;
    }

    .qty-link:hover {
        background:#ffffff;
        box-shadow:0 3px 10px rgba(32,33,36,.25);
    }

    .zero-qty {
        color: var(--st-text);
        opacity:.55;
        font-weight:600;
    }

    .pagination-info {
        display:flex;
        justify-content:space-between;
        align-items:center;
        padding:10px 4px;
        color: var(--st-text);
        opacity:.8;
        font-size: 14px;
        font-weight:600;
    }

    .back-link {
        display: inline-block;
        margin-bottom: 16px;
        padding: 9px 15px;
        border-radius: 11px;
        background: var(--st-bg2);
        border: 1px solid var(--border);
        color: var(--wine) !important;
        text-decoration: none !important;
        font-weight: 700;
    }

    .product-card {
        background: var(--card-bg);
        border: 1px solid var(--card-border);
        border-radius: 17px;
        overflow: hidden;
        margin-bottom: 22px;
        box-shadow: 0 7px 20px var(--card-shadow);
        height: 100%;
        display: flex;
        flex-direction: column;
    }

    .product-image {
        width: 100%;
        height: 245px;
        object-fit: cover;
        display: block;
        background: var(--st-bg2);
    }

    .no-image {
        height: 245px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--st-bg2);
        color: var(--muted);
        font-weight: 700;
        font-size: 14px;
    }

    .card-body {
        padding: 16px 14px 15px;
        color: var(--st-text);
        background: var(--card-bg);
        display: flex;
        flex-direction: column;
        flex: 1;
    }

    .product-title {
        font-size: 17px;
        font-weight: 800;
        margin-bottom: 12px;
        color: var(--st-text);
        line-height: 1.3;
    }

    .product-subtitle {
        font-size: 14px;
        color: var(--muted);
        line-height: 1.55;
        min-height: 41px;
        margin-bottom: 10px;
    }

    .detail-row {
        display: flex;
        justify-content: space-between;
        gap: 10px;
        border-bottom: 1px dashed var(--border);
        padding: 7px 0;
        font-size: 14px;
    }

    .detail-label { color: var(--muted); }

    .detail-value {
        color: var(--st-text);
        font-weight: 700;
        text-align: right;
        max-width: 65%;
        overflow-wrap: anywhere;
    }

    .price-row {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-top: 15px;
    }

    .price {
        color: var(--wine2);
        font-size: 14px;
        font-weight: 800;
    }

    .stock-badge {
        background: var(--stock-bg);
        color: var(--stock-fg);
        border-radius: 999px;
        padding: 7px 10px;
        font-size: 14px;
        font-weight: 800;
        white-space: nowrap;
    }

    .cost {
        margin-top: 5px;
        font-size: 14px;
        color: var(--wine2);
        opacity: .85;
    }

    /* Native widgets and portal-mounted popovers share the white surfaces. */
    :root { color-scheme: light; }
    [data-testid="stHeader"],
    [data-testid="stPopover"] button,
    [data-testid="stPopoverBody"],
    [data-testid="stDateInput"] input,
    [data-baseweb="popover"] > div,
    [data-baseweb="calendar"],
    [data-baseweb="menu"],
    [data-baseweb="tag"],
    [data-testid="stAlert"],
    [data-testid="stExpander"] details,
    [data-testid="stNumberInput"] input,
    [data-testid="stNumberInput"] button {
        background: #ffffff !important;
        color: #202124 !important;
    }
    [data-testid="stPopover"] button {
        border: 1px solid #dddddd !important;
        border-radius: 12px;
    }
    [data-testid="stPopover"] button p { color: #202124 !important; }
    /* Streamlit's native select widgets no longer use BaseWeb wrappers. */
    [data-testid="stWidgetLabel"],
    [data-testid="stWidgetLabel"] p,
    [data-testid="stCaptionContainer"],
    [data-testid="stCaptionContainer"] p {
        color: #202124 !important;
    }
    [data-testid="stSelectbox"] button,
    [data-testid="stSelectbox"] [role="combobox"],
    [data-testid="stMultiSelect"] > div,
    [data-testid="stMultiSelect"] [role="combobox"],
    [data-testid="stMultiSelectTagsContainer"],
    [data-testid="stSelectboxVirtualDropdown"],
    [data-testid="stMultiSelectDropdown"],
    [data-testid="stSelectboxVirtualDropdown"] [role="option"],
    [data-testid="stMultiSelectDropdown"] [role="option"] {
        background: #ffffff !important;
        color: #202124 !important;
        border-color: #dddddd !important;
    }
    [data-testid="stSelectbox"] button *,
    [data-testid="stMultiSelect"] *,
    [data-testid="stSelectboxVirtualDropdown"] *,
    [data-testid="stMultiSelectDropdown"] * {
        color: #202124 !important;
    }
    [data-testid="stMultiSelect"] input {
        background: #ffffff !important;
        color: #202124 !important;
        caret-color: #202124;
    }
    [data-testid="stMultiSelect"] input::placeholder {
        color: #666666 !important;
        opacity: 1;
    }
    [data-testid="stSelectboxVirtualDropdown"] [role="option"][data-focused],
    [data-testid="stMultiSelectDropdown"] [role="option"][data-focused] {
        outline: 1px solid #999999;
        outline-offset: -1px;
    }

    /* Prashanti Sarees palette; preserve Streamlit's default font. */
    [data-testid="stMultiSelect"] div:has(> [data-testid="stMultiSelectTagsContainer"]) {
        background: #ffffff !important;
        border-color: #d4c3e1 !important;
        color: #202124 !important;
    }
    [data-testid="stMultiSelect"] button,
    [data-testid="stMultiSelect"] [role="combobox"] {
        background: #ffffff !important;
        color: #202124 !important;
        color-scheme: light;
    }
    [data-testid="stMultiSelect"] svg { color: #560835 !important; }
    .stApp h1, .stApp h2, .stApp h3 { color: #560835; }
    .stApp a { color: #560835; }
    [data-testid="stDownloadButton"] button {
        border-color: #560835;
        color: #560835;
    }
    .report-table th { background: #560835 !important; color: #ffffff !important; }
    .report-table .grand-total td { background: #FBF6EC; border-bottom-color: #c9a24b; }
    [data-testid="stMultiSelectTagsContainer"] { height: auto !important; max-height: none !important; }
    [data-testid="stMultiSelect"] [data-baseweb="tag"],
    [data-testid="stMultiSelect"] [data-tag] {
        height: auto !important;
        max-width: 100%;
        background: #e5dbed !important;
    }
    [data-testid="stMultiSelect"] [data-baseweb="tag"] span,
    [data-testid="stMultiSelect"] [data-tag] [title] {
        white-space: normal !important;
        overflow-wrap: anywhere;
        text-overflow: clip !important;
        max-height: none !important;
    }
    [data-testid="stMultiSelectDropdown"] [role="option"] {
        white-space: normal !important;
        overflow-wrap: anywhere;
        height: auto !important;
        min-height: 2.5rem;
    }

    /* One body size across native widgets and custom report/card content. */
    .stApp p,
    .stApp label,
    .stApp input,
    .stApp textarea,
    .stApp button,
    .stApp button p,
    .stApp summary,
    [data-testid="stWidgetLabel"] p,
    [data-testid="stCaptionContainer"] p,
    [data-testid="stMetricLabel"] p,
    [data-testid="stMetricValue"],
    [data-testid="stMetricValue"] div,
    [data-testid="stSelectbox"] *,
    [data-testid="stMultiSelect"] *,
    [data-testid="stPopoverBody"] p,
    [data-testid="stPopoverBody"] label,
    [data-testid="stPopoverBody"] button,
    [data-testid="stSelectboxVirtualDropdown"] *,
    [data-testid="stMultiSelectDropdown"] *,
    [data-baseweb="popover"] [role="option"] *,
    [data-baseweb="calendar"] button,
    .report-table td,
    .product-subtitle,
    .detail-row,
    .price,
    .stock-badge,
    .cost,
    .back-link {
        font-size: 14px !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# =====================================================================
# SQL connection
# =====================================================================

def get_sql_server_driver():
    installed = pyodbc.drivers()
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for name in preferred:
        if name in installed:
            return name
    for name in installed:
        if "SQL Server" in name:
            return name
    return None


def build_connection_string(server, db, client_id_, client_secret_, tenant_id_):
    driver = get_sql_server_driver()
    if not driver:
        raise RuntimeError(
            f"No SQL Server ODBC driver found. Drivers: {pyodbc.drivers() or 'none'}"
        )

    return (
        f"Driver={{{driver}}};"
        f"Server={server},1433;"
        f"Database={db};"
        f"UID={client_id_}@{tenant_id_};"
        f"PWD={client_secret_};"
        "Authentication=ActiveDirectoryServicePrincipal;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=60;"
    )


def connection_is_configured():
    return all([
        SQL_ENDPOINT,
        DATABASE,
        TENANT_ID,
        CLIENT_ID,
        CLIENT_SECRET,
    ])


@st.cache_resource(show_spinner=False)
def get_connection(conn_str):
    return pyodbc.connect(conn_str, autocommit=True)


def get_live_connection(conn_str):
    conn = get_connection(conn_str)
    try:
        conn.cursor().execute("SELECT 1")
        return conn
    except Exception:
        get_connection.clear()
        return get_connection(conn_str)


@st.cache_data(ttl=600, show_spinner=False)
def run_query(conn_str, sql, params):
    conn = get_live_connection(conn_str)
    cursor = conn.cursor()
    cursor.execute(sql, params)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame.from_records(rows, columns=columns)


@st.cache_data(ttl=AGGREGATE_CACHE_TTL_SECONDS, max_entries=32, show_spinner=False)
def run_isolated_report_query(conn_str, sql, params):
    """Each cache miss owns its connection; concurrent workers never share ODBC objects."""
    conn = pyodbc.connect(conn_str, autocommit=True)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            columns = [column[0] for column in cursor.description]
            return pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
        finally:
            cursor.close()
    finally:
        conn.close()


def load_report_parallel(conn_str, start, end, reference, _query_runner=None):
    """Fetch the four independent summaries together, then join only in memory."""
    runner = run_isolated_report_query if _query_runner is None else _query_runner
    meta = dict(start_date=start, end_date=end)
    queries = {"Report totals": build_query_and_params(start.isoformat(), end.isoformat()),
               "Sold age": quantity_age_query("sales", meta, reference),
               "Available age": quantity_age_query("available", meta, reference),
               "Transfer ages": transfer_age_query(meta, reference)}
    def fetch(name, query):
        started = time.perf_counter()
        try:
            frame = runner(conn_str, query[0], query[1])
        except Exception as error:
            raise RuntimeError(f"{name} query failed: {error}") from error
        return frame, time.perf_counter() - started
    frames, timings = {}, {}
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="report-fetch") as pool:
        futures = {name: pool.submit(fetch, name, query) for name, query in queries.items()}
        for name, future in futures.items():
            frames[name], timings[name] = future.result()
    started = time.perf_counter()
    result = frames.pop("Report totals").copy()
    keys = ["company_id_name", "Vendor", "categ_id_name"]
    for totals in frames.values():
        columns = [column for column in totals if column.startswith("age_")]
        result = result.merge(totals[keys + columns], on=keys, how="left", validate="one_to_one")
        result[columns] = result[columns].fillna(0)
    timings["Combine results"] = time.perf_counter() - started
    return result, timings


# =====================================================================
# Image loading (OneLake / ADLS Gen2)
# =====================================================================

def get_datalake_service_client():
    """Fresh client each call — avoids stale / expired credentials.
    Uses the same service principal as the SQL connection."""
    credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    return DataLakeServiceClient(
        account_url=ONELAKE_ACCOUNT_URL,
        credential=credential,
    )


def _parse_abfss_path(abfss_path: str):
    """
    abfss://<filesystem>@onelake.dfs.fabric.microsoft.com/<path>
    -> (filesystem, file_path)

    urlparse treats <filesystem> as the netloc's username and the host
    as the netloc's host.
    """
    parsed = urlparse(abfss_path)
    filesystem = parsed.username
    file_path = parsed.path.lstrip("/")
    return filesystem, file_path


def fetch_image_bytes(client, abfss_path: str) -> bytes:
    filesystem, file_path = _parse_abfss_path(abfss_path)
    fs_client = client.get_file_system_client(filesystem)
    file_client = fs_client.get_file_client(file_path)
    downloader = file_client.download_file()
    return downloader.readall()


def load_image(client, image_ref: str):
    """
    Returns (PIL.Image or None, raw_bytes or None, error_message or None).
    Never raises.
    """
    if image_ref is None or (isinstance(image_ref, str) and image_ref in ("", "False")):
        return None, None, "No image reference"

    if not (isinstance(image_ref, str) and image_ref.startswith("abfss://")):
        return None, None, f"Unexpected image reference format: {image_ref!r}"

    try:
        image_bytes = fetch_image_bytes(client, image_ref)
    except Exception as e:
        return None, None, f"{type(e).__name__}: {e}"

    if not image_bytes:
        return None, image_bytes, "Empty file (0 bytes)"

    try:
        img = Image.open(io.BytesIO(image_bytes))
        img.load()
        return img, image_bytes, None
    except Exception as e:
        return None, image_bytes, f"{type(e).__name__}: {e}"


@st.cache_data(ttl=DATA_TTL_SECONDS, show_spinner=False)
def load_image_cached(_client, image_ref):
    # Leading underscore tells Streamlit not to hash the client object.
    return load_image(_client, image_ref)


def load_images_parallel(client, refs):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        return list(ex.map(lambda r: load_image_cached(client, r), refs))


def image_to_data_uri(img: Image.Image, fmt: str = "JPEG", quality: int = 82) -> str:
    """PIL image -> data:image/jpeg;base64,... so it can be embedded in
    the markdown HTML the cards already build."""
    buf = io.BytesIO()
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")
    img.save(buf, format=fmt, quality=quality)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/{fmt.lower()};base64,{b64}"


# =====================================================================
# Main report SQL
#
# NOTE: This is now filtered by DATE RANGE ONLY. Vendor / Category /
# Company are intentionally NOT part of the WHERE clause anymore —
# they're applied afterwards, in pandas, on the cached result (see
# filter_report_df / get_aggregate_data below). This keeps the query
# shape (and therefore the cache key) stable across filter tweaks.
# =====================================================================

HO_COMPANY = "Wedtree eStore Private Limited - HO"
STORE_COMPANIES = tuple("Wedtree eStore Private Limited - " + name for name in
                        ("Coimbatore", "Hyderabad", "Jayanagar", "Malleshwaram", "T Nagar", "Vizag"))
NETWORK_COMPANIES_SQL = ", ".join("'" + name + "'" for name in (HO_COMPANY,) + STORE_COMPANIES)
# EXCLUDED_STOCK_LOCATIONS_SQL = "'Physical Locations/Subcontracting Location', 'Physical Locations/Inter-warehouse transit', 'Virtual Locations/Production'"
EXCLUDED_STOCK_LOCATIONS_SQL = "'Physical Locations/Subcontractors','CBWH/Damage','HYDWH/Stock/Damage','JAYWH/Warehouse/Damage','MLMWH/warehouse/DAMAGE','TNWH/Warehouse/DAMAGE','HYD/Stock/CONSUMABLES','JYR/Stock/CONSUMABLE','MLM/Stock/CONSUMABLE','Prash/Stock/CONSUMABLE','TN/Stock/CONSUMABLE','VZG/Stock/CONSUMABLE'"

def sql_relation_key(expression):
    """Normalize the integer and [id, 'name'] relation encodings in the supplied SQL."""
    value = f"CAST({expression} AS VARCHAR(MAX))"
    return (f"CASE WHEN LEFT({value}, 1) = '[' THEN LTRIM(RTRIM(SUBSTRING({value}, 2, "
            f"CHARINDEX(',', {value} + ',') - 2))) ELSE NULLIF(CAST({expression} AS VARCHAR(50)), 'False') END")


PICKING_PARTNER_CTE = """,
pk AS (
    SELECT
        sp.*,
        CASE
            WHEN CHARINDEX('''', CAST(sp.partner_id AS VARCHAR(MAX))) > 0
             AND CHARINDEX(
                    '''',
                    CAST(sp.partner_id AS VARCHAR(MAX)),
                    CHARINDEX('''', CAST(sp.partner_id AS VARCHAR(MAX))) + 1
                 ) > CHARINDEX('''', CAST(sp.partner_id AS VARCHAR(MAX)))
            THEN LTRIM(RTRIM(
                SUBSTRING(
                    CAST(sp.partner_id AS VARCHAR(MAX)),
                    CHARINDEX('''', CAST(sp.partner_id AS VARCHAR(MAX))) + 1,
                    CHARINDEX(
                        '''',
                        CAST(sp.partner_id AS VARCHAR(MAX)),
                        CHARINDEX('''', CAST(sp.partner_id AS VARCHAR(MAX))) + 1
                    ) - CHARINDEX('''', CAST(sp.partner_id AS VARCHAR(MAX))) - 1
                )
            ))
            ELSE NULL
        END AS partner_name
    FROM WT_LH_Silver.Odoo.stock_picking sp
)"""

TRANSFER_CTES = PICKING_PARTNER_CTE + f""",
normalized_move_lines AS (
    SELECT ml.*,
        {sql_relation_key('ml.picking_id')} AS picking_key,
        {sql_relation_key('ml.product_id')} AS product_key
    FROM WT_LH_Silver.Odoo.stock_move_line ml
),
transfer_facts AS (
    -- Completed inbound receipts and ready inbound transfers: supplied PO-line formulas.
    SELECT CAST(pk.id AS VARCHAR(50)) AS picking_id,
        pk.company_id_name AS company,
        CAST(pro.id AS VARCHAR(50)) AS product_id,
        pro.categ_id_name AS category,
        CASE WHEN pk.state = 'done' THEN 'inward' ELSE 'intransit' END AS kind,
        CAST(pk.date_done AS DATE) AS event_date,
        SUM(CASE WHEN pk.state = 'done' THEN pol.qty_received
                 ELSE pol.product_qty - pol.qty_received END) AS quantity,
        SUM((CASE WHEN pk.state = 'done' THEN pol.qty_received
                  ELSE pol.product_qty - pol.qty_received END) * pro.lst_price) AS value
    FROM pk
    LEFT JOIN WT_LH_Silver.Odoo.purchase_order po ON pk.origin = po.name
    LEFT JOIN WT_LH_Silver.Odoo.purchase_order_line pol ON po.id = pol.order_id
    LEFT JOIN WT_LH_Silver.Odoo.product_product pro ON CAST(pol.product_id AS VARCHAR(50)) = CAST(pro.id AS VARCHAR(50))
    CROSS JOIN report_dates d
    WHERE pk.picking_type_code = 'incoming'
      AND pk.state IN ('done', 'assigned')
      AND pk.company_id_name IN ({NETWORK_COMPANIES_SQL})
      AND pk.partner_name IN ({NETWORK_COMPANIES_SQL})
      AND pk.partner_name <> pk.company_id_name
      AND ((pk.state = 'done' AND CAST(pk.date_done AS DATE) BETWEEN d.start_date AND d.end_date)
           OR (pk.state = 'assigned' AND pol.product_qty - pol.qty_received > 0))
      AND LOWER(pro.categ_id_name) NOT LIKE '%admin%'
    GROUP BY CAST(pk.id AS VARCHAR(50)), pk.company_id_name, CAST(pro.id AS VARCHAR(50)),
             pro.categ_id_name, pk.state, CAST(pk.date_done AS DATE)
    UNION ALL
    -- Completed non-POS outgoing transfers: supplied delivered SO-line quantity.
    SELECT CAST(pk.id AS VARCHAR(50)), pk.company_id_name, CAST(pro.id AS VARCHAR(50)),
        pro.categ_id_name, 'outgoing', CAST(pk.date_done AS DATE),
        SUM(sol.qty_delivered), SUM(sol.qty_delivered * pro.lst_price)
    FROM pk
    LEFT JOIN WT_LH_Silver.Odoo.sale_order so ON pk.origin = so.name
    LEFT JOIN WT_LH_Silver.Odoo.sale_order_line sol ON so.id = sol.order_id
    LEFT JOIN WT_LH_Silver.Odoo.product_product pro ON CAST(sol.product_id AS VARCHAR(50)) = CAST(pro.id AS VARCHAR(50))
    CROSS JOIN report_dates d
    WHERE pk.picking_type_code = 'outgoing' AND pk.state = 'done'
      AND pk.name LIKE '%/OUT/%' AND UPPER(pk.name) NOT LIKE '%POS%'
      AND pk.company_id_name IN ({NETWORK_COMPANIES_SQL})
      AND pk.partner_name IN ({NETWORK_COMPANIES_SQL})
      AND pk.partner_name <> pk.company_id_name
      AND CAST(pk.date_done AS DATE) BETWEEN d.start_date AND d.end_date
      AND LOWER(pro.categ_id_name) NOT LIKE '%admin%'
    GROUP BY CAST(pk.id AS VARCHAR(50)), pk.company_id_name, CAST(pro.id AS VARCHAR(50)),
             pro.categ_id_name, CAST(pk.date_done AS DATE)
),
return_facts AS (
    SELECT pk.company_id_name AS company, CAST(pro.id AS VARCHAR(50)) AS product_id,
        pro.categ_id_name AS category,
        SUM(ml.quantity) AS quantity, SUM(ml.quantity * pro.lst_price) AS value
    FROM pk
    INNER JOIN normalized_move_lines ml ON ml.picking_key = CAST(pk.id AS VARCHAR(50)) AND ml.state = 'done'
    LEFT JOIN WT_LH_Silver.Odoo.product_product pro ON ml.product_key = CAST(pro.id AS VARCHAR(50))
    CROSS JOIN report_dates d
    WHERE pk.picking_type_code = 'outgoing' AND pk.state = 'done'
      AND pk.name LIKE '%/OUT/%' AND UPPER(pk.name) NOT LIKE '%POS%'
      AND pk.company_id_name = '{HO_COMPANY}'
      AND pk.location_dest_id_name = 'Partners/Vendors'
      AND CAST(pk.date_done AS DATE) BETWEEN d.start_date AND d.end_date
      AND LOWER(pro.categ_id_name) NOT LIKE '%admin%'
    GROUP BY pk.company_id_name, CAST(pro.id AS VARCHAR(50)), pro.categ_id_name
)
"""

QUERY_TEMPLATE = """WITH report_dates AS (SELECT CAST(? AS DATE) AS start_date, CAST(? AS DATE) AS end_date),
product_master AS (
    SELECT
        CAST(p.id AS VARCHAR(50)) AS product_id,
        pt.vendor_id_name AS product_vendor,
        p.categ_id_name AS category,
        p.lst_price
    FROM WT_LH_Silver.Odoo.product_product p
    LEFT JOIN WT_LH_Silver.Odoo.product_template pt
        ON p.product_variant_id = pt.product_variant_id
),

sales_data AS (
    SELECT
        CAST(pol.id AS VARCHAR(50)) AS pol_id,
        CAST(po.id AS VARCHAR(50)) AS pos_id,
        pol.product_id,
        pol.qty,
        pol.price_subtotal_incl,
        pol.company_id_name
    FROM WT_LH_Silver.Odoo.pos_order_line pol
    INNER JOIN WT_LH_Silver.Odoo.pos_order po
        ON CAST(po.id AS VARCHAR(50)) = CAST(pol.order_id AS VARCHAR(50))
    CROSS JOIN report_dates d
    WHERE CAST(po.date_order AS DATE) >= d.start_date
      AND CAST(po.date_order AS DATE) <= d.end_date
      AND po.user_id_name <> 'Administrator'
      AND pol.company_id_name NOT IN (
            'Saree Trails',
            'Wedtree Estore private Limited - Saree Trails',
            'Wedtree eStore Private Limited - Online',
            'Wedtree eStore Private Limited - HO'
      )
      AND po.config_id_name NOT IN (
            'CB BILLING 3 (not used)',
            'MLM Billing 3 (not used)',
            'JYR Billing 3 (not used)',
            'TN BILLING 4 (not used)',
            'HYD BILLING - 4 (not used)',
            'Vizag Billing 3 (not used)'
      )
),

sales_summary AS (
    SELECT
        sd.company_id_name AS company,
        pm.product_vendor AS vendor,
        pm.category,
        SUM(sd.qty) AS sale_qty,
        SUM(sd.price_subtotal_incl) AS sale_value
    FROM sales_data sd
    LEFT JOIN product_master pm
        ON CAST(sd.product_id AS VARCHAR(50)) = pm.product_id
    WHERE pm.category IS NOT NULL
      AND LOWER(pm.category) NOT LIKE '%admin%'
    GROUP BY
        sd.company_id_name,
        pm.product_vendor,
        pm.category
)""" + TRANSFER_CTES + f""",
inventory_summary AS (
    SELECT sq.company_id_name AS company, pm.product_vendor AS vendor, pm.category,
        SUM(sq.quantity) AS available_qty, SUM(sq.quantity * pm.lst_price) AS available_value
    FROM WT_LH_Silver.Odoo.stock_quant_n1 sq
    LEFT JOIN product_master pm ON CAST(sq.product_id AS VARCHAR(50)) = pm.product_id
    WHERE sq.company_id_name NOT IN ('Saree Trails', 'Wedtree Estore private Limited - Saree Trails', 'Wedtree eStore Private Limited - Online')
      AND sq.location_id_name NOT IN ({EXCLUDED_STOCK_LOCATIONS_SQL})
    GROUP BY sq.company_id_name, pm.product_vendor, pm.category
),
metric_facts AS (
    SELECT company, vendor, category, sale_qty, sale_value, 0 AS available_qty, 0 AS available_value,
           0 AS inward_qty, 0 AS inward_value, 0 AS intransit_qty, 0 AS intransit_value,
           0 AS outgoing_qty, 0 AS outgoing_value, 0 AS returns_qty, 0 AS returns_value
    FROM sales_summary
    UNION ALL
    SELECT company, vendor, category, 0, 0, available_qty, available_value, 0, 0, 0, 0, 0, 0, 0, 0
    FROM inventory_summary
    UNION ALL
    SELECT f.company, pm.product_vendor, f.category, 0, 0, 0, 0,
        CASE WHEN f.kind = 'inward' THEN f.quantity ELSE 0 END,
        CASE WHEN f.kind = 'inward' THEN f.value ELSE 0 END,
        CASE WHEN f.kind = 'intransit' THEN f.quantity ELSE 0 END,
        CASE WHEN f.kind = 'intransit' THEN f.value ELSE 0 END,
        CASE WHEN f.kind = 'outgoing' THEN f.quantity ELSE 0 END,
        CASE WHEN f.kind = 'outgoing' THEN f.value ELSE 0 END, 0, 0
    FROM transfer_facts f
    LEFT JOIN product_master pm ON f.product_id = pm.product_id
    UNION ALL
    SELECT f.company, pm.product_vendor, f.category, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, f.quantity, f.value
    FROM return_facts f
    LEFT JOIN product_master pm ON f.product_id = pm.product_id
)
SELECT vendor AS Vendor, category AS categ_id_name, company AS company_id_name,
    SUM(sale_qty) AS sale_qty, SUM(sale_value) AS sale_value,
    SUM(available_qty) AS available_qty, SUM(available_value) AS available_value,
    SUM(inward_qty) AS inward_qty, SUM(inward_value) AS inward_value,
    SUM(intransit_qty) AS intransit_qty, SUM(intransit_value) AS intransit_value,
    SUM(outgoing_qty) AS outgoing_qty, SUM(outgoing_value) AS outgoing_value,
    SUM(returns_qty) AS returns_qty, SUM(returns_value) AS returns_value
FROM metric_facts
GROUP BY vendor, category, company
ORDER BY vendor, category, company;
"""


def build_query_and_params(start_str, end_str):
    """Bind the shared selected period once; inventory and ready receipts are snapshots."""
    params = [start_str, end_str]
    return QUERY_TEMPLATE, params


@st.cache_data(ttl=AGGREGATE_CACHE_TTL_SECONDS, show_spinner=False)
def get_aggregate_data(conn_str, start_str, end_str, query_revision="network-transfers-v2"):
    """Fetch the supplied HO/store metric rules, retaining unmapped master categories. Cached for
    AGGREGATE_CACHE_TTL_SECONDS (6 hours), keyed on the connection
    string + date range + query revision. Every Vendor/Category/Company tweak below
    reuses this exact DataFrame — no new query."""
    sql, params = build_query_and_params(start_str, end_str)
    return run_query(conn_str, sql, params)


def filter_report_df(df, vendors, categories, companies):
    """Apply Vendor / Category / Company filters entirely in pandas,
    against the already-fetched (date-scoped) DataFrame. No SQL."""
    if df is None or df.empty:
        return df

    filtered = df

    if companies:
        filtered = filtered[filtered["company_id_name"].isin(companies)]

    if categories:
        filtered = filtered[filtered["categ_id_name"].isin(categories)]

    if vendors:
        include_none = NONE_VENDOR_LABEL in vendors
        real = [v for v in vendors if v != NONE_VENDOR_LABEL]
        mask = pd.Series(False, index=filtered.index)
        if real:
            mask = mask | filtered["Vendor"].isin(real)
        if include_none:
            mask = mask | filtered["Vendor"].isna()
        filtered = filtered[mask]

    return filtered


def derive_filter_options(df):
    """Vendor/Category/Company choices derived from the fetched
    DataFrame itself (no extra SQL round trip). Naturally scoped to
    whatever actually appears in the selected date range."""
    if df is None or df.empty:
        return [], [NONE_VENDOR_LABEL], []

    categories = sorted(df["categ_id_name"].dropna().unique().tolist())

    vendors = sorted(df["Vendor"].dropna().unique().tolist())
    vendors = [NONE_VENDOR_LABEL] + vendors

    companies = sorted(df["company_id_name"].dropna().unique().tolist())
    companies = [c for c in companies if c not in COMPANIES_HIDDEN_FROM_FILTER]

    return companies, vendors, categories


# =====================================================================
# Drill-through SQL
# =====================================================================

# product_images CTE now:
#  - handles both the malformed string form '[123813, ''NALINA TJRS34'']'
#    and the plain integer-string form '123813'
#  - prefers a real image over blank / 'False' when picking rn=1
PRODUCT_BASE = """
WITH product_base AS (
    SELECT
        CAST(p.id AS VARCHAR(50)) AS product_id,
        p.display_name AS product_name,
        p.categ_id_name AS category,
        p.sku,
        pt.vendor_id_name AS vendor,
        p.lst_price AS sp,
        p.standard_price AS cp
    FROM WT_LH_Silver.Odoo.product_product p
    LEFT JOIN WT_LH_Silver.Odoo.product_template pt
        ON p.product_variant_id = pt.product_variant_id
),
product_images AS (
    SELECT product_id, image_1920
    FROM (
        SELECT
            CASE
                WHEN CAST(product_id AS VARCHAR(MAX)) LIKE '[[]%'
                    THEN
                        CAST(
                            SUBSTRING(
                                CAST(product_id AS VARCHAR(MAX)),
                                2,
                                CHARINDEX(
                                    ',',
                                    CAST(product_id AS VARCHAR(MAX)) + ','
                                ) - 2
                            ) AS VARCHAR(50)
                        )
                ELSE CAST(product_id AS VARCHAR(50))
            END AS product_id,
            CAST(image_1920 AS VARCHAR(MAX)) AS image_1920,
            ROW_NUMBER() OVER (
                PARTITION BY
                    CASE
                        WHEN CAST(product_id AS VARCHAR(MAX)) LIKE '[[]%'
                            THEN
                                CAST(
                                    SUBSTRING(
                                        CAST(product_id AS VARCHAR(MAX)),
                                        2,
                                        CHARINDEX(
                                            ',',
                                            CAST(product_id AS VARCHAR(MAX)) + ','
                                        ) - 2
                                    ) AS VARCHAR(50)
                                )
                        ELSE CAST(product_id AS VARCHAR(50))
                    END
                ORDER BY
                    CASE
                        WHEN image_1920 IS NOT NULL
                         AND CAST(image_1920 AS VARCHAR(MAX)) <> ''
                         AND CAST(image_1920 AS VARCHAR(MAX)) <> 'False'
                        THEN 0
                        ELSE 1
                    END
            ) AS rn
        FROM WT_LH_Bronze.Odoo.product_images
    ) x
    WHERE rn = 1
)
"""

AVAILABLE_SQL = PRODUCT_BASE + """
,
stock_min_date AS (
    SELECT
        CAST(product_id AS VARCHAR(50)) AS product_id,
        lot_id_name AS lot_number,
        MIN(CAST(date AS DATE)) AS stock_move_date
    FROM WT_LH_Silver.Odoo.stock_move_line
    WHERE company_id_name =
          'Wedtree eStore Private Limited - HO'
      AND location_id_name =
          'Partners/Vendors'
    GROUP BY
        CAST(product_id AS VARCHAR(50)),
        lot_id_name
),
inventory_detail AS (
    SELECT
        sq.company_id_name AS company,
        CAST(sq.product_id AS VARCHAR(50)) AS product_id,
        p.sku,
        p.product_name,
        p.vendor,
        p.category,
        p.sp,
        p.cp,
        sq.lot_id_name AS lot_number,
        sq.location_id_name AS location,
        sq.quantity AS available_inventory,
        sq.quantity * p.sp AS available_selling_price,
        smd.stock_move_date,
        DATEDIFF(
            DAY,
            smd.stock_move_date,
            CAST(GETDATE() AS DATE)
        ) AS overall_age
    FROM WT_LH_Silver.Odoo.stock_quant_n1 sq
    LEFT JOIN product_base p
        ON CAST(sq.product_id AS VARCHAR(50)) = p.product_id
    LEFT JOIN stock_min_date smd
        ON CAST(sq.product_id AS VARCHAR(50)) = smd.product_id
       AND sq.lot_id_name = smd.lot_number
    WHERE sq.company_id_name NOT IN (
        'Saree Trails',
        'Wedtree Estore private Limited - Saree Trails',
        'Wedtree eStore Private Limited - Online'
    )
      AND sq.location_id_name NOT IN (
          'Physical Locations/Subcontracting Location',
          'Physical Locations/Inter-warehouse transit',
          'Virtual Locations/Production'
      )
)
SELECT
    i.company,
    i.product_id,
    i.sku,
    i.product_name,
    i.vendor,
    i.category,
    i.sp,
    i.cp,
    i.lot_number,
    i.location,
    i.available_inventory,
    i.available_selling_price,
    i.stock_move_date,
    i.overall_age,
    COALESCE(pi.image_1920, '') AS image_1920
FROM inventory_detail i
LEFT JOIN product_images pi
    ON i.product_id = pi.product_id
WHERE ( (? = '' AND i.company IS NULL)  OR i.company  = ? )
  AND ( (? = '' AND i.vendor  IS NULL)  OR i.vendor   = ? )
  AND ( (? = '' AND i.category IS NULL) OR i.category = ? )
  AND i.available_inventory <> 0
ORDER BY
    CASE
        WHEN pi.image_1920 IS NULL
          OR pi.image_1920 = ''
          OR pi.image_1920 = 'False'
        THEN 1
        ELSE 0
    END,
    i.sku,
    i.lot_number;
"""

STOCK_RECEIPT_CTE = AVAILABLE_SQL[AVAILABLE_SQL.index(",\nstock_min_date AS ("):AVAILABLE_SQL.index(",\ninventory_detail AS (")]

SALES_SQL = PRODUCT_BASE + STOCK_RECEIPT_CTE + """
,
pos_sales AS (
    SELECT
        po.company_id_name AS company,
        CAST(po.id AS VARCHAR(50)) AS order_id,
        CAST(po.date_order AS DATE) AS sold_date,
        CAST(po.picking_ids AS VARCHAR(MAX)) AS picking_ids,
        CAST(pol.product_id AS VARCHAR(50)) AS product_id,
        SUM(pol.qty) AS sale_qty,
        SUM(pol.price_subtotal_incl) AS sale_value
    FROM WT_LH_Silver.Odoo.pos_order_line pol
    INNER JOIN WT_LH_Silver.Odoo.pos_order po
        ON CAST(po.id AS VARCHAR(50)) =
           CAST(pol.order_id AS VARCHAR(50))
    WHERE CAST(po.date_order AS DATE) >= ?
      AND CAST(po.date_order AS DATE) <= ?
      AND po.user_id_name <> 'Administrator'
      AND pol.company_id_name NOT IN (
          'Saree Trails',
          'Wedtree Estore private Limited - Saree Trails',
          'Wedtree eStore Private Limited - Online',
          'Wedtree eStore Private Limited - HO'
      )
      AND po.config_id_name NOT IN (
          'CB BILLING 3 (not used)',
          'MLM Billing 3 (not used)',
          'JYR Billing 3 (not used)',
          'TN BILLING 4 (not used)',
          'HYD BILLING - 4 (not used)',
          'Vizag Billing 3 (not used)'
    )
    GROUP BY
        po.company_id_name,
        CAST(po.id AS VARCHAR(50)),
        CAST(po.date_order AS DATE),
        CAST(po.picking_ids AS VARCHAR(MAX)),
        CAST(pol.product_id AS VARCHAR(50))
),
lot_moves AS (
    SELECT
        CAST(pk.id AS VARCHAR(50)) AS picking_id,
        CAST(sml.product_id AS VARCHAR(50)) AS product_id,
        NULLIF(sml.lot_id_name, 'False') AS lot_number,
        sml.location_id_name AS location,
        SUM(ABS(COALESCE(sml.quantity_product_uom, sml.quantity, 0))) AS lot_qty
    FROM WT_LH_Silver.Odoo.stock_picking pk
    INNER JOIN WT_LH_Silver.Odoo.stock_move_line sml
        ON NULLIF(CAST(sml.picking_id AS VARCHAR(50)), 'False') =
           CAST(pk.id AS VARCHAR(50))
    WHERE pk.state = 'done'
      AND pk.picking_type_code = 'outgoing'
      AND sml.state = 'done'
    GROUP BY
        CAST(pk.id AS VARCHAR(50)),
        CAST(sml.product_id AS VARCHAR(50)),
        NULLIF(sml.lot_id_name, 'False'),
        sml.location_id_name
),
sales_lot_matches AS (
    SELECT ps.*,
        lm.lot_number, lm.location, lm.lot_qty,
        SUM(lm.lot_qty) OVER (PARTITION BY ps.order_id, ps.product_id) AS matched_qty,
        COUNT(*) OVER (PARTITION BY ps.order_id, ps.product_id) AS matched_rows
    FROM pos_sales ps
    LEFT JOIN lot_moves lm
        ON (',' + REPLACE(REPLACE(REPLACE(ps.picking_ids, '[', ''), ']', ''), ' ', '') + ',')
           LIKE '%,' + lm.picking_id + ',%'
       AND lm.product_id = ps.product_id
),
sales_detail AS (
    SELECT company, product_id, sold_date,
        COALESCE(lot_number, 'No lot recorded') AS lot_number,
        COALESCE(location, 'No location recorded') AS location,
        sale_qty * CASE WHEN matched_qty > 0 THEN 1.0 * lot_qty / matched_qty
                        ELSE 1.0 / matched_rows END AS sale_qty,
        sale_value * CASE WHEN matched_qty > 0 THEN 1.0 * lot_qty / matched_qty
                          ELSE 1.0 / matched_rows END AS sale_value
    FROM sales_lot_matches
)
SELECT
    s.company,
    s.product_id,
    p.sku,
    p.product_name,
    p.vendor,
    p.category,
    p.sp,
    p.cp,
    s.lot_number,
    s.location,
    s.sold_date,
    smd.stock_move_date,
    s.sale_qty,
    s.sale_value,
    COALESCE(pi.image_1920, '') AS image_1920
FROM sales_detail s
LEFT JOIN product_base p
    ON s.product_id = p.product_id
LEFT JOIN stock_min_date smd
    ON s.product_id = smd.product_id
   AND s.lot_number = smd.lot_number
LEFT JOIN product_images pi
    ON s.product_id = pi.product_id
WHERE ( (? = '' AND s.company IS NULL)  OR s.company  = ? )
  AND ( (? = '' AND p.vendor  IS NULL)  OR p.vendor   = ? )
  AND ( (? = '' AND p.category IS NULL) OR p.category = ? )
ORDER BY
    s.sale_value DESC,
    p.sku;
"""

# =====================================================================
# Shared small helpers
# =====================================================================

def _clean(v):
    """Normalize None/NaN/'None'/'null' -> '' and strip whitespace."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "null", "nan", ""}:
        return ""
    return s


# Separator used to pack multiselect values into a single query-string
# value. Unlikely to appear in a company/vendor/category name.
_LIST_SEP = "\u241f"


def _encode_list(values):
    return _LIST_SEP.join(values) if values else ""


def _decode_list(s):
    return [v for v in s.split(_LIST_SEP) if v] if s else []


def build_report_state_params(start_date, end_date, companies, vendors, categories):
    """The applied-filter state of the main report, packed so it can be
    carried through a drill-through link and back again. A drill-through
    click is a full page navigation (new Streamlit session), so this has
    to live in the URL rather than st.session_state to survive it."""
    return {
        "r_start": start_date.isoformat(),
        "r_end": end_date.isoformat(),
        "r_companies": _encode_list(companies),
        "r_vendors": _encode_list(vendors),
        "r_categories": _encode_list(categories),
    }


def safe_html(value):
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    text = display_store_name(str(value))
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def make_drill_url(drill_type, row, start_date, end_date, report_state_params=None):
    """Relative URL on THIS SAME app - no second app/port involved.

    report_state_params (r_start/r_end/r_companies/...) are carried along
    so the report's applied filters can be restored when the user clicks
    "Back to Report" - see build_report_state_params().
    """
    params = {
        "drill": drill_type,
        "company":  _clean(row["company_id_name"]),
        "vendor":   _clean(row["Vendor"]),
        "category": _clean(row["categ_id_name"]),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    if report_state_params:
        params.update(report_state_params)
    return f"?{urlencode(params)}"


def render_report_table(df, start_date, end_date, report_state_params=None):
    headers = [
        "Vendor",
        "Category",
        "Sale Qty",
        "Sale Value",
        "Available Qty",
        "Available Value",
        "Inward Qty",
        "Inward Value",
        "Company",
    ]

    html = ['<div class="report-table-wrap"><table class="report-table">']
    html.append("<thead><tr>")
    for h in headers:
        html.append(f"<th>{safe_html(h)}</th>")
    html.append("</tr></thead><tbody>")

    for _, r in df.iterrows():
        sale_qty = r["sale_qty"] if pd.notna(r["sale_qty"]) else 0
        available_qty = r["available_qty"] if pd.notna(r["available_qty"]) else 0

        sale_link = (
            f'<a class="qty-link" href="{safe_html(make_drill_url("sales", r, start_date, end_date, report_state_params))}" target="_self">{int(round(sale_qty))}</a>'
            if float(sale_qty) != 0
            else '<span class="zero-qty">0</span>'
        )

        available_link = (
            f'<a class="qty-link" href="{safe_html(make_drill_url("available", r, start_date, end_date, report_state_params))}" target="_self">{int(round(available_qty))}</a>'
            if float(available_qty) != 0
            else '<span class="zero-qty">0</span>'
        )

        html.append("<tr>")
        html.append(f"<td>{safe_html(r['Vendor'])}</td>")
        html.append(f"<td>{safe_html(r['categ_id_name'])}</td>")
        html.append(f"<td>{sale_link}</td>")
        html.append(f"<td>₹{float(r['sale_value'] or 0):,.2f}</td>")
        html.append(f"<td>{available_link}</td>")
        html.append(f"<td>₹{float(r['available_value'] or 0):,.2f}</td>")
        html.append(f"<td>{int(round(float(r['inward_qty'] or 0)))}</td>")
        html.append(f"<td>₹{float(r['inward_value'] or 0):,.2f}</td>")
        html.append(f"<td>{safe_html(r['company_id_name'])}</td>")
        html.append("</tr>")

    html.append("</tbody></table></div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def image_html(image_value, resolved_data_uri=None):
    """
    image_value       : raw value from the SQL row (e.g. 'abfss://...' or '')
    resolved_data_uri : data:image/...;base64,... produced by
                        load_image_cached + image_to_data_uri, or None
    """
    # Already a browser-usable URL? Use it directly.
    if image_value is not None:
        src = str(image_value).strip()
        if src.startswith(("http://", "https://", "data:image/")):
            return (
                f'<img class="product-image" src="{safe_html(src)}" '
                f'alt="Product image" loading="lazy">'
            )

    # Server-side resolved data URI wins.
    if resolved_data_uri:
        return (
            f'<img class="product-image" src="{resolved_data_uri}" '
            f'alt="Product image" loading="lazy">'
        )

    return '<div class="no-image">No image available</div>'


def _detail_row(label, value):
    return (
        '<div class="detail-row">'
        f'<span class="detail-label">{label}</span>'
        f'<span class="detail-value">{value}</span>'
        '</div>'
    )


def render_available_card(row):
    stock_qty = float(row.get("available_inventory", 0) or 0)
    stock_value = float(row.get("available_selling_price", 0) or 0)
    sp = float(row.get("sp", 0) or 0)
    cp = float(row.get("cp", 0) or 0)

    parts = [
        '<div class="product-card">',
        image_html(row.get("image_1920", ""), row.get("_image_data_uri")),
        '<div class="card-body">',
        f'<div class="product-title">{safe_html(row.get("product_name", ""))}</div>',
        '<div class="product-subtitle">'
        f'{safe_html(row.get("category", ""))}<br>{safe_html(row.get("vendor", ""))}'
        '</div>',
        _detail_row("Master category", safe_html(row.get("master_category", ""))),
        _detail_row("Store", safe_html(row.get("company", ""))),
        _detail_row("SKU", safe_html(row.get("sku", ""))),
        _detail_row("Lot No.", safe_html(row.get("lot_number", ""))),
        _detail_row("Location", safe_html(row.get("location", ""))),
        _detail_row("Age (days)", safe_html(row.get("overall_age", ""))),
        _detail_row("Stock value", f"₹{stock_value:,.0f}"),
        '<div class="price-row">'
        f'<span class="price">₹{sp:,.0f}</span>'
        f'<span class="stock-badge">In stock · {stock_qty:g}</span>'
        '</div>',
        f'<div class="cost">Cost: ₹{cp:,.0f}</div>',
        '</div>',
        '</div>',
    ]
    return "".join(parts)


def render_sales_card(row):
    sale_qty = float(row.get("sale_qty", 0) or 0)
    sale_value = float(row.get("sale_value", 0) or 0)
    sp = float(row.get("sp", 0) or 0)
    cp = float(row.get("cp", 0) or 0)

    parts = [
        '<div class="product-card">',
        image_html(row.get("image_1920", ""), row.get("_image_data_uri")),
        '<div class="card-body">',
        f'<div class="product-title">{safe_html(row.get("product_name", ""))}</div>',
        '<div class="product-subtitle">'
        f'{safe_html(row.get("category", ""))}<br>{safe_html(row.get("vendor", ""))}'
        '</div>',
        _detail_row("Master category", safe_html(row.get("master_category", ""))),
        _detail_row("Store", safe_html(row.get("company", ""))),
        _detail_row("SKU", safe_html(row.get("sku", ""))),
        _detail_row("Lot No.", safe_html(row.get("lot_number", ""))),
        _detail_row("Location", safe_html(row.get("location", ""))),
        _detail_row("Latest sold date", safe_html(row.get("sold_date", ""))),
        _detail_row("Days from inward to sale", safe_html(row.get("sale_age", ""))),
        _detail_row("Sale value", f"₹{sale_value:,.0f}"),
        '<div class="price-row">'
        f'<span class="price">₹{sp:,.0f}</span>'
        f'<span class="stock-badge">Sold · {sale_qty:g}</span>'
        '</div>',
        f'<div class="cost">Cost: ₹{cp:,.0f}</div>',
        '</div>',
        '</div>',
    ]
    return "".join(parts)


def resolve_images_for_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    For every row in df, if image_1920 is an abfss:// reference, fetch the
    bytes from OneLake via the Azure SDK, decode into a PIL image, and
    re-encode as a data:image/...;base64 URI. Adds a '_image_data_uri'
    column. Never raises; rows that fail get None in that column.
    """
    if df.empty:
        df = df.copy()
        df["_image_data_uri"] = None
        return df

    if "image_1920" not in df.columns:
        df = df.copy()
        df["_image_data_uri"] = None
        return df

    refs = df["image_1920"].fillna("").astype(str).tolist()

    has_abfss = any(r.startswith("abfss://") for r in refs)
    can_fetch = connection_is_configured() and has_abfss

    results = [(None, None, None)] * len(refs)
    if can_fetch:
        try:
            client = get_datalake_service_client()
            unique_refs = list(dict.fromkeys(r for r in refs if r.startswith("abfss://")))
            fetched = dict(zip(unique_refs, load_images_parallel(client, unique_refs)))
            results = [fetched.get(ref, (None, None, None)) for ref in refs]
        except Exception as e:
            # Do not kill the page - just fall back to "No image available".
            st.warning(f"Image client init failed: {type(e).__name__}: {e}")
            results = [(None, None, str(e))] * len(refs)

    resolved: list = []
    for img, _raw, _err in results:
        if img is None:
            resolved.append(None)
            continue
        try:
            resolved.append(image_to_data_uri(img))
        except Exception:
            resolved.append(None)

    df = df.copy()
    df["_image_data_uri"] = resolved
    return df


# =====================================================================
# View: Main report
# =====================================================================

def _reset_page():
    st.session_state["page"] = 1


@st.cache_data(show_spinner=False)
def _mapping_for_file(filename, modified):
    return read_category_mapping(filename)


def load_master_mapping():
    filename = Path(__file__).with_name("PRASHANTI-ODOO CATEGORY.xlsx")
    try:
        return _mapping_for_file(str(filename), filename.stat().st_mtime_ns)
    except (OSError, ValueError, KeyError) as error:
        st.error(f"Category mapping could not be loaded: {error}")
        st.stop()


def business_today():
    return datetime.now(timezone(timedelta(hours=5, minutes=30))).date()


def appearance_controls(qp, show_settings=True):
    default = int(qp.get("r_header_size", "16")) if str(qp.get("r_header_size", "16")).isdigit() else 16
    if show_settings:
        with st.sidebar.expander("Display settings"):
            size = st.slider("Table and card header size", 12, 24, max(12, min(24, default)), key="header_size")
    else:
        size = 16
        st.html("""<style>
            section[data-testid="stSidebar"] p,
            section[data-testid="stSidebar"] label,
            section[data-testid="stSidebar"] input,
            section[data-testid="stSidebar"] [data-baseweb="select"],
            section[data-testid="stSidebar"] [data-baseweb="select"] * {
                font-size: 14px !important;
            }
            div[data-baseweb="popover"] [role="option"],
            div[data-baseweb="popover"] [role="option"] *,
            [data-testid="stSelectboxVirtualDropdown"] li,
            [data-testid="stSelectboxVirtualDropdown"] li * {
                font-size: 14px !important;
            }
        </style>""")
    st.html(f"<style>.report-table th {{font-size:{size}px;}} .product-title {{font-size:{size}px;}} "
            ".hero h1,.drill-hero h1 {font-size:28px;} "
            ".report-table.has-category {table-layout:fixed;} "
            ".report-table .pin-category {position:sticky;left:0;z-index:2;width:260px;box-sizing:border-box;} "
            ".report-table .pin-vendor {position:sticky;left:260px;z-index:2;width:180px;box-sizing:border-box;} "
            ".report-table th {white-space:normal;overflow-wrap:anywhere;} "
            ".report-table td {overflow:hidden;text-overflow:ellipsis;} "
            ".report-table td.dimension-cell {white-space:normal;overflow-wrap:anywhere;vertical-align:top;} "
            ".report-table td.pin-category,.report-table td.pin-vendor {background:var(--st-bg2);} "
            ".report-table th.pin-category,.report-table th.pin-vendor {background:#ffffff;z-index:3;} "
            ".report-table th {position:sticky;top:0;} </style>")


def options(df, column, null_label=None):
    values = sorted(df[column].dropna().astype(str).unique().tolist())
    if column == "master_category" and "Unmatched category" in values:
        values.remove("Unmatched category")
        values.append("Unmatched category")
    if null_label is not None and df[column].isna().any():
        values.insert(0, null_label)
    return values


def select_filter(label, values, key, defaults=(), target=None):
    if key not in st.session_state:
        st.session_state[key] = [value for value in defaults if value in values]
    else:
        st.session_state[key] = [value for value in st.session_state[key] if value in values]
    target = st.sidebar if target is None else target
    selected = target.multiselect(label, values, key=key, format_func=display_store_name)
    if label in {"Category", "Master category"} and selected:
        with target.expander(f"Selected {label.lower()} names ({len(selected)})"):
            for value in selected:
                st.text(value)
    return selected


def selected_mask(df, column, values, null_label=None):
    if not values:
        return pd.Series(True, index=df.index)
    mask = df[column].isin([v for v in values if v != null_label])
    if null_label is not None and null_label in values:
        mask |= df[column].isna()
    return mask


def scope_values(df, column, null_label):
    return sorted(set(df[column].fillna(null_label).astype(str)))


def scoped_drill_url(kind, df, meta, state):
    params = dict(state)
    params.update({"drill": kind, "scope": "group", "start_date": meta["start_date"].isoformat(),
                   "end_date": meta["end_date"].isoformat(),
                   "d_companies": _encode_list(scope_values(df, "company_id_name", "(No store)")),
                   "d_vendors": _encode_list(scope_values(df, "Vendor", NONE_VENDOR_LABEL)),
                   "d_categories": _encode_list(scope_values(df, "categ_id_name", "(No category)"))})
    return "?" + urlencode(params)


def render_group_table(frame, keys, meta, state, sort_field, ascending, totals_frame=None):
    grouped = aggregate_report(frame, keys)
    grouped = grouped.sort_values([sort_field] + [key for key in keys if key != sort_field],
                                 ascending=[ascending] + [True for key in keys if key != sort_field],
                                 na_position="last", kind="stable")
    headers = [{"categ_id_name": "Category", "Vendor": "Vendor", "company_id_name": "Store", "master_category": "Master category"}[key] for key in keys]
    headers += ["Sold qty", "Sales value", "Available qty", "Available value", "Inward qty", "Inward value", "Intransit qty", "Intransit value", "Outgoing qty", "Outgoing value", "Returns qty", "Returns value"]
    table_class = "report-table has-category" if "categ_id_name" in keys else "report-table"
    dimension_widths = {"categ_id_name": 260, "Vendor": 180, "master_category": 180, "company_id_name": 300}
    widths = [dimension_widths[key] for key in keys] + [125, 160] * (len(METRICS) // 2)
    table_width = sum(widths)
    html = [f'<div class="report-table-wrap"><table class="{table_class}" style="width:{table_width}px;min-width:{table_width}px"><colgroup>']
    html.extend(f'<col style="width:{width}px">' for width in widths)
    html.append("</colgroup><thead><tr>")
    classes = ["dimension-cell " + ("pin-category" if key == "categ_id_name" else "pin-vendor" if key == "Vendor" and "categ_id_name" in keys else "") for key in keys]
    html.extend(f'<th class="{classes[i] if i < len(classes) else ""}">{safe_html(label)}</th>' for i, label in enumerate(headers))
    html.append("</tr></thead><tbody>")
    def add_row(labels, totals, scope):
        html.append("<tr>" + "".join(f'<td class="{classes[i]}" title="{safe_html(label)}">{safe_html(label)}</td>' for i, label in enumerate(labels)))
        for metric in METRICS:
            value = float(totals[metric])
            if metric in {"sale_qty", "available_qty"}:
                html.append(f"<td>{value:,.2f}</td>")
            elif metric.endswith("value"):
                html.append(f"<td>₹{value:,.2f}</td>")
            else:
                html.append(f"<td>{value:,.2f}</td>")
        html.append("</tr>")
    total_source = frame if totals_frame is None else totals_frame
    if not total_source.empty:
        add_row(["Grand total"] + [""] * (len(keys) - 1), total_source[METRICS].sum(), total_source)
    for _, row in grouped.iterrows():
        mask = pd.Series(True, index=frame.index)
        for key in keys:
            mask &= frame[key].isna() if pd.isna(row[key]) else frame[key].eq(row[key])
        add_row([row[key] if pd.notna(row[key]) else "(Not recorded)" for key in keys], row, frame[mask])
    html.append("</tbody></table></div>")
    st.markdown("".join(html), unsafe_allow_html=True)
    return grouped


_HIERARCHY_COMPONENT = st.components.v2.component(
    "report_hierarchy",
    html="<div class='hierarchy-root'></div>",
    isolate_styles=False,
    js="""
export default function({data, parentElement, setStateValue}) {
    const root = parentElement.querySelector('.hierarchy-root');
    const palette = JSON.stringify(data.ageColors);
    const changed = root._reportHtml !== data.html || root._agePalette !== palette;
    if (changed) {
        root.innerHTML = data.html;
        root._reportHtml = data.html;
        root._agePalette = palette;
        root._reportNodes = [...root.querySelectorAll('details')];
        root._reportRows = [...root.querySelectorAll('tr[data-ancestors]')].map(row => ({row, ancestors: JSON.parse(row.dataset.ancestors)}));
    }
    const nodes = root._reportNodes;
    const hierarchy = nodes.filter(node => !node.classList.contains('quantity-age-toggle'));
    const rows = root._reportRows;
    const opened = new Set(data.opened.filter(id => !id.startsWith('age-')));
    if (changed) hierarchy.forEach(node => { node.open = opened.has(node.id); });
    const updateRows = () => {
        const active = new Set(hierarchy.filter(node => node.open).map(node => node.id));
        const visible = rows.filter(({ancestors}) => ancestors.every(id => active.has(id))).map(({row}) => {
            row.hidden = false;
            return row;
        });
        const body = root.querySelector('.pivot-hierarchy>tbody');
        if (body.children.length !== visible.length || visible.some((row, index) => body.children[index] !== row)) {
            body.replaceChildren(...visible);
        }
    };
    updateRows();
    let timer;
    nodes.forEach(node => {
        node.ontoggle = () => {
            if (node.classList.contains('quantity-age-toggle')) {
                if (node.open) {
                    const body = node.querySelector('[data-quantities]');
                    if (body && !body.dataset.ready) {
                        body.dataset.ready = 'true';
                        const boxes = document.createElement('div');
                        boxes.className = 'quantity-age-boxes';
                        boxes.style.cssText = 'display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:3px;margin-top:5px;width:100%';
                        JSON.parse(body.dataset.quantities).forEach((qty, index) => {
                            const box = document.createElement('div');
                            box.style.cssText = `background:${data.ageColors[index]};color:#111827;border:1px solid #9ca3af;border-radius:3px;padding:4px 1px;text-align:center;min-width:0;display:flex;flex-direction:column;justify-content:center`;
                            box.title = data.ageBands[index];
                            const label = document.createElement('div');
                            label.style.cssText = 'font-size:9px;line-height:12px;margin-bottom:2px;white-space:nowrap;min-width:0';
                            label.textContent = '';
                            const value = document.createElement('div');
                            value.style.cssText = 'font-size:11px;line-height:15px;font-weight:700;white-space:nowrap;min-width:0';
                            value.textContent = Number(qty).toLocaleString('en-IN', {minimumFractionDigits:0, maximumFractionDigits:0});
                            box.append(label, value);
                            boxes.append(box);
                        });
                        body.append(boxes);
                        // Fit both the age label and quantity to their box width.
                        requestAnimationFrame(() => {
                            [...boxes.children].forEach(box => {
                                [...box.children].forEach((text, index) => {
                                    let size = index === 0 ? 9 : 11;
                                    text.style.fontSize = size + 'px';
                                    while (text.scrollWidth > text.clientWidth && size > 6) {
                                        size -= 0.5;
                                        text.style.fontSize = size + 'px';
                                    }
                                });
                            });
                        });
                    }
                }
                return;
            }
            updateRows();
            const next = hierarchy.filter(item => item.open).map(item => item.id);
            clearTimeout(timer);
            if (JSON.stringify([...opened].sort()) !== JSON.stringify([...next].sort())) {
                timer = setTimeout(() => {
                    opened.clear();
                    next.forEach(id => opened.add(id));
                    setStateValue('opened', next);
                }, 200);
            }
        };
    });
    return () => { clearTimeout(timer); nodes.forEach(node => { node.ontoggle = null; }); };
}
""",
)


@st.fragment
def render_hierarchy_table(frame, meta, state, sort_field, ascending, pivot_order=("Category", "Vendor", "Store")):
    signature = hashlib.sha256(pd.util.hash_pandas_object(frame, index=True).values.tobytes() + str((pivot_order, sort_field, ascending)).encode()).hexdigest()[:16]
    component_key = "hierarchy_" + signature
    opened = st.session_state.get(component_key, {}).get("opened", [])
    table_html, headers, rows, ancestors = build_hierarchy_content(frame, meta, sort_field, ascending, pivot_order)
    _HIERARCHY_COMPONENT(data={"html": table_html, "opened": opened, "ageBands": AGE_BANDS, "ageColors": list(AGE_COLORS.values())}, key=component_key,
                         default={"opened": []}, on_opened_change=lambda: None)
    active = set(opened)
    export = pd.DataFrame([row for row, path in zip(rows, ancestors) if all(node in active for node in path)], columns=headers)
    st.session_state.current_report_export = export
    holder = st.session_state.get("report_export_holder")
    if holder is not None:
        holder["frame"] = export
    return export


@st.cache_data(ttl=21600, max_entries=16, show_spinner=False)
def build_hierarchy_content(frame, meta, sort_field, ascending, pivot_order, age_group_revision="four-compact-bands-v2"):
    dimensions = {"Store": ["company_id_name"], "Vendor": ["Vendor"],
                  "Category": ["master_category", "categ_id_name"]}
    levels = [key for name in pivot_order for key in dimensions[name]]
    headers = [" / ".join(pivot_order), "Sold qty", "Sales value", "Available qty",
               "Available value", "Inward qty", "Inward value", "Intransit qty", "Intransit value", "Outgoing qty", "Outgoing value", "Returns qty", "Returns value"]
    visible_rows = []
    row_ancestors = []
    css = [".pivot-hierarchy tr[hidden] {display:none !important;} "
           ".pivot-hierarchy .tree-cell {min-width:320px;white-space:normal;} "
           ".pivot-hierarchy summary {cursor:pointer;} "
           ".pivot-hierarchy .master-row,.pivot-hierarchy .grand-total {font-weight:700;} "
           ".pivot-hierarchy .grand-total td {border-bottom:2px solid #dddddd;} "]
    markup = ['<div class="report-table-wrap"><table class="report-table pivot-hierarchy" style="table-layout:fixed;width:2532px;min-width:2532px"><colgroup><col style="width:360px">' + '<col style="width:181px">' * len(METRICS) + '</colgroup><thead><tr>']
    markup.extend(f"<th>{safe_html(header)}</th>" for header in headers)
    markup.append("</tr></thead><tbody>")

    def add_row(label, totals, classes, depth=0, toggle_id=None, visible=True, scope=None, row_id="total", ancestors=()):
        companies = scope["company_id_name"].dropna()
        is_ho_only = not companies.empty and companies.eq(HO_COMPANY).all()
        has_ho = companies.eq(HO_COMPANY).any()
        def not_applicable(metric):
            return (metric.startswith("sale_") and is_ho_only) or (metric.startswith("returns_") and not has_ho)
        visible_rows.append(["    " * depth + str(label)] + [None if not_applicable(metric) else float(totals[metric]) for metric in METRICS])
        row_ancestors.append(ancestors)
        markup.append(f'<tr class="{classes}" data-ancestors="{safe_html(json.dumps(ancestors))}"' + (' hidden' if depth else '') + f'><td class="tree-cell" style="padding-left:{13 + depth * 22}px">')
        if toggle_id:
            toggle_class = "master-toggle" if depth == 0 else "category-toggle"
            markup.append(f'<details id="{toggle_id}" class="{toggle_class}"><summary>{safe_html(label)}</summary></details>')
        else:
            markup.append(safe_html(label))
        markup.append("</td>")
        for metric in METRICS:
            if not_applicable(metric):
                markup.append("<td></td>")
                continue
            prefix = "₹" if metric.endswith("value") else ""
            if metric in {"sale_qty", "available_qty", "inward_qty", "intransit_qty", "outgoing_qty"}:
                kind = {"sale_qty": "sales", "available_qty": "available", "inward_qty": "inward", "intransit_qty": "intransit", "outgoing_qty": "outgoing"}[metric]
                age_id = f"age-{kind}-{row_id}"
                markup.append(f'<td><details id="{age_id}" class="quantity-age-toggle"><summary title="Expand age bands">{float(totals[metric]):,.2f}</summary>')
                markup.append(inline_quantity_age_html(scope, meta, kind))
                markup.append("</details></td>")
            else:
                markup.append(f"<td>{prefix}{float(totals[metric]):,.2f}</td>")
        markup.append("</tr>")

    css.append(".quantity-age-toggle {min-width:110px;position:relative;} .quantity-age-toggle>summary {white-space:nowrap;} "
               ".quantity-age-body {width:100%;min-width:0;padding:6px 0;white-space:normal;font-weight:400;}")
    add_row("Grand total", frame[METRICS].sum(), "grand-total", scope=frame)
    def render_level(group, depth, ancestors, path):
        key = levels[depth]
        grouped = aggregate_report(group, [key])
        primary = sort_field if sort_field in METRICS else key
        fields = [primary] + ([key] if key != primary else [])
        grouped = grouped.sort_values(fields, ascending=[ascending] + ([True] if key != primary else []), na_position="last", kind="stable")
        if key == "master_category":
            unmatched = grouped[key].eq("Unmatched category")
            grouped = pd.concat([grouped[~unmatched], grouped[unmatched]], ignore_index=True)
        for index, (_, record) in enumerate(grouped.iterrows()):
            value = record[key]
            subset = group[group[key].isna() if pd.isna(value) else group[key].eq(value)]
            node = path + (index,)
            node_suffix = "-".join(str(part) for part in node)
            toggle_id = "pivot-node-" + node_suffix
            row_class = "pivot-row-" + node_suffix
            classes = "master-row" if depth == 0 else f"nested-row {row_class} " + ("vendor-row pivot-vendors-" + node_suffix if key == "Vendor" else "category-row")
            leaf = depth == len(levels) - 1
            label = value if pd.notna(value) else "(No vendor)" if key == "Vendor" else "(No category)" if key == "categ_id_name" else "(Not recorded)"
            add_row(label, record, classes, depth, None if leaf else toggle_id, True, subset, node_suffix, ancestors)
            if not leaf:
                render_level(subset, depth + 1, ancestors + (toggle_id,), node)
    render_level(frame, 0, (), ())
    markup.append("</tbody></table></div>")
    table_html = "<style>" + "".join(css) + "</style>" + "".join(markup)
    return table_html, headers, visible_rows, row_ancestors


def render_main_report():
    qp = st.query_params
    mapping = load_master_mapping()
    appearance_controls(qp, show_settings=False)
    today = business_today()
    try:
        initial = (date.fromisoformat(qp["r_start"]), date.fromisoformat(qp["r_end"])) if qp.get("r_start") and qp.get("r_end") else date_bounds("Month to date", today)
    except ValueError:
        initial = date_bounds("Month to date", today)
    if "applied_dates" not in st.session_state:
        st.session_state.applied_dates = initial
    if "date_preset" not in st.session_state:
        st.session_state.date_preset = "Custom range" if qp.get("r_start") else "Month to date"
    def reset_date_draft():
        st.session_state.pop("custom_dates", None)
    def cancel_dates():
        reset_date_draft()
        st.session_state.date_preset = "Custom range"
    st.markdown("### Inventory purchase report")
    date_area, report_controls = st.columns([2, 4], vertical_alignment="bottom", wrap=False)
    with date_area:
        with st.container(horizontal=True, horizontal_alignment="left"):
            applied_start, applied_end = st.session_state.applied_dates
            date_label = f"{applied_start:%d %b %Y} – {applied_end:%d %b %Y}"
            with st.popover(date_label, icon=":material/calendar_month:"):
                preset = st.selectbox("Period", DATE_PRESETS, key="date_preset", on_change=reset_date_draft)
                if preset == "Custom range":
                    pending = st.date_input("Start and end dates", value=st.session_state.applied_dates,
                                            max_value=today, key="custom_dates", format="DD/MM/YYYY")
                else:
                    pending = date_bounds(preset, today)
                with st.container(horizontal=True):
                    apply = st.button("Apply", type="primary", disabled=len(pending) != 2)
                    st.button("Cancel", on_click=cancel_dates)
    start_date, end_date = st.session_state.applied_dates
    if apply and len(pending) == 2:
        start_date, end_date = pending
    auto_restore = "raw_df" not in st.session_state and bool(qp.get("r_start"))
    if (apply or auto_restore) and connection_is_configured():
        with st.spinner("Loading report..."):
            try:
                conn_str = build_connection_string(SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID)
                t0 = time.time()
                raw, timings = load_report_parallel(conn_str, start_date, end_date, business_today())
                st.session_state.raw_df = raw
                st.session_state.raw_meta = dict(start_date=start_date, end_date=end_date,
                                                fetched_at=datetime.now(), elapsed=time.time() - t0, query_timings=timings)
                st.session_state.applied_dates = (start_date, end_date)
                if apply:
                    st.rerun()
            except Exception as error:
                st.error(f"Report query failed: {error}")
    elif not connection_is_configured():
        st.info("Configure the SQL connection secrets to fetch report data.")
    raw = st.session_state.get("raw_df")
    if raw is None:
        st.info("Open the date filter and click Apply to load the report.")
        return
    if not set(METRICS).issubset(raw.columns):
        st.info("Click Apply in the date filter to load the new transfer and returns columns.")
        return
    meta = st.session_state.raw_meta
    if meta.get("query_timings"):
        with st.sidebar.expander("Last load timings"):
            st.caption(f"Total load: {meta['elapsed']:.1f} seconds. Query times overlap.")
            st.dataframe(pd.DataFrame([{"Query": name, "Seconds": round(seconds, 2)}
                                       for name, seconds in meta["query_timings"].items()]), hide_index=True)
    raw = add_master_category(raw, mapping, "categ_id_name")
    labels = {"categ_id_name": "Category", "Vendor": "Vendor", "sale_qty": "Sold quantity", "sale_value": "Sales value", "available_qty": "Available quantity", "available_value": "Available value", "inward_qty": "Inward quantity", "inward_value": "Inward value", "intransit_qty": "Intransit quantity", "intransit_value": "Intransit value", "outgoing_qty": "Outgoing quantity", "outgoing_value": "Outgoing value", "returns_qty": "Returns quantity", "returns_value": "Returns value"}
    view = "Category hierarchy"
    if "report_sort" not in st.session_state:
        st.session_state.report_sort = qp.get("r_sort", "categ_id_name") if qp.get("r_sort") in labels else "categ_id_name"
    if "report_direction" not in st.session_state:
        st.session_state.report_direction = qp.get("r_direction", "Ascending") if qp.get("r_direction") in {"Ascending", "Descending"} else "Ascending"
    with st.sidebar:
        companies = select_filter("Store (company)", options(raw, "company_id_name", "(No store)"), "report_stores", _decode_list(qp.get("r_companies", "")))
        vendors = select_filter("Vendor", options(raw, "Vendor", NONE_VENDOR_LABEL), "report_vendors", _decode_list(qp.get("r_vendors", "")))
        masters = select_filter("Master category", options(raw, "master_category"), "report_masters", _decode_list(qp.get("r_masters", "")))
        category_source = raw[raw.master_category.isin(masters)] if masters else raw
        categories = select_filter("Category", options(category_source, "categ_id_name", "(No category)"), "report_categories", _decode_list(qp.get("r_categories", "")))
        sort_field = st.selectbox("Sort by", list(labels), format_func=labels.get, key="report_sort")
        ascending = st.selectbox("Direction", ["Ascending", "Descending"], key="report_direction") == "Ascending"
    mask = selected_mask(raw, "company_id_name", companies, "(No store)") & selected_mask(raw, "Vendor", vendors, NONE_VENDOR_LABEL) & selected_mask(raw, "master_category", masters) & selected_mask(raw, "categ_id_name", categories, "(No category)")
    df = raw[mask]
    if df.empty:
        st.info("No rows match these filters.")
    render_report_section(df, meta, sort_field, ascending, report_controls)


def render_report_section(df, meta, sort_field, ascending, report_controls):
    """Render controls alongside the date filter and the table beneath them."""
    view = "Category hierarchy"
    state = {}
    pivot_orders = [order for count in (1, 2, 3) for order in permutations(("Store", "Vendor", "Category"), count)]
    with report_controls:
        legend = '<div style="display:flex;justify-content:flex-end;align-items:center;gap:6px;flex-wrap:wrap;font-size:12px;"><span>Age (days):</span>'
        for band in AGE_BANDS:
            legend += f'<span style="background:{AGE_COLORS[band]};color:#111827;padding:3px 7px;border-radius:3px;white-space:nowrap;">{safe_html(band)}</span>'
        st.markdown(legend + '</div>', unsafe_allow_html=True)
        with st.container(horizontal=True, horizontal_alignment="right", vertical_alignment="center", wrap=False):
            pivot_order = st.selectbox("Pivot order", pivot_orders, index=pivot_orders.index(("Category", "Vendor", "Store")), format_func=lambda order: " \u2192 ".join(order), key="pivot_order", width=360, label_visibility="collapsed")
            download_toolbar = st.container(horizontal=True, width="content", wrap=False)
    st.caption("Sold, Inward and Outgoing: selected period; age at sale or transfer completion from original HO inward. Available and Intransit: current snapshot; age today from original HO inward. Returns: HO vendor deliveries in the selected period; no age split.")
    export = render_hierarchy_table(df, meta, state, sort_field, ascending, pivot_order)
    # Deferred downloads run in a worker without Streamlit's session context.
    # Share a plain holder so table-only reruns can update the current snapshot.
    download_snapshot = {"frame": export}
    st.session_state.report_export_holder = download_snapshot
    with download_toolbar:
        filename = f"inventory_report_{meta['start_date']}_{meta['end_date']}"
        st.download_button("CSV", lambda: download_snapshot["frame"].to_csv(index=False, float_format="%.2f").encode("utf-8-sig"), filename + ".csv", "text/csv", icon=":material/download:", width="content", on_click="ignore", help="Downloads the current table, including only expanded rows.")
        st.download_button("PDF", lambda: report_pdf(download_snapshot["frame"], meta["start_date"], meta["end_date"], view, {}, table_only=True), filename + ".pdf", "application/pdf", icon=":material/download:", width="content", on_click="ignore", help="Downloads the current table, including only expanded rows.")


AGE_BANDS = ["0–90", "91–180", "181–360", ">360 / no age"]
AGE_COLORS = dict(zip(AGE_BANDS, ["#4ade80", "#facc15", "#fb923c", "#f87171"]))


def quantity_age_summary(frame, age_column, quantity_column):
    """Use four age groups, including missing/invalid dates in the final red group."""
    detail = frame.copy()
    ages = pd.to_numeric(detail[age_column], errors="coerce")
    detail["Age band"] = pd.cut(ages, [-1, 90, 180, 360, float("inf")], labels=AGE_BANDS)
    detail.loc[ages.isna() | ages.lt(0), "Age band"] = AGE_BANDS[-1]
    rows = []
    for band in AGE_BANDS:
        group = detail[detail["Age band"].eq(band)]
        lots = group[group.lot_number.notna() & ~group.lot_number.isin(["", "False", "No lot recorded"])]
        rows.append({"Age band": band, "Products": group.product_id.nunique(),
                     "Lots": len(lots[["product_id", "lot_number"]].drop_duplicates()),
                     "Quantity": pd.to_numeric(group[quantity_column], errors="coerce").sum()})
    return detail, pd.DataFrame(rows)


def inline_quantity_age_html(scope, meta, kind):
    """Send compact totals; create the five boxes only when opened in the browser."""
    if f"age_{kind}_0" not in scope:
        return '<div class="quantity-age-body">Click Apply in the date filter once to load age quantities.</div>'
    # Merge the preloaded SQL buckets in memory, including existing session data.
    source = [float(scope[f"age_{kind}_{index}"].sum()) for index in range(5)]
    unknown = float(scope[f"age_{kind}_unknown"].sum())
    quantities = [source[0], source[1], source[2] + source[3], source[4] + unknown]
    caption = "Days from original inward to sale; selected period" if kind == "sales" else "Current stock; days since original inward (not period receipts)"
    return (f'<div class="quantity-age-body" data-quantities="{safe_html(json.dumps(quantities))}" '
            f'data-unknown="{unknown}" data-caption="{safe_html(caption)}"></div>')


def quantity_age_query(kind, meta, reference):
    """Summarize in SQL; sold age follows the receipt of the actual sold lot."""
    product = PRODUCT_BASE[:PRODUCT_BASE.index(",\nproduct_images AS (")]
    if kind == "sales":
        ctes = SALES_SQL[SALES_SQL.index(",\npos_sales AS ("):SALES_SQL.index("\nSELECT\n    s.company,")]
        ctes += AVAILABLE_SQL[AVAILABLE_SQL.index(",\nstock_min_date AS ("):AVAILABLE_SQL.index(",\ninventory_detail AS (")]
        source = ("sales_detail s LEFT JOIN product_base p ON s.product_id = p.product_id "
                  "LEFT JOIN stock_min_date smd ON s.product_id = smd.product_id AND s.lot_number = smd.lot_number")
        company, vendor, category = "s.company", "p.vendor", "p.category"
        date_column, qty = "smd.stock_move_date", "s.sale_qty"
        age = "DATEDIFF(DAY, smd.stock_move_date, s.sold_date)"
        reference_join = ""
        params = [meta["start_date"].isoformat(), meta["end_date"].isoformat()]
        condition = "p.category IS NOT NULL AND LOWER(p.category) NOT LIKE '%admin%'"
    else:
        ctes = AVAILABLE_SQL[AVAILABLE_SQL.index(",\nstock_min_date AS ("):AVAILABLE_SQL.index("\nSELECT\n    i.company,")]
        source = "inventory_detail i"
        company, vendor, category = "i.company", "i.vendor", "i.category"
        date_column, qty = "i.stock_move_date", "i.available_inventory"
        params = []
        condition = "i.available_inventory <> 0"
        age = f"DATEDIFF(DAY, {date_column}, ref.age_reference)"
        reference_join = " CROSS JOIN (SELECT CAST(? AS DATE) AS age_reference) ref"
        params.append(reference.isoformat())
    expressions = []
    for index, (lower, upper) in enumerate([(0, 90), (91, 180), (181, 270), (271, 360), (361, None)]):
        bounds = f"{age} >= {lower}" if upper is None else f"{age} BETWEEN {lower} AND {upper}"
        expressions.append(f"SUM(CASE WHEN {bounds} THEN {qty} ELSE 0 END) AS age_{kind}_{index}")
    expressions.append(f"SUM(CASE WHEN {date_column} IS NULL OR {age} < 0 THEN {qty} ELSE 0 END) AS age_{kind}_unknown")
    sql = (product + ctes + f"\nSELECT {company} AS company_id_name, {vendor} AS Vendor, {category} AS categ_id_name,\n"
           + ",\n".join(expressions) + f"\nFROM {source}" + reference_join
           + f"\nWHERE {condition} GROUP BY {company}, {vendor}, {category}")
    return sql, params



def transfer_age_query(meta, reference):
    """Allocate each supplied transfer quantity to its recorded lots without changing totals."""
    product = PRODUCT_BASE[:PRODUCT_BASE.index(",\nproduct_images AS (")]
    sql = "WITH report_dates AS (SELECT CAST(? AS DATE) AS start_date, CAST(? AS DATE) AS end_date),\n" + product.removeprefix("\nWITH ")
    sql += TRANSFER_CTES + STOCK_RECEIPT_CTE + """,
transfer_lots AS (
    SELECT ml.picking_key, ml.product_key, NULLIF(NULLIF(ml.lot_id_name, 'False'), '') AS lot_number,
        SUM(ABS(COALESCE(ml.quantity_product_uom, ml.quantity, 0))) AS lot_qty
    FROM normalized_move_lines ml
    INNER JOIN (SELECT DISTINCT picking_id, product_id FROM transfer_facts) f
        ON ml.picking_key = f.picking_id AND ml.product_key = f.product_id
    WHERE ml.state <> 'cancel' OR ml.state IS NULL
    GROUP BY ml.picking_key, ml.product_key, NULLIF(NULLIF(ml.lot_id_name, 'False'), '')
),
transfer_age_matches AS (
    SELECT f.*, l.lot_number, l.lot_qty,
        SUM(l.lot_qty) OVER (PARTITION BY f.picking_id, f.product_id, f.kind) AS matched_qty,
        COUNT(*) OVER (PARTITION BY f.picking_id, f.product_id, f.kind) AS matched_rows
    FROM transfer_facts f
    LEFT JOIN transfer_lots l ON f.picking_id = l.picking_key AND f.product_id = l.product_key
),
transfer_ages AS (
    SELECT f.company, p.vendor, f.category, f.kind,
        f.quantity * CASE WHEN f.matched_qty > 0 THEN 1.0 * f.lot_qty / f.matched_qty
                          ELSE 1.0 / f.matched_rows END AS quantity,
        DATEDIFF(DAY, smd.stock_move_date,
                 CASE WHEN f.kind = 'intransit' THEN ref.age_reference ELSE f.event_date END) AS age_days
    FROM transfer_age_matches f
    LEFT JOIN product_base p ON f.product_id = p.product_id
    LEFT JOIN stock_min_date smd ON f.product_id = smd.product_id AND f.lot_number = smd.lot_number
    CROSS JOIN (SELECT CAST(? AS DATE) AS age_reference) ref
)
SELECT company AS company_id_name, vendor AS Vendor, category AS categ_id_name,
"""
    expressions = []
    for kind in ("inward", "intransit", "outgoing"):
        for index, (lower, upper) in enumerate([(0, 90), (91, 180), (181, 270), (271, 360), (361, None)]):
            bounds = f"age_days >= {lower}" if upper is None else f"age_days BETWEEN {lower} AND {upper}"
            expressions.append(f"SUM(CASE WHEN kind = '{kind}' AND {bounds} THEN quantity ELSE 0 END) AS age_{kind}_{index}")
        expressions.append(f"SUM(CASE WHEN kind = '{kind}' AND (age_days IS NULL OR age_days < 0) THEN quantity ELSE 0 END) AS age_{kind}_unknown")
    sql += ",\n".join(expressions) + "\nFROM transfer_ages GROUP BY company, vendor, category"
    return sql, [meta["start_date"].isoformat(), meta["end_date"].isoformat(), reference.isoformat()]

def preload_quantity_ages(raw, meta):
    """Download only one small age-summary row per store/vendor/category."""
    result = raw.copy()
    keys = ["company_id_name", "Vendor", "categ_id_name"]
    conn_str = build_connection_string(SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID)
    for kind in ("sales", "available"):
        sql, params = quantity_age_query(kind, meta, business_today())
        totals = run_query(conn_str, sql, params)
        columns = [f"age_{kind}_{index}" for index in [0, 1, 2, 3, 4, "unknown"]]
        result = result.merge(totals[keys + columns], on=keys, how="left", validate="one_to_one")
        result[columns] = result[columns].fillna(0)
    sql, params = transfer_age_query(meta, business_today())
    totals = run_query(conn_str, sql, params)
    columns = [f"age_{kind}_{index}" for kind in ("inward", "intransit", "outgoing") for index in [0, 1, 2, 3, 4, "unknown"]]
    result = result.merge(totals[keys + columns], on=keys, how="left", validate="one_to_one")
    result[columns] = result[columns].fillna(0)
    return result


def quantity_age_details(scope, meta, kind):
    qp = {"scope": "group", "start_date": meta["start_date"].isoformat(), "end_date": meta["end_date"].isoformat(),
          "d_companies": _encode_list(scope_values(scope, "company_id_name", "(No store)")),
          "d_vendors": _encode_list(scope_values(scope, "Vendor", NONE_VENDOR_LABEL)),
          "d_categories": _encode_list(scope_values(scope, "categ_id_name", "(No category)"))}
    sql, params = drill_query(kind, qp)
    conn_str = build_connection_string(SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID)
    detail = run_query(conn_str, sql, params)
    # Preserve exact selected combinations, rather than the cross product of scope lists.
    keys = scope[["company_id_name", "Vendor", "categ_id_name"]].drop_duplicates().rename(
        columns={"company_id_name": "company", "Vendor": "vendor", "categ_id_name": "category"})
    detail = detail.merge(keys, on=["company", "vendor", "category"], how="inner")
    return detail


def drill_query(kind, qp):
    sql = AVAILABLE_SQL if kind == "available" else SALES_SQL
    company_col = "i.company" if kind == "available" else "s.company"
    vendor_col = "i.vendor" if kind == "available" else "p.vendor"
    category_col = "i.category" if kind == "available" else "p.category"
    marker = "WHERE ( (? = '' AND " + company_col
    begin = sql.index(marker)
    finish = sql.index("\nORDER BY", begin)
    conditions = []
    params = [] if kind == "available" else [qp.get("start_date", ""), qp.get("end_date", "")]
    for column, scope_key, old_key, null_label in [(company_col, "d_companies", "company", "(No store)"), (vendor_col, "d_vendors", "vendor", NONE_VENDOR_LABEL), (category_col, "d_categories", "category", "(No category)")]:
        values = _decode_list(qp.get(scope_key, "")) if qp.get("scope") == "group" else [_clean(qp.get(old_key, "")) or null_label]
        predicate, values_params = scope_predicate(column, values, null_label)
        conditions.append(predicate)
        params.extend(values_params)
    if kind == "available":
        conditions.append("i.available_inventory <> 0")
    else:
        conditions.append("p.category IS NOT NULL AND LOWER(p.category) NOT LIKE '%admin%'")
    return sql[:begin] + "WHERE " + " AND ".join(conditions) + sql[finish:], params


def render_drillthrough(qp):
    kind = "sales" if qp.get("drill") == "sales" else "available"
    mapping = load_master_mapping()
    appearance_controls(qp)
    back = {key: value for key, value in dict(qp).items() if key.startswith("r_")}
    st.markdown(f'<a class="back-link" href="?{safe_html(urlencode(back))}" target="_self">← Back to report</a>', unsafe_allow_html=True)
    st.title("Sold products" if kind == "sales" else "Available inventory")
    if not connection_is_configured():
        st.error("Configure SQL connection secrets to load products.")
        return
    try:
        start = date.fromisoformat(qp.get("start_date", ""))
        end = date.fromisoformat(qp.get("end_date", ""))
        if start > end:
            raise ValueError("Start date is after end date.")
    except ValueError as error:
        st.error(f"Invalid drill-through date range: {error}")
        return
    st.caption(f"Report period: {start:%d %b %Y} – {end:%d %b %Y}" if kind == "sales" else "Current inventory snapshot")
    with st.spinner("Loading products..."):
        try:
            sql, params = drill_query(kind, qp)
            conn_str = build_connection_string(SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID)
            raw = run_query(conn_str, sql, params)
        except Exception as error:
            st.error(f"Product query failed: {error}")
            return
    raw = add_master_category(raw, mapping, "category")
    st.sidebar.subheader("Product filters")
    frame = raw
    for column, label, null_label in [("company", "Store (company)", "(No store)"), ("vendor", "Vendor", NONE_VENDOR_LABEL), ("master_category", "Master category", None), ("category", "Category", None), ("location", "Location", "(No location)")]:
        selected = select_filter(label, options(frame, column, null_label), f"drill_{kind}_{column}")
        frame = frame[selected_mask(frame, column, selected, null_label)]
    age_column = "sale_age" if kind == "sales" else "overall_age"
    if kind == "sales":
        frame = frame.copy()
        frame["sold_date"] = pd.to_datetime(frame["sold_date"], errors="coerce")
        frame[age_column] = (frame.sold_date - pd.to_datetime(frame.stock_move_date, errors="coerce")).dt.days
    else:
        frame = frame.copy()
        # Recompute after cached queries so age cannot become stale at midnight.
        frame[age_column] = (pd.Timestamp(business_today()) - pd.to_datetime(frame.stock_move_date, errors="coerce")).dt.days
    with st.sidebar.expander("Age / days filter"):
        minimum = st.number_input("Minimum days", min_value=0, value=0)
        maximum = st.number_input("Maximum days", min_value=0, value=36500)
        unknown = st.checkbox("Include unknown age", value=True)
    age_mask = frame[age_column].between(minimum, maximum)
    if unknown:
        age_mask |= frame[age_column].isna()
    frame = frame[age_mask]
    if minimum > maximum:
        st.warning("Minimum days must not exceed maximum days.")
    if frame.empty:
        st.info("No products match this selection.")
        return
    consolidated = st.toggle("Combine lots and stores into one card per product", value=True)
    display = consolidate_products(frame, kind) if consolidated else frame.copy()
    st.caption("Combined stock age is the oldest receipt age; combined sold age is the longest time from inward to sale. Age filters apply to the underlying detail rows before totals are combined.")
    qty = "sale_qty" if kind == "sales" else "available_inventory"
    value = "sale_value" if kind == "sales" else "available_selling_price"
    a, b, c = st.columns(3)
    a.metric("Products", f"{frame.product_id.nunique():,}")
    b.metric("Sold quantity" if kind == "sales" else "Available quantity", f"{frame[qty].sum():,.2f}")
    c.metric("Sales value" if kind == "sales" else "Available value", f"₹{frame[value].sum():,.2f}")
    sort_labels = {age_column: "Days from inward to sale" if kind == "sales" else "Stock age (days)", value: "Sales value" if kind == "sales" else "Available value", qty: "Sold quantity" if kind == "sales" else "Available quantity", "product_name": "Product name", "sku": "SKU"}
    if kind == "sales":
        sort_labels["sold_date"] = "Sold date"
    controls = st.columns([3, 1, 1])
    with controls[0]:
        sort = st.selectbox("Sort products by", list(sort_labels), format_func=sort_labels.get)
    with controls[1]:
        ascending = st.selectbox("Direction", ["Descending", "Ascending"]) == "Ascending"
    with controls[2]:
        size = st.selectbox("Cards per page", [12, 24, 48])
    display = display.sort_values([sort, "product_id"], ascending=[ascending, True], na_position="last", kind="stable")
    page_count = max(1, (len(display) + size - 1) // size)
    # Scope/filters/sort changes reset pagination through the widget identity.
    signature = str((tuple(frame.index), sort, ascending, size, consolidated))
    page_key = "cards_page_" + hashlib.sha256(signature.encode()).hexdigest()[:16]
    page = st.selectbox("Page", list(range(1, page_count + 1)), key=page_key)
    visible = display.iloc[(page - 1) * size:page * size]
    with st.spinner("Loading page images..."):
        visible = resolve_images_for_df(visible)
    for offset in range(0, len(visible), 4):
        for container, (_, row) in zip(st.columns(4), visible.iloc[offset:offset + 4].iterrows()):
            with container:
                card = render_sales_card(row) if kind == "sales" else render_available_card(row)
                st.markdown(card, unsafe_allow_html=True)
                if consolidated:
                    with st.expander("Lot, store and date details", key=f"product_details_{kind}_{row.product_id}", on_change="rerun") as details:
                        if details.open:
                            rows = frame[frame.product_id.eq(row.product_id)]
                            detail_columns = [col for col in ["company", "location", "lot_number", "sold_date", "stock_move_date", age_column, qty, value] if col in rows]
                            st.dataframe(display_store_frame(rows[detail_columns]), hide_index=True)
    st.caption(f"Page {page}/{page_count} · {len(visible)} of {len(display):,} cards. Totals include all matching pages.")
    st.download_button("Download product details CSV", display_store_frame(frame.drop(columns=["image_1920"], errors="ignore")).to_csv(index=False).encode("utf-8-sig"), f"{kind}_products.csv", "text/csv")


# =====================================================================
# Router
# =====================================================================

_qp = st.query_params

if _qp.get("drill"):
    render_drillthrough(_qp)
else:
    render_main_report()
