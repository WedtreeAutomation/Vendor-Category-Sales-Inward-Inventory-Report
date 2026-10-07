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
Bronze lakehouse. Requires: azure-identity, azure-storage-filedatalake,
Pillow.
"""

import base64
import hashlib
import io
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from report_logic import (DATE_PRESETS, METRICS, date_bounds, read_category_mapping,
    add_master_category, aggregate_report, consolidate_products, scope_predicate)
from urllib.parse import urlencode, urlparse

import pandas as pd
import pyodbc
import streamlit as st
from PIL import Image

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

st.set_page_config(
    page_title="Vendor / Category Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =====================================================================
# Secrets
# =====================================================================

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
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700;800&family=Inter:wght@400;500;600;700&display=swap');

    :root {
        --st-bg: #fffaf4;
        --st-bg2: #fdf8f2;
        --st-text: #3a1f28;
        --muted: rgba(58,31,40,.65);
        --gold: #b6871f;
        --on-accent: #fff8ec;
        --accent-chip: #8a1055;
        --wine: #560835;
        --wine2: #7c1049;
        --card-border: #f0e2cd;
        --card-bg: #ffffff;
        --card-shadow: rgba(58,31,40,.08);
        --sidebar-border: #ecd9bd;
        --select-border: #ddc4a0;
        --pill-border: #ecd9bd;
        --notice-bg-1: #fff7ed;
        --notice-bg-2: #fffbeb;
        --notice-border: #e3b23c;
        --notice-title: #7c1049;
        --notice-text: #5a2e1f;
        --hero-shadow: rgba(86,8,53,.28);
        --stock-bg: #12351f;
        --stock-fg: #9fe1b2;
        --border: #eadbc6;
    }

    html, body, .stApp {
        font-family: 'Inter', -apple-system, sans-serif;
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
        color: #560835 !important;
        z-index: 999999 !important;
    }

    .block-container {
        padding-top: 1.4rem;
        padding-bottom: 3rem;
        max-width: 1450px;
    }

    .hero, .drill-hero {
        background: linear-gradient(120deg,#560835 0%,#7c1049 55%,#37041f 100%);
        border-radius: 20px;
        padding: 26px 32px;
        margin-bottom: 22px;
        box-shadow: 0 10px 30px var(--hero-shadow);
        position: relative;
        overflow: hidden;
        color: #fff8ec;
    }

    .hero h1, .drill-hero h1 {
        font-family: 'Playfair Display', serif;
        color: #fff8ec;
        font-size: 2.1rem;
        font-weight: 800;
        margin: 0 0 4px 0;
    }

    .hero p, .drill-hero p {
        color: #e9d488;
        font-size: .95rem;
        margin: 0;
        font-weight: 500;
    }

    .hero .rule {
        width: 64px;
        height: 3px;
        border-radius: 3px;
        background: linear-gradient(90deg,#c9a227,#e9d488);
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
        background: linear-gradient(120deg,#560835,#7c1049) !important;
        border: 1px solid #37041f !important;
        color: #fff8ec !important;
    }

    [data-testid="stDownloadButton"] button {
        border-radius: 12px !important;
        background: linear-gradient(120deg,#d8b34c,#b6871f) !important;
        color: #2e2005 !important;
        border: 1px solid #96721c !important;
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
        font-size: .72rem;
        letter-spacing: .06em;
    }

    [data-testid="stMetricValue"] {
        color: var(--st-text);
        font-family: 'Playfair Display', serif;
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
        font-size: .8rem;
        margin: 2px 6px 2px 0;
        font-weight: 600;
    }

    .context-row { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0 20px; }
    .context-row .context-chip { margin: 0; font-size: 13px; padding: 7px 12px; }

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
        font-size:14.5px;
    }
    .notice-body {
        color:var(--notice-text);
        font-size:13.5px;
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
        font-size: 12px;
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
        background: #560835;
        color: #fff8ec;
        padding: 12px 13px;
        text-align: left;
        font-size: 13px;
        white-space: nowrap;
    }

    .report-table td {
        padding: 11px 13px;
        border-bottom: 1px solid var(--card-border);
        color: var(--st-text);
        white-space: nowrap;
        font-size: 13px;
    }

    .report-table tr:hover td {
        background: rgba(182,135,31,.06);
    }

    .qty-link {
        display:inline-block;
        min-width:34px;
        text-align:center;
        padding:4px 9px;
        border-radius:8px;
        background:#8a1055;
        color:#fff8ec !important;
        text-decoration:none !important;
        font-weight:700;
    }

    .qty-link:hover {
        background:#560835;
        box-shadow:0 3px 10px rgba(86,8,53,.25);
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
        font-size:13px;
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
        font-size: 13px;
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
        font-size: 12.5px;
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
        font-size: 21px;
        font-weight: 800;
    }

    .stock-badge {
        background: var(--stock-bg);
        color: var(--stock-fg);
        border-radius: 999px;
        padding: 7px 10px;
        font-size: 11px;
        font-weight: 800;
        white-space: nowrap;
    }

    .cost {
        margin-top: 5px;
        font-size: 12px;
        color: var(--wine2);
        opacity: .85;
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

QUERY_TEMPLATE = """
WITH product_master AS (
    SELECT
        CAST(p.id AS VARCHAR(50)) AS product_id,
        pt.vendor_id_name AS product_vendor,
        p.categ_id_name AS category,
        p.lst_price
    FROM WT_LH_Silver.Odoo.product_product p
    LEFT JOIN WT_LH_Silver.Odoo.product_template pt
        ON p.product_variant_id = pt.product_variant_id
    WHERE p.categ_id_name IS NOT NULL
      AND LOWER(p.categ_id_name) NOT LIKE '%admin%'
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
    WHERE CAST(po.date_order AS DATE) >= ?
      AND CAST(po.date_order AS DATE) <= ?
      AND po.user_id_name <> 'Administrator'
      AND pol.company_id_name NOT IN (
            'Saree Trails',
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
    GROUP BY
        sd.company_id_name,
        pm.product_vendor,
        pm.category
),

inward_summary AS (
    SELECT
        pk.company_id_name AS company,
        pm.product_vendor AS vendor,
        pm.category,
        SUM(pol.qty_received) AS inward_qty,
        SUM(pol.qty_received * pm.lst_price) AS inward_value
    FROM WT_LH_Silver.Odoo.stock_picking pk
    LEFT JOIN WT_LH_Silver.Odoo.purchase_order po
        ON pk.origin = po.name
    LEFT JOIN WT_LH_Silver.Odoo.purchase_order_line pol
        ON po.id = pol.order_id
    LEFT JOIN product_master pm
        ON CAST(pol.product_id AS VARCHAR(50)) = pm.product_id
    WHERE pk.picking_type_code = 'incoming'
      AND pk.state = 'done'
      AND pk.company_id_name NOT IN (
            'Saree Trails',
            'Wedtree eStore Private Limited - HO'
      )
      AND pk.location_id_name = 'Partners/Vendors'
      AND CAST(pk.date_done AS DATE) >= ?
      AND CAST(pk.date_done AS DATE) <= ?
      AND pm.category IS NOT NULL
    GROUP BY
        pk.company_id_name,
        pm.product_vendor,
        pm.category
),

inventory_summary AS (
    SELECT
        sq.company_id_name AS company,
        pm.product_vendor AS vendor,
        pm.category,
        SUM(sq.quantity) AS available_qty,
        SUM(sq.quantity * pm.lst_price) AS available_value
    FROM WT_LH_Silver.Odoo.stock_quant_n1 sq
    LEFT JOIN product_master pm
        ON CAST(sq.product_id AS VARCHAR(50)) = pm.product_id
    WHERE sq.company_id_name NOT IN (
            'Saree Trails',
            'Wedtree eStore Private Limited - HO'
      )
      AND pm.category IS NOT NULL
    GROUP BY
        sq.company_id_name,
        pm.product_vendor,
        pm.category
),

consolidated AS (
    SELECT
        COALESCE(s.vendor, i.vendor, inv.vendor) AS vendor,
        COALESCE(s.category, i.category, inv.category) AS category,
        COALESCE(s.sale_qty, 0) AS sale_qty,
        COALESCE(s.sale_value, 0) AS sale_value,
        COALESCE(inv.available_qty, 0) AS available_qty,
        COALESCE(inv.available_value, 0) AS available_value,
        COALESCE(i.inward_qty, 0) AS inward_qty,
        COALESCE(i.inward_value, 0) AS inward_value,
        COALESCE(s.company, i.company, inv.company) AS company
    FROM sales_summary s
    FULL OUTER JOIN inward_summary i
        ON s.company = i.company
       AND s.vendor = i.vendor
       AND s.category = i.category
    FULL OUTER JOIN inventory_summary inv
        ON COALESCE(s.company, i.company) = inv.company
       AND COALESCE(s.vendor, i.vendor) = inv.vendor
       AND COALESCE(s.category, i.category) = inv.category
)

SELECT
    vendor AS Vendor,
    category AS categ_id_name,
    SUM(sale_qty) AS sale_qty,
    SUM(sale_value) AS sale_value,
    SUM(available_qty) AS available_qty,
    SUM(available_value) AS available_value,
    SUM(inward_qty) AS inward_qty,
    SUM(inward_value) AS inward_value,
    company AS company_id_name
FROM consolidated
GROUP BY
    vendor,
    category,
    company
ORDER BY
    vendor,
    category,
    company;
"""


def build_query_and_params(start_str, end_str):
    """Date range is the ONLY filter baked into SQL now. Params are
    just the two date bounds, used twice (sales window, inward window)."""
    params = [start_str, end_str, start_str, end_str]
    return QUERY_TEMPLATE, params


@st.cache_data(ttl=AGGREGATE_CACHE_TTL_SECONDS, show_spinner=False)
def get_aggregate_data(conn_str, start_str, end_str):
    """The one and only SQL round trip for the report. Cached for
    AGGREGATE_CACHE_TTL_SECONDS (6 hours), keyed on the connection
    string + date range. Every Vendor/Category/Company tweak below
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
    WHERE p.categ_id_name IS NOT NULL
      AND LOWER(p.categ_id_name) NOT LIKE '%admin%'
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
        'Wedtree eStore Private Limited - HO'
    )
      AND p.category IS NOT NULL
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

SALES_SQL = PRODUCT_BASE + """
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
    s.sale_qty,
    s.sale_value,
    COALESCE(pi.image_1920, '') AS image_1920
FROM sales_detail s
LEFT JOIN product_base p
    ON s.product_id = p.product_id
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
    text = str(value)
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
        _detail_row("Days since sale", safe_html(row.get("sale_age", ""))),
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


def appearance_controls(qp):
    with st.sidebar.expander("Display settings"):
        default = int(qp.get("r_header_size", "16")) if str(qp.get("r_header_size", "16")).isdigit() else 16
        size = st.slider("Table and card header size", 12, 24, max(12, min(24, default)), key="header_size")
    st.html(f"<style>.report-table th {{font-size:{size}px;}} .product-title {{font-size:{size}px;}} "
            ".hero h1,.drill-hero h1 {font-size:28px;} "
            ".report-table.has-category {table-layout:fixed;} "
            ".report-table .pin-category {position:sticky;left:0;z-index:2;width:260px;box-sizing:border-box;} "
            ".report-table .pin-vendor {position:sticky;left:260px;z-index:2;width:180px;box-sizing:border-box;} "
            ".report-table th {white-space:normal;overflow-wrap:anywhere;} "
            ".report-table td {overflow:hidden;text-overflow:ellipsis;} "
            ".report-table td.dimension-cell {white-space:normal;overflow-wrap:anywhere;vertical-align:top;} "
            ".report-table td.pin-category,.report-table td.pin-vendor {background:var(--st-bg2);} "
            ".report-table th.pin-category,.report-table th.pin-vendor {background:#560835;z-index:3;} "
            ".report-table th {position:sticky;top:0;} </style>")


def options(df, column, null_label=None):
    values = sorted(df[column].dropna().astype(str).unique().tolist())
    if null_label is not None and df[column].isna().any():
        values.insert(0, null_label)
    return values


def select_filter(label, values, key, defaults=()):
    if key not in st.session_state:
        st.session_state[key] = [value for value in defaults if value in values]
    else:
        st.session_state[key] = [value for value in st.session_state[key] if value in values]
    return st.sidebar.multiselect(label, values, key=key)


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


def render_group_table(frame, keys, meta, state, sort_field, ascending):
    grouped = aggregate_report(frame, keys)
    grouped = grouped.sort_values([sort_field] + [key for key in keys if key != sort_field],
                                 ascending=[ascending] + [True for key in keys if key != sort_field],
                                 na_position="last", kind="stable")
    headers = [{"categ_id_name": "Category", "Vendor": "Vendor", "company_id_name": "Store", "master_category": "Master category"}[key] for key in keys]
    headers += ["Sold qty", "Sales value", "Available qty", "Available value", "Inward qty", "Inward value"]
    table_class = "report-table has-category" if "categ_id_name" in keys else "report-table"
    dimension_widths = {"categ_id_name": 260, "Vendor": 180, "master_category": 180, "company_id_name": 300}
    widths = [dimension_widths[key] for key in keys] + [125, 160, 125, 160, 125, 160]
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
                kind = "sales" if metric == "sale_qty" else "available"
                url = scoped_drill_url(kind, scope, meta, state)
                html.append(f'<td><a class="qty-link" href="{safe_html(url)}" target="_self">{value:,.2f}</a></td>')
            elif metric.endswith("value"):
                html.append(f"<td>₹{value:,.2f}</td>")
            else:
                html.append(f"<td>{value:,.2f}</td>")
        html.append("</tr>")
    for _, row in grouped.iterrows():
        mask = pd.Series(True, index=frame.index)
        for key in keys:
            mask &= frame[key].isna() if pd.isna(row[key]) else frame[key].eq(row[key])
        add_row([row[key] if pd.notna(row[key]) else "(Not recorded)" for key in keys], row, frame[mask])
    if not frame.empty:
        add_row(["Total"] + [""] * (len(keys) - 1), frame[METRICS].sum(), frame)
    html.append("</tbody></table></div>")
    st.markdown("".join(html), unsafe_allow_html=True)
    return grouped


def render_hierarchy_table(frame, meta, state, sort_field, ascending):
    """Expand master/category rows in the same HTML table, without a rerun."""
    headers = ["Master category / Category / Vendor", "Sold qty", "Sales value",
               "Available qty", "Available value", "Inward qty", "Inward value"]
    style = ["""
        .pivot-hierarchy .category-row,.pivot-hierarchy .vendor-row {display:none;}
        .pivot-hierarchy tbody:has(.master-toggle[open]) .category-row {display:table-row;}
        .pivot-hierarchy .tree-cell {min-width:320px;}
        .pivot-hierarchy .category-row .tree-cell {padding-left:32px;}
        .pivot-hierarchy .vendor-row .tree-cell {padding-left:60px;}
        .pivot-hierarchy summary {cursor:pointer;white-space:normal;}
        .pivot-hierarchy summary:focus-visible {outline:2px solid #b6871f;outline-offset:3px;}
        .pivot-hierarchy .master-row,.pivot-hierarchy .grand-total {font-weight:700;}
        .pivot-hierarchy .master-row td {background:var(--st-bg2);}
        .pivot-hierarchy .grand-total td {border-top:2px solid #b6871f;}
    """]
    markup = ['<div class="report-table-wrap"><table class="report-table pivot-hierarchy"><thead><tr>']
    markup.extend(f"<th>{safe_html(header)}</th>" for header in headers)
    markup.append("</tr></thead>")

    def ordered(group, key):
        rows = aggregate_report(group, [key])
        primary = sort_field if sort_field in METRICS else key
        fields = [primary] + ([key] if primary != key else [])
        return rows.sort_values(fields, ascending=[ascending] + ([True] if primary != key else []),
                                na_position="last", kind="stable")

    def row(label, totals, scope, row_class, toggle_id=None, toggle_class=""):
        markup.append(f'<tr class="{row_class}"><td class="tree-cell">')
        if toggle_id:
            markup.append(f'<details id="{toggle_id}" class="{toggle_class}"><summary>{safe_html(label)}</summary></details>')
        else:
            markup.append(safe_html(label))
        markup.append("</td>")
        for metric in METRICS:
            value = float(totals[metric])
            if metric in {"sale_qty", "available_qty"}:
                kind = "sales" if metric == "sale_qty" else "available"
                url = scoped_drill_url(kind, scope, meta, state)
                markup.append(f'<td><a class="qty-link" href="{safe_html(url)}" target="_self">{value:,.2f}</a></td>')
            else:
                prefix = "₹" if metric.endswith("value") else ""
                markup.append(f"<td>{prefix}{value:,.2f}</td>")
        markup.append("</tr>")

    for master_index, (_, master_row) in enumerate(ordered(frame, "master_category").iterrows()):
        master = master_row.master_category
        group = frame[frame.master_category.eq(master)]
        master_id = f"pivot-master-{master_index}"
        markup.append("<tbody>")
        row(master, master_row, group, "master-row", master_id, "master-toggle")
        for category_index, (_, category_row) in enumerate(ordered(group, "categ_id_name").iterrows()):
            category = category_row.categ_id_name
            category_group = group[group.categ_id_name.eq(category)]
            category_id = f"pivot-category-{master_index}-{category_index}"
            vendor_class = f"pivot-vendors-{master_index}-{category_index}"
            style.append(f".pivot-hierarchy tbody:has(#{master_id}[open]):has(#{category_id}[open]) .{vendor_class} {{display:table-row;}}")
            row(category, category_row, category_group, "category-row", category_id, "category-toggle")
            for _, vendor_row in ordered(category_group, "Vendor").iterrows():
                vendor = vendor_row.Vendor
                vendor_group = category_group[category_group.Vendor.isna() if pd.isna(vendor) else category_group.Vendor.eq(vendor)]
                row("(No vendor)" if pd.isna(vendor) else vendor, vendor_row, vendor_group,
                    f"vendor-row {vendor_class}")
        markup.append("</tbody>")
    markup.append("<tbody>")
    row("Grand total", frame[METRICS].sum(), frame, "grand-total")
    markup.append("</tbody></table></div>")
    st.html("<style>" + "".join(style) + "</style>")
    st.markdown("".join(markup), unsafe_allow_html=True)


def render_main_report():
    qp = st.query_params
    mapping = load_master_mapping()
    appearance_controls(qp)
    st.title("Sales, inward and inventory")
    st.caption("Master category → Category → Vendor · totals across selected stores")
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
    applied_start, applied_end = st.session_state.applied_dates
    date_label = f"{applied_start:%d %b %Y} – {applied_end:%d %b %Y}"
    with st.popover(date_label, icon=":material/calendar_month:"):
        preset = st.selectbox("Period", DATE_PRESETS, key="date_preset", on_change=reset_date_draft)
        if preset == "Custom range":
            pending = st.date_input("Start and end dates", value=st.session_state.applied_dates,
                                    max_value=today, key="custom_dates", format="DD/MM/YYYY")
        else:
            pending = date_bounds(preset, today)
            st.caption(f"{pending[0]:%d %b %Y} – {pending[1]:%d %b %Y}")
        apply = st.button("Apply", type="primary", disabled=len(pending) != 2)
        st.button("Cancel", on_click=cancel_dates)
    start_date, end_date = st.session_state.applied_dates
    if apply and len(pending) == 2:
        start_date, end_date = pending
    fetch_clicked = st.sidebar.button("Fetch data", type="primary")
    run_clicked = apply or fetch_clicked
    refresh = st.sidebar.button("Refresh applied range")
    if refresh:
        get_aggregate_data.clear()
        run_query.clear()
        run_clicked = True
    clear_clicked = st.sidebar.button("Clear data and image cache")
    if clear_clicked:
        get_aggregate_data.clear()
        run_query.clear()
        load_image_cached.clear()
        get_connection.clear()
        st.session_state.pop("raw_df", None)
        st.session_state.pop("raw_meta", None)
        st.sidebar.success("Cache cleared. Fetch data to reload.")
    auto_restore = not clear_clicked and "raw_df" not in st.session_state and bool(qp.get("r_start"))
    if (run_clicked or auto_restore) and connection_is_configured():
        with st.spinner("Loading report..."):
            try:
                conn_str = build_connection_string(SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID)
                t0 = time.time()
                raw = get_aggregate_data(conn_str, start_date.isoformat(), end_date.isoformat())
                st.session_state.raw_df = raw
                st.session_state.raw_meta = dict(start_date=start_date, end_date=end_date,
                                                fetched_at=datetime.now(), elapsed=time.time() - t0)
                st.session_state.applied_dates = (start_date, end_date)
                if apply:
                    # Redraw the closed date control with the newly applied dates.
                    st.rerun()
            except Exception as error:
                st.error(f"Report query failed: {error}")
    elif not connection_is_configured():
        st.info("Configure the SQL connection secrets to fetch report data.")
    raw = st.session_state.get("raw_df")
    if raw is None:
        st.info("Choose a date range and click Apply or Fetch data.")
        return
    meta = st.session_state.raw_meta
    st.caption(f"Applied period: {meta['start_date']:%d %b %Y} – {meta['end_date']:%d %b %Y} · Asia/Kolkata · week starts Monday")
    raw = add_master_category(raw, mapping, "categ_id_name")
    st.sidebar.subheader("Report filters")
    companies = select_filter("Store (company)", options(raw, "company_id_name", "(No store)"), "report_stores", _decode_list(qp.get("r_companies", "")))
    vendors = select_filter("Vendor", options(raw, "Vendor", NONE_VENDOR_LABEL), "report_vendors", _decode_list(qp.get("r_vendors", "")))
    masters = select_filter("Master category", options(raw, "master_category"), "report_masters", _decode_list(qp.get("r_masters", "")))
    category_source = raw[raw.master_category.isin(masters)] if masters else raw
    categories = select_filter("Category", options(category_source, "categ_id_name"), "report_categories", _decode_list(qp.get("r_categories", "")))
    mask = selected_mask(raw, "company_id_name", companies, "(No store)") & selected_mask(raw, "Vendor", vendors, NONE_VENDOR_LABEL) & selected_mask(raw, "master_category", masters) & selected_mask(raw, "categ_id_name", categories)
    df = raw[mask]
    state = build_report_state_params(meta["start_date"], meta["end_date"], companies, vendors, categories)
    state.update(r_masters=_encode_list(masters), r_header_size=str(st.session_state.header_size))
    view_options = ["Category hierarchy", "Store pivot", "Detailed rows"]
    if "report_view" not in st.session_state:
        st.session_state.report_view = qp.get("r_view", view_options[0]) if qp.get("r_view") in view_options else view_options[0]
    view = st.segmented_control("Report view", view_options, key="report_view") or view_options[0]
    state["r_view"] = view
    labels = {"categ_id_name": "Category", "Vendor": "Vendor", "sale_qty": "Sold quantity", "sale_value": "Sales value", "available_qty": "Available quantity", "available_value": "Available value", "inward_qty": "Inward quantity", "inward_value": "Inward value"}
    if "report_sort" not in st.session_state:
        st.session_state.report_sort = qp.get("r_sort", "categ_id_name") if qp.get("r_sort") in labels else "categ_id_name"
    if "report_direction" not in st.session_state:
        st.session_state.report_direction = qp.get("r_direction", "Ascending") if qp.get("r_direction") in {"Ascending", "Descending"} else "Ascending"
    controls = st.columns([3, 1])
    with controls[0]:
        sort_field = st.selectbox("Sort by", list(labels), format_func=labels.get, key="report_sort")
    with controls[1]:
        ascending = st.selectbox("Direction", ["Ascending", "Descending"], key="report_direction") == "Ascending"
    state.update(r_sort=sort_field, r_direction=st.session_state.report_direction)
    metrics = st.columns(4)
    for container, metric, label in zip(metrics, ["sale_value", "available_value", "inward_value", "sale_qty"], ["Sales value", "Available value", "Inward value", "Sold quantity"]):
        container.metric(label, f"₹{df[metric].sum():,.2f}" if metric.endswith("value") else f"{df[metric].sum():,.2f}")
    st.caption("Sales and inward use the applied period. Available stock is the current snapshot. Click sold or available quantities to open the separate product page.")
    unmapped = options(raw[raw.master_category.eq("Unmapped")], "categ_id_name")
    if unmapped:
        with st.expander(f"Unmapped categories ({len(unmapped)})"):
            st.write(unmapped)
    if df.empty:
        st.info("No rows match these filters.")
        return
    if view == "Category hierarchy":
        st.caption("Use the arrow inside a master-category row to expand categories, then expand a category for vendors. Totals sum all selected stores.")
        render_hierarchy_table(df, meta, state, sort_field, ascending)
    elif view == "Store pivot":
        metric = st.selectbox("Pivot measure", METRICS, format_func=lambda v: labels[v])
        pivot = df.groupby(["master_category", "categ_id_name", "company_id_name"], dropna=False)[metric].sum().unstack("company_id_name", fill_value=0)
        pivot["Total"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("Total", ascending=ascending) if sort_field in METRICS else pivot.sort_index(ascending=ascending)
        st.dataframe(pivot, width="stretch")
        st.caption(f"Grand total: {df[metric].sum():,.2f}. Use the category totals below for drill-through.")
        render_group_table(df, ["categ_id_name"], meta, state, sort_field if sort_field != "Vendor" else "categ_id_name", ascending)
        st.download_button("Download pivot CSV", pivot.to_csv().encode("utf-8-sig"), "store_pivot.csv", "text/csv")
    else:
        # Sort before paging; totals below remain for the whole filtered result.
        detail_keys = ["categ_id_name", "Vendor", "master_category", "company_id_name"]
        detailed = aggregate_report(df, detail_keys)
        detailed = detailed.sort_values([sort_field] + [k for k in detail_keys if k != sort_field], ascending=ascending, na_position="last", kind="stable")
        page_size = st.selectbox("Rows per page", [25, 50, 100], key="report_page_size")
        pages = max(1, (len(detailed) + page_size - 1) // page_size)
        page_signature = str((tuple(df.index), sort_field, ascending, page_size))
        page_key = "report_page_" + hashlib.sha256(page_signature.encode()).hexdigest()[:16]
        page = st.selectbox("Page", list(range(1, pages + 1)), key=page_key)
        page_rows = detailed.iloc[(page - 1) * page_size:page * page_size]
        page_keys = page_rows[detail_keys]
        page_facts = df.merge(page_keys, on=detail_keys, how="inner")
        render_group_table(page_facts, detail_keys, meta, state, sort_field, ascending)
        st.caption(f"Page {page}/{pages}. Table total is this page; filtered grand total follows.")
        render_group_table(df, ["master_category"], meta, state, "master_category", True)
    st.download_button("Download filtered data CSV", df.to_csv(index=False).encode("utf-8-sig"), "category_report.csv", "text/csv")
    with st.expander("Top vendors by sales value", key="top_vendors", on_change="rerun") as vendor_chart:
        if vendor_chart.open:
            top_vendors = df.groupby("Vendor", dropna=False).sale_value.sum().sort_values(ascending=False).head(10)
            top_vendors.index = top_vendors.index.fillna("(No vendor)")
            st.bar_chart(top_vendors)


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
        reference_name = st.sidebar.selectbox("Days since sale reference", ["Today", "Report end date"])
        reference = business_today() if reference_name == "Today" else end
        frame = frame.copy()
        frame["sold_date"] = pd.to_datetime(frame["sold_date"], errors="coerce")
        frame[age_column] = (pd.Timestamp(reference) - frame.sold_date).dt.days
        frame.attrs["age_reference"] = reference
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
    st.caption("Combined stock age is the oldest receipt age; combined sold date is the latest sale. Age filters apply to the underlying detail rows before totals are combined.")
    qty = "sale_qty" if kind == "sales" else "available_inventory"
    value = "sale_value" if kind == "sales" else "available_selling_price"
    a, b, c = st.columns(3)
    a.metric("Products", f"{frame.product_id.nunique():,}")
    b.metric("Sold quantity" if kind == "sales" else "Available quantity", f"{frame[qty].sum():,.2f}")
    c.metric("Sales value" if kind == "sales" else "Available value", f"₹{frame[value].sum():,.2f}")
    sort_labels = {age_column: "Days since sale" if kind == "sales" else "Stock age (days)", value: "Sales value" if kind == "sales" else "Available value", qty: "Sold quantity" if kind == "sales" else "Available quantity", "product_name": "Product name", "sku": "SKU"}
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
                            st.dataframe(rows[detail_columns], hide_index=True)
    st.caption(f"Page {page}/{page_count} · {len(visible)} of {len(display):,} cards. Totals include all matching pages.")
    st.download_button("Download product details CSV", frame.drop(columns=["image_1920"], errors="ignore").to_csv(index=False).encode("utf-8-sig"), f"{kind}_products.csv", "text/csv")


# =====================================================================
# Router
# =====================================================================

_qp = st.query_params

if _qp.get("drill"):
    render_drillthrough(_qp)
else:
    render_main_report()
