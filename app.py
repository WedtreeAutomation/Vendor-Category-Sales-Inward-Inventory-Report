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

Routing (no separate pages/files needed):
    no `drill` query param   -> main report view
    `drill=sales|available`  -> drill-through product-card view

Set these in Streamlit secrets:
    SQL_ENDPOINT, DATABASE, TENANT_ID, CLIENT_ID, CLIENT_SECRET

Image loading uses the same service principal to read OneLake via the
Azure Data Lake SDK. The service principal must have Fabric workspace
read access plus OneLake data access to the workspace holding the
Bronze lakehouse. Requires: azure-identity, azure-storage-filedatalake,
Pillow.
"""

import base64
import io
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
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

    @media (prefers-color-scheme: dark) {
        :root {
            --st-bg: #14100e;
            --st-bg2: #1c1815;
            --st-text: #f3e8d9;
            --muted: rgba(243,232,217,.65);
            --gold: #d8b34c;
            --on-accent: #2b0a1c;
            --accent-chip: #b66289;
            --wine: #b66289;
            --wine2: #cf6a91;
            --card-border: #3a2f2a;
            --card-bg: #1d1f21;
            --card-shadow: rgba(0,0,0,.4);
            --sidebar-border: #3a2f2a;
            --select-border: #4a3a30;
            --pill-border: #3a2f2a;
            --notice-bg-1: #2b1e13;
            --notice-bg-2: #2a2415;
            --notice-border: #8a6a1f;
            --notice-title: #f0c060;
            --notice-text: #f3e8d9;
            --stock-bg: #12351f;
            --stock-fg: #9fe1b2;
            --border: #3a2f2a;
        }
    }

    [data-theme="dark"] {
        --st-bg: #14100e;
        --st-bg2: #1c1815;
        --st-text: #f3e8d9;
        --muted: rgba(243,232,217,.65);
        --gold: #d8b34c;
        --on-accent: #2b0a1c;
        --accent-chip: #b66289;
        --wine: #b66289;
        --wine2: #cf6a91;
        --card-border: #3a2f2a;
        --card-bg: #1d1f21;
        --card-shadow: rgba(0,0,0,.4);
        --sidebar-border: #3a2f2a;
        --select-border: #4a3a30;
        --pill-border: #3a2f2a;
        --notice-bg-1: #2b1e13;
        --notice-bg-2: #2a2415;
        --notice-border: #8a6a1f;
        --notice-title: #f0c060;
        --notice-text: #f3e8d9;
        --stock-bg: #12351f;
        --stock-fg: #9fe1b2;
        --border: #3a2f2a;
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
WHERE {vendor_clause}
  AND {category_clause}
  AND {company_clause}
GROUP BY
    vendor,
    category,
    company
ORDER BY
    vendor,
    category,
    company;
"""


def build_in_clause(column, values, params):
    if not values:
        return "1=1"
    placeholders = ",".join(["?"] * len(values))
    params.extend(values)
    return f"{column} IN ({placeholders})"


def build_vendor_clause(values, params):
    if not values:
        return "1=1"

    include_none = NONE_VENDOR_LABEL in values
    real = [v for v in values if v != NONE_VENDOR_LABEL]
    conditions = []

    if real:
        placeholders = ",".join(["?"] * len(real))
        params.extend(real)
        conditions.append(f"vendor IN ({placeholders})")

    if include_none:
        conditions.append("vendor IS NULL")

    return "(" + " OR ".join(conditions) + ")" if conditions else "1=1"


def build_query_and_params(start_str, end_str, vendors, categories, companies):
    params = [start_str, end_str, start_str, end_str]
    vendor_clause = build_vendor_clause(vendors, params)
    category_clause = build_in_clause("category", categories, params)
    company_clause = build_in_clause("company", companies, params)

    sql = QUERY_TEMPLATE.format(
        vendor_clause=vendor_clause,
        category_clause=category_clause,
        company_clause=company_clause,
    )
    return sql, params


# =====================================================================
# Filter options
# =====================================================================

FILTER_OPTIONS_BATCH_SQL = """
SELECT DISTINCT categ_id_name
FROM WT_LH_Silver.Odoo.product_product
WHERE categ_id_name IS NOT NULL
  AND LOWER(categ_id_name) NOT LIKE '%admin%'
ORDER BY categ_id_name;

SELECT DISTINCT vendor_id_name
FROM WT_LH_Silver.Odoo.product_template
WHERE vendor_id_name IS NOT NULL
ORDER BY vendor_id_name;

SELECT DISTINCT company_id_name FROM (
    SELECT company_id_name FROM WT_LH_Silver.Odoo.pos_order
    UNION
    SELECT company_id_name FROM WT_LH_Silver.Odoo.stock_picking
    UNION
    SELECT company_id_name FROM WT_LH_Silver.Odoo.stock_quant_n1
) t
WHERE company_id_name IS NOT NULL
  AND company_id_name NOT IN ({excluded_sql})
ORDER BY company_id_name;
"""


@st.cache_data(ttl=3600, show_spinner=False)
def get_filter_options(conn_str):
    excluded_sql = ",".join(["?"] * len(EXCLUDED_COMPANIES))
    sql = FILTER_OPTIONS_BATCH_SQL.format(excluded_sql=excluded_sql)

    conn = get_live_connection(conn_str)
    cur = conn.cursor()
    cur.execute(sql, EXCLUDED_COMPANIES)

    categories = [r[0] for r in cur.fetchall()]
    cur.nextset()
    vendors = [r[0] for r in cur.fetchall()]
    cur.nextset()
    companies = [r[0] for r in cur.fetchall()]
    cur.close()

    companies = [
        c for c in companies
        if c not in COMPANIES_HIDDEN_FROM_FILTER
    ]
    vendors = [NONE_VENDOR_LABEL] + vendors

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
        CAST(po.picking_ids AS VARCHAR(MAX)),
        CAST(pol.product_id AS VARCHAR(50))
),
lot_moves AS (
    SELECT
        CAST(pk.id AS VARCHAR(50)) AS picking_id,
        CAST(sml.product_id AS VARCHAR(50)) AS product_id,
        NULLIF(sml.lot_id_name, 'False') AS lot_number,
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
        NULLIF(sml.lot_id_name, 'False')
),
sales_detail AS (
    SELECT
        ps.company,
        ps.product_id,
        COALESCE(lm.lot_number, 'No lot recorded') AS lot_number,
        COALESCE(lm.lot_qty, ps.sale_qty) AS sale_qty,
        CASE
            WHEN lm.lot_qty IS NULL THEN ps.sale_value
            WHEN ps.sale_qty <> 0 THEN ps.sale_value * lm.lot_qty / ps.sale_qty
            ELSE 0
        END AS sale_value
    FROM pos_sales ps
    LEFT JOIN lot_moves lm
        ON (',' + REPLACE(REPLACE(REPLACE(ps.picking_ids, '[', ''), ']', ''), ' ', '') + ',')
           LIKE '%,' + lm.picking_id + ',%'
       AND lm.product_id = ps.product_id
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


def build_report_state_params(result):
    """The applied-filter state of the main report, packed so it can be
    carried through a drill-through link and back again. A drill-through
    click is a full page navigation (new Streamlit session), so this has
    to live in the URL rather than st.session_state to survive it."""
    return {
        "r_start": result["start_date"].isoformat(),
        "r_end": result["end_date"].isoformat(),
        "r_companies": _encode_list(result["companies"]),
        "r_vendors": _encode_list(result["vendors"]),
        "r_categories": _encode_list(result["categories"]),
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
        _detail_row("SKU", safe_html(row.get("sku", ""))),
        _detail_row("Lot No.", safe_html(row.get("lot_number", ""))),
        _detail_row("Sold Qty", f"{sale_qty:g}"),
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
            results = load_images_parallel(client, refs)
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

def render_main_report():
    st.markdown(
        '<div class="hero">'
        '<h1>📊 Vendor / Category Sales, Inward &amp; Inventory Report</h1>'
        '<p>Live data from the Fabric Lakehouse — filtered by the panel on the left.</p>'
        '<div class="rule"></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # If we arrived here via "← Back to Report" from a drill-through page,
    # the previously applied filters were carried in the URL (r_start,
    # r_end, r_companies, ...). Read them so we can pre-fill the widgets
    # and re-run the same report automatically, instead of showing the
    # empty "click Fetch Data" state.
    qp = st.query_params
    restore_start_str = qp.get("r_start", "")
    restore_end_str = qp.get("r_end", "")
    restore_companies = _decode_list(qp.get("r_companies", ""))
    restore_vendors = _decode_list(qp.get("r_vendors", ""))
    restore_categories = _decode_list(qp.get("r_categories", ""))
    has_restore_state = bool(restore_start_str and restore_end_str)

    try:
        default_start_date = (
            date.fromisoformat(restore_start_str) if restore_start_str else date(2026, 8, 1)
        )
    except Exception:
        default_start_date = date(2026, 8, 1)

    try:
        default_end_date = (
            date.fromisoformat(restore_end_str) if restore_end_str else date(2026, 8, 9)
        )
    except Exception:
        default_end_date = date(2026, 8, 9)

    st.sidebar.markdown("## 🔎 Filters")
    st.sidebar.markdown("---")

    st.sidebar.markdown("**📅 Date Range**")
    start_date = st.sidebar.date_input("Start Date", value=default_start_date)
    end_date = st.sidebar.date_input("End Date", value=default_end_date)

    st.sidebar.markdown("---")

    if not connection_is_configured():
        st.sidebar.warning(
            "Connection details aren't configured. Add SQL_ENDPOINT, DATABASE, "
            "TENANT_ID, CLIENT_ID and CLIENT_SECRET to Streamlit secrets."
        )
        company_options, vendor_options, category_options = [], [], []
    else:
        try:
            conn_str_for_options = build_connection_string(
                SQL_ENDPOINT,
                DATABASE,
                CLIENT_ID,
                CLIENT_SECRET,
                TENANT_ID,
            )
            with st.sidebar:
                with st.spinner("Loading filters..."):
                    company_options, vendor_options, category_options = get_filter_options(
                        conn_str_for_options
                    )
        except Exception as e:
            st.sidebar.error(f"Couldn't load filter options: {e}")
            company_options, vendor_options, category_options = [], [], []

    st.sidebar.markdown("**🏢 Company**")
    # selected_companies = st.sidebar.multiselect(
    #     "Company",
    #     options=company_options,
    #     default=[c for c in restore_companies if c in company_options],
    #     label_visibility="collapsed",
    # )

    company_display_map = {
    c: c.split(" - ", 1)[-1] if " - " in c else c
    for c in company_options
}

    selected_display = st.sidebar.multiselect(
        "Company",
        options=list(company_display_map.values()),
        default=[
            company_display_map[c]
            for c in restore_companies
            if c in company_display_map
        ],
        label_visibility="collapsed",
    )

    selected_companies = [
        company
        for company, display in company_display_map.items()
        if display in selected_display
    ]

    st.sidebar.markdown("**🏷️ Vendor**")
    selected_vendors = st.sidebar.multiselect(
        "Vendor",
        options=vendor_options,
        default=[v for v in restore_vendors if v in vendor_options],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("**📦 Category**")
    selected_categories = st.sidebar.multiselect(
        "Category",
        options=category_options,
        default=[c for c in restore_categories if c in category_options],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("---")
    run_clicked = st.sidebar.button(
        "▶️  Fetch Data",
        type="primary",
        use_container_width=True,
    )

    col_a, col_b = st.sidebar.columns(2)

    with col_a:
        if st.button("↻ Refresh filters", use_container_width=True):
            get_filter_options.clear()
            st.rerun()

    with col_b:
        if st.button("🗑️ Clear cache", use_container_width=True):
            run_query.clear()
            get_connection.clear()
            get_filter_options.clear()
            load_image_cached.clear()
            st.sidebar.info("Cache cleared.")

    query_sql, query_params = build_query_and_params(
        start_date.isoformat(),
        end_date.isoformat(),
        selected_vendors,
        selected_categories,
        selected_companies,
    )

    if "result" not in st.session_state:
        st.session_state["result"] = None

    if "page" not in st.session_state:
        st.session_state["page"] = 1

    if "page_size" not in st.session_state:
        st.session_state["page_size"] = 25

    # Auto-restore: this is a fresh session (e.g. we just navigated back
    # from a drill-through page) and the URL says a report was already
    # applied - re-run it automatically instead of waiting for another
    # "Fetch Data" click.
    auto_restore = (
        not run_clicked
        and st.session_state["result"] is None
        and has_restore_state
        and connection_is_configured()
    )

    if run_clicked or auto_restore:
        if start_date > end_date:
            st.error("Start Date must be on or before End Date.")
        elif not connection_is_configured():
            st.error("Connection details aren't configured.")
        else:
            conn_str = build_connection_string(
                SQL_ENDPOINT,
                DATABASE,
                CLIENT_ID,
                CLIENT_SECRET,
                TENANT_ID,
            )

            t0 = time.time()

            with st.spinner("Running query against the Lakehouse..."):
                try:
                    df = run_query(conn_str, query_sql, query_params)

                    st.session_state["result"] = {
                        "df": df,
                        "start_date": start_date,
                        "end_date": end_date,
                        "companies": selected_companies,
                        "vendors": selected_vendors,
                        "categories": selected_categories,
                        "elapsed": time.time() - t0,
                    }
                    st.session_state["page"] = 1
                except Exception as e:
                    st.error(f"Query failed: {e}")

    result = st.session_state["result"]

    if result is not None:
        df = result["df"]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Rows", f"{len(df):,}")
        k2.metric("Sale Value", f"₹{df['sale_value'].sum():,.0f}")
        k3.metric("Inward Value", f"₹{df['inward_value'].sum():,.0f}")
        k4.metric("Available Value", f"₹{df['available_value'].sum():,.0f}")

        st.caption(f"Fetched in {result['elapsed']:.2f}s")

        st.markdown(
            '<div class="notice-card">'
            '<div class="notice-icon">📦</div>'
            '<div>'
            '<div class="notice-title">Inventory Snapshot Notice</div>'
            '<div class="notice-body">'
            '<b>Quantity</b> and <b>stock value</b> figures reflect '
            '<b>current inventory on hand</b> — they are <u>not</u> '
            'scoped to the selected date range. Only Sales and '
            'Inward figures are filtered by date.'
            '</div>'
            '</div>'
            '</div>',
            unsafe_allow_html=True,
        )

        filter_pills = []

        if result["companies"]:
            filter_pills += [
                f'<span class="pill">🏢 {safe_html(c)}</span>'
                for c in result["companies"]
            ]

        if result["vendors"]:
            filter_pills += [
                f'<span class="pill">🏷️ {safe_html(v)}</span>'
                for v in result["vendors"]
            ]

        if result["categories"]:
            filter_pills += [
                f'<span class="pill">📦 {safe_html(c)}</span>'
                for c in result["categories"]
            ]

        if filter_pills:
            st.markdown(
                f'<div class="filter-pill-row">{"".join(filter_pills)}</div>',
                unsafe_allow_html=True,
            )

        st.markdown("### Report")
        st.caption(
            "Click a non-zero **Sale Qty** or **Available Qty** to drill through "
            "to the respective product cards."
        )

        total_rows = len(df)

        page_size_options = [10, 25, 50, 100, 250, 500]
        current_page_size = st.session_state["page_size"]
        if current_page_size not in page_size_options:
            page_size_options.append(current_page_size)
            page_size_options.sort()

        ctl1, ctl2, ctl3, ctl4 = st.columns([2, 2, 1, 1])

        with ctl1:
            new_page_size = st.selectbox(
                "Rows per page",
                options=page_size_options,
                index=page_size_options.index(current_page_size),
                key="page_size_selector",
            )
            if new_page_size != current_page_size:
                st.session_state["page_size"] = new_page_size
                st.session_state["page"] = 1
                st.rerun()

        total_pages = max(1, (total_rows + st.session_state["page_size"] - 1) // st.session_state["page_size"])

        if st.session_state["page"] > total_pages:
            st.session_state["page"] = total_pages

        page = st.session_state["page"]
        page_size = st.session_state["page_size"]

        with ctl2:
            st.markdown(
                f'<div class="pagination-info">Page {page} of {total_pages} '
                f'· {total_rows:,} rows</div>',
                unsafe_allow_html=True,
            )

        with ctl3:
            if st.button("← Prev", use_container_width=True, disabled=(page <= 1)):
                st.session_state["page"] = page - 1
                st.rerun()

        with ctl4:
            if st.button("Next →", use_container_width=True, disabled=(page >= total_pages)):
                st.session_state["page"] = page + 1
                st.rerun()

        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        page_df = df.iloc[start_idx:end_idx]

        render_report_table(
            page_df,
            result["start_date"],
            result["end_date"],
            build_report_state_params(result),
        )

        st.caption(
            f"Showing rows {start_idx + 1:,}–{min(end_idx, total_rows):,} "
            f"of {total_rows:,}"
        )

        b1, b2, b3, b4, b5 = st.columns([2, 2, 2, 1, 1])

        with b4:
            if st.button("← Prev ", use_container_width=True, disabled=(page <= 1), key="prev_bottom"):
                st.session_state["page"] = page - 1
                st.rerun()

        with b5:
            if st.button("Next → ", use_container_width=True, disabled=(page >= total_pages), key="next_bottom"):
                st.session_state["page"] = page + 1
                st.rerun()

        if "sale_value" in df and "Vendor" in df and df["sale_value"].sum() > 0:
            top_vendors = (
                df.groupby("Vendor", dropna=False)["sale_value"]
                .sum()
                .sort_values(ascending=False)
                .head(8)
            )
            top_vendors.index = top_vendors.index.fillna("(No vendor)")

            with st.expander("📈 Top vendors by sale value", expanded=False):
                st.bar_chart(top_vendors)

        csv_bytes = df.to_csv(index=False).encode("utf-8")

        st.download_button(
            "⬇️ Download CSV",
            data=csv_bytes,
            file_name=(
                f"vendor_category_report_"
                f"{result['start_date']}_{result['end_date']}.csv"
            ),
            mime="text/csv",
        )
    else:
        st.info(
            "Set your filters on the left, then click **Fetch Data** to load the report."
        )


# =====================================================================
# View: Drill-through
# =====================================================================

def render_drillthrough(qp):
    drill_type = _clean(qp.get("drill", "available")) or "available"
    company = _clean(qp.get("company", ""))
    vendor = _clean(qp.get("vendor", ""))
    category = _clean(qp.get("category", ""))
    start_date_str = _clean(qp.get("start_date", ""))
    end_date_str = _clean(qp.get("end_date", ""))

    try:
        start_date = date.fromisoformat(start_date_str)
    except Exception:
        start_date = date.today()

    try:
        end_date = date.fromisoformat(end_date_str)
    except Exception:
        end_date = date.today()

    if drill_type not in {"sales", "available"}:
        drill_type = "available"

    # Relative link back to the main view - same app, no MAIN_APP_URL needed.
    # Carry the r_* (applied report filter) params forward so the main
    # view can auto-restore the previously fetched report instead of
    # showing the empty "click Fetch Data" state.
    back_state = {k: v for k, v in dict(qp).items() if k.startswith("r_")}
    back_href = f"?{urlencode(back_state)}" if back_state else "?"
    st.markdown(
        f'<a class="back-link" href="{back_href}" target="_self">← Back to Report</a>',
        unsafe_allow_html=True,
    )

    if not connection_is_configured():
        st.error(
            "SQL connection secrets are not configured. "
            "Configure SQL_ENDPOINT, DATABASE, TENANT_ID, CLIENT_ID and CLIENT_SECRET."
        )
        st.stop()

    try:
        conn_str = build_connection_string(
            SQL_ENDPOINT,
            DATABASE,
            CLIENT_ID,
            CLIENT_SECRET,
            TENANT_ID,
        )
    except Exception as e:
        st.error(str(e))
        st.stop()

    if drill_type == "available":
        title = "📦 Available Inventory"
        subtitle = "Current inventory for the selected Vendor / Category / Company."

        params = [
            company,  company,
            vendor,   vendor,
            category, category,
        ]
        sql = AVAILABLE_SQL
    else:
        title = "🛍️ Sold Products"
        subtitle = (
            f"Products sold between {start_date:%d-%b-%Y} and "
            f"{end_date:%d-%b-%Y}."
        )

        params = [
            start_date.isoformat(),
            end_date.isoformat(),
            company,  company,
            vendor,   vendor,
            category, category,
        ]
        sql = SALES_SQL

    with st.spinner("Loading products..."):
        try:
            # Same run_query cache as the main report - if the main
            # report already pulled overlapping data this session, or
            # another user already drilled into the same combination,
            # this can be a cache hit with zero DB round trip.
            df = run_query(conn_str, sql, params)
        except Exception as e:
            st.error(f"Drill-through query failed: {e}")
            st.stop()

    st.markdown(
        f'<div class="drill-hero"><h1>{title}</h1><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="context-row">'
        f'<span class="context-chip">🏢 {company or "All companies"}</span>'
        f'<span class="context-chip">🏷️ {vendor or "All vendors"}</span>'
        f'<span class="context-chip">📦 {category or "All categories"}</span>'
        '</div>',
        unsafe_allow_html=True,
    )

    if drill_type == "sales":
        total_qty = df["sale_qty"].sum() if not df.empty else 0
        total_value = df["sale_value"].sum() if not df.empty else 0

        st.markdown(
            '<div class="summary-box">'
            f'<strong>Sales drill-through:</strong> '
            f'{len(df):,} products · '
            f'{total_qty:,.0f} units sold · '
            f'₹{total_value:,.0f} sales value'
            '</div>',
            unsafe_allow_html=True,
        )
    else:
        total_qty = df["available_inventory"].sum() if not df.empty else 0
        total_value = (
            df["available_selling_price"].sum()
            if not df.empty
            else 0
        )

        st.markdown(
            '<div class="summary-box">'
            f'<strong>Inventory drill-through:</strong> '
            f'{len(df):,} inventory rows · '
            f'{total_qty:,.0f} units currently available · '
            f'₹{total_value:,.0f} current stock value'
            '</div>',
            unsafe_allow_html=True,
        )

    if df.empty:
        st.warning("No products were found for this drill-through selection.")

        with st.expander("🔍 Debug information", expanded=True):
            st.write("**Drill type:**", drill_type)
            st.write("**Company:**", repr(company))
            st.write("**Vendor:**", repr(vendor))
            st.write("**Category:**", repr(category))
            st.write("**Start date:**", repr(start_date_str))
            st.write("**End date:**", repr(end_date_str))
            st.write("**Raw query params:**", dict(qp))
            st.caption(
                "If a value shows as 'None' or has trailing spaces, a "
                "malformed parameter was sent. If the values look correct, "
                "the underlying table has no rows for this combination."
            )

        st.stop()
    #Fix here for image drill through
    # ---------------------------------------------------------------
    # Resolve abfss:// images server-side via the Azure SDK.
    # Adds a '_image_data_uri' column with data:image/...;base64,... URIs.
    # ---------------------------------------------------------------
    with st.spinner("Loading images..."):
        df = resolve_images_for_df(df)

    #     st.subheader("Debug - Before Image Resolution")

    # st.write(
    #     df[["product_id", "image_1920"]].head(10)
    # )

    # with st.spinner("Loading images..."):
    #     df = resolve_images_for_df(df)

    # st.subheader("Debug - After Image Resolution")

    # st.write(
    #     df[["product_id", "image_1920", "_image_data_uri"]].head(10)
    # )

    # st.write(
    #     "Resolved Images:",
    #     df["_image_data_uri"].notna().sum()
    # )

    CARDS_PER_ROW = 4

    for start in range(0, len(df), CARDS_PER_ROW):
        chunk = df.iloc[start:start + CARDS_PER_ROW]
        cols = st.columns(CARDS_PER_ROW)

        for col, (_, row) in zip(cols, chunk.iterrows()):
            with col:
                if drill_type == "available":
                    st.markdown(
                        render_available_card(row),
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        render_sales_card(row),
                        unsafe_allow_html=True,
                    )

    st.caption(f"Showing {len(df):,} product rows.")

    # Optional debug expander - remove once images are confirmed working.
    if st.session_state.get("_show_image_debug"):
        with st.expander("🐞 Image debug", expanded=True):
            n_abfss = sum(
                1 for r in df["image_1920"].fillna("").astype(str)
                if r.startswith("abfss://")
            )
            n_resolved = df["_image_data_uri"].notna().sum()
            st.write("Rows:", len(df))
            st.write("Rows with abfss:// ref:", n_abfss)
            st.write("Rows resolved to data URI:", int(n_resolved))
            first = df["image_1920"].fillna("").astype(str)
            first = first[first.str.startswith("abfss://")]
            if not first.empty:
                st.code(first.iloc[0], language="text")


# =====================================================================
# Router
# =====================================================================

_qp = st.query_params

if _qp.get("drill"):
    render_drillthrough(_qp)
else:
    render_main_report()
