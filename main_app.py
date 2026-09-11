"""
main_app.py
Vendor / Category Sales, Inward & Inventory Report

Main Streamlit application.

The report remains aggregated at:
    Company + Vendor + Category

Sale Qty and Available Qty are drill-through links.
The drill-through page is a separate Streamlit app.

Set DRILL_APP_URL in Streamlit secrets, for example:
DRILL_APP_URL = "http://localhost:8502"
"""

import time
from datetime import date
from urllib.parse import urlencode

import pandas as pd
import pyodbc
import streamlit as st

st.set_page_config(
    page_title="Vendor / Category Report",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------------------------------------------------
# Secrets
# -------------------------------------------------------------------

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

DRILL_APP_URL = _get_secret("DRILL_APP_URL") or "http://localhost:8502"

EXCLUDED_COMPANIES = [
    "Saree Trails",
    "Wedtree eStore Private Limited - HO",
]
COMPANIES_HIDDEN_FROM_FILTER = [
    "Wedtree eStore Private Limited - Online",
]
NONE_VENDOR_LABEL = "None"

# -------------------------------------------------------------------
# Styling
# -------------------------------------------------------------------

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700;800&family=Inter:wght@400;500;600;700&display=swap');

    :root {
        --st-bg: #fffaf4;
        --st-bg2: #fdf8f2;
        --st-text: #3a1f28;
        --gold: #b6871f;
        --on-accent: #fff8ec;
        --accent-chip: #8a1055;
        --card-border: #f0e2cd;
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
    }

    @media (prefers-color-scheme: dark) {
        :root {
            --st-bg: #14100e;
            --st-bg2: #1c1815;
            --st-text: #f3e8d9;
            --gold: #d8b34c;
            --on-accent: #2b0a1c;
            --accent-chip: #b66289;
            --card-border: #3a2f2a;
            --card-shadow: rgba(0,0,0,.4);
            --sidebar-border: #3a2f2a;
            --select-border: #4a3a30;
            --pill-border: #3a2f2a;
            --notice-bg-1: #2b1e13;
            --notice-bg-2: #2a2415;
            --notice-border: #8a6a1f;
            --notice-title: #f0c060;
            --notice-text: #f3e8d9;
        }
    }

    [data-theme="dark"] {
        --st-bg: #14100e;
        --st-bg2: #1c1815;
        --st-text: #f3e8d9;
        --gold: #d8b34c;
        --on-accent: #2b0a1c;
        --accent-chip: #b66289;
        --card-border: #3a2f2a;
        --card-shadow: rgba(0,0,0,.4);
        --sidebar-border: #3a2f2a;
        --select-border: #4a3a30;
        --pill-border: #3a2f2a;
        --notice-bg-1: #2b1e13;
        --notice-bg-2: #2a2415;
        --notice-border: #8a6a1f;
        --notice-title: #f0c060;
        --notice-text: #f3e8d9;
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

    .hero {
        background: linear-gradient(120deg,#560835 0%,#7c1049 55%,#37041f 100%);
        border-radius: 20px;
        padding: 26px 32px;
        margin-bottom: 22px;
        box-shadow: 0 10px 30px var(--hero-shadow);
        position: relative;
        overflow: hidden;
    }

    .hero h1 {
        font-family: 'Playfair Display', serif;
        color: #fff8ec;
        font-size: 2.1rem;
        font-weight: 800;
        margin: 0 0 4px 0;
    }

    .hero p {
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

    .filter-pill-row span.pill {
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

    .notice-card {
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
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------------------------------------------------
# Connection
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Main report SQL
# -------------------------------------------------------------------

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


# -------------------------------------------------------------------
# Filter options
# -------------------------------------------------------------------

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


@st.cache_data(ttl=600, show_spinner=False)
def run_query(conn_str, sql, params):
    conn = get_live_connection(conn_str)
    cursor = conn.cursor()
    cursor.execute(sql, params)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return pd.DataFrame.from_records(rows, columns=columns)


# -------------------------------------------------------------------
# Drill-through URL
# -------------------------------------------------------------------

def _clean_param(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "null", "nan"}:
        return ""
    return s


def make_drill_url(drill_type, row, start_date, end_date):
    params = {
        "drill": drill_type,
        "company":  _clean_param(row["company_id_name"]),
        "vendor":   _clean_param(row["Vendor"]),
        "category": _clean_param(row["categ_id_name"]),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
    }
    return f"{DRILL_APP_URL.rstrip('/')}/?{urlencode(params)}"


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


def render_report_table(df, start_date, end_date):
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
            f'<a class="qty-link" href="{safe_html(make_drill_url("sales", r, start_date, end_date))}" target="_self">{int(round(sale_qty))}</a>'
            if float(sale_qty) != 0
            else '<span class="zero-qty">0</span>'
        )

        available_link = (
            f'<a class="qty-link" href="{safe_html(make_drill_url("available", r, start_date, end_date))}" target="_self">{int(round(available_qty))}</a>'
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


# -------------------------------------------------------------------
# Hero
# -------------------------------------------------------------------

st.markdown(
    '<div class="hero">'
    '<h1>📊 Vendor / Category Sales, Inward &amp; Inventory Report</h1>'
    '<p>Live data from the Fabric Lakehouse — filtered by the panel on the left.</p>'
    '<div class="rule"></div>'
    '</div>',
    unsafe_allow_html=True,
)

# -------------------------------------------------------------------
# Sidebar
# -------------------------------------------------------------------

st.sidebar.markdown("## 🔎 Filters")
st.sidebar.markdown("---")

st.sidebar.markdown("**📅 Date Range**")
start_date = st.sidebar.date_input("Start Date", value=date(2026, 8, 1))
end_date = st.sidebar.date_input("End Date", value=date(2026, 8, 9))

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
selected_companies = st.sidebar.multiselect(
    "Company",
    options=company_options,
    label_visibility="collapsed",
)

st.sidebar.markdown("**🏷️ Vendor**")
selected_vendors = st.sidebar.multiselect(
    "Vendor",
    options=vendor_options,
    label_visibility="collapsed",
)

st.sidebar.markdown("**📦 Category**")
selected_categories = st.sidebar.multiselect(
    "Category",
    options=category_options,
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
        st.sidebar.info("Cache cleared.")

# -------------------------------------------------------------------
# Main report
# -------------------------------------------------------------------

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

if run_clicked:
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
                # Reset pagination when a new fetch happens
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

    # -------------------- Pagination controls (top) --------------------
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

    # -------------------- Slice and render --------------------
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size
    page_df = df.iloc[start_idx:end_idx]

    render_report_table(
        page_df,
        result["start_date"],
        result["end_date"],
    )

    st.caption(
        f"Showing rows {start_idx + 1:,}–{min(end_idx, total_rows):,} "
        f"of {total_rows:,}"
    )

    # -------------------- Pagination controls (bottom) --------------------
    b1, b2, b3, b4, b5 = st.columns([2, 2, 2, 1, 1])

    with b4:
        if st.button("← Prev ", use_container_width=True, disabled=(page <= 1), key="prev_bottom"):
            st.session_state["page"] = page - 1
            st.rerun()

    with b5:
        if st.button("Next → ", use_container_width=True, disabled=(page >= total_pages), key="next_bottom"):
            st.session_state["page"] = page + 1
            st.rerun()

    # -------------------- Top vendors chart --------------------
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

    # -------------------- CSV download (full dataset) --------------------
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