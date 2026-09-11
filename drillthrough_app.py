"""
drillthrough_app.py

Second Streamlit application for drill-through product cards.

Expected query parameters from main_app.py:
    drill=sales|available
    company=...
    vendor=...
    category=...
    start_date=YYYY-MM-DD
    end_date=YYYY-MM-DD

Available drill:
    stock_quant_n1 -> product_product -> product_images

Sales drill:
    pos_order_line -> pos_order -> product_product -> product_images

Important:
POS order lines do not reliably expose the exact sold lot/location by
the fields provided in the current report. Therefore the sales drill
shows product-level sold quantity/value. The available drill shows exact
current lot/location/age information.
"""

from datetime import date

import pandas as pd
import pyodbc
import streamlit as st

st.set_page_config(
    page_title="Product Drill-through",
    page_icon="🧵",
    layout="wide",
)


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

# -------------------------------------------------------------------
# CSS  (light + dark theme aware)
# -------------------------------------------------------------------

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@600;700;800&family=Inter:wght@400;500;600;700&display=swap');

    /* ---------- LIGHT THEME ---------- */
    :root {
        --bg: #fffaf4;
        --bg2: #fdf8f2;
        --text: #3a1f28;
        --muted: rgba(58,31,40,.65);
        --wine: #560835;
        --wine2: #7c1049;
        --gold: #b6871f;
        --border: #eadbc6;
        --card-bg: #ffffff;
        --card-border: #f0e2cd;
        --card-shadow: rgba(58,31,40,.08);
        --stock-bg: #12351f;
        --stock-fg: #9fe1b2;
    }

    /* ---------- DARK THEME ---------- */
    @media (prefers-color-scheme: dark) {
        :root {
            --bg: #14100e;
            --bg2: #1c1815;
            --text: #f3e8d9;
            --muted: rgba(243,232,217,.65);
            --wine: #b66289;
            --wine2: #cf6a91;
            --gold: #d8b34c;
            --border: #3a2f2a;
            --card-bg: #1d1f21;
            --card-border: #3a2f2a;
            --card-shadow: rgba(0,0,0,.4);
            --stock-bg: #12351f;
            --stock-fg: #9fe1b2;
        }
    }

    [data-theme="dark"] {
        --bg: #14100e;
        --bg2: #1c1815;
        --text: #f3e8d9;
        --muted: rgba(243,232,217,.65);
        --wine: #b66289;
        --wine2: #cf6a91;
        --gold: #d8b34c;
        --border: #3a2f2a;
        --card-bg: #1d1f21;
        --card-border: #3a2f2a;
        --card-shadow: rgba(0,0,0,.4);
        --stock-bg: #12351f;
        --stock-fg: #9fe1b2;
    }

    .stApp {
        background: linear-gradient(180deg, var(--bg) 0%, var(--bg2) 100%);
        color: var(--text);
    }

    #MainMenu, footer { visibility: hidden; height: 0; }

    header[data-testid="stHeader"] { background: transparent; }

    .block-container {
        max-width: 1450px;
        padding-top: 1.4rem;
        padding-bottom: 3rem;
    }

    .back-link {
        display: inline-block;
        margin-bottom: 16px;
        padding: 9px 15px;
        border-radius: 11px;
        background: var(--bg2);
        border: 1px solid var(--border);
        color: var(--wine) !important;
        text-decoration: none !important;
        font-weight: 700;
    }

    .drill-hero {
        background: linear-gradient(120deg,#560835 0%,#7c1049 55%,#37041f 100%);
        border-radius: 20px;
        padding: 25px 30px;
        color: #fff8ec;
        box-shadow: 0 10px 30px rgba(86,8,53,.25);
        margin-bottom: 22px;
    }

    .drill-hero h1 {
        font-family: 'Playfair Display', serif;
        margin: 0;
        font-size: 2rem;
    }

    .drill-hero p {
        color: #e9d488;
        margin: 6px 0 0;
    }

    .context-row {
        display: flex;
        flex-wrap: wrap;
        gap: 8px;
        margin: 12px 0 20px;
    }

    .context-chip {
        padding: 7px 12px;
        border-radius: 999px;
        background: var(--bg2);
        border: 1px solid var(--border);
        color: var(--text);
        font-size: 13px;
        font-weight: 600;
    }

    /* ---------- PRODUCT CARD ---------- */
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
        background: var(--bg2);
    }

    .no-image {
        height: 245px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--bg2);
        color: var(--muted);
        font-weight: 700;
        font-size: 14px;
    }

    .card-body {
        padding: 16px 14px 15px;
        color: var(--text);
        background: var(--card-bg);
        display: flex;
        flex-direction: column;
        flex: 1;
    }

    .product-title {
        font-size: 17px;
        font-weight: 800;
        margin-bottom: 12px;
        color: var(--text);
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
        color: var(--text);
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

    .summary-box {
        background: var(--bg2);
        border: 1px solid var(--border);
        border-left: 5px solid var(--gold);
        border-radius: 14px;
        padding: 15px 18px;
        margin-bottom: 20px;
        color: var(--text);
    }

    .summary-box strong { color: var(--wine); }

    .stButton > button {
        border-radius: 11px !important;
        font-weight: 700 !important;
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
    cur = conn.cursor()
    cur.execute(sql, params)
    columns = [c[0] for c in cur.description]
    rows = cur.fetchall()
    cur.close()
    return pd.DataFrame.from_records(rows, columns=columns)


# -------------------------------------------------------------------
# URL parameters  (normalized)
# -------------------------------------------------------------------

def _clean(v):
    """Normalize URL params: None/NaN/'None'/'null' -> '' and strip whitespace."""
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "null", "nan", ""}:
        return ""
    return s


qp = st.query_params

drill_type     = _clean(qp.get("drill", "available")) or "available"
company        = _clean(qp.get("company", ""))
vendor         = _clean(qp.get("vendor", ""))
category       = _clean(qp.get("category", ""))
start_date_str = _clean(qp.get("start_date", ""))
end_date_str   = _clean(qp.get("end_date", ""))

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

# -------------------------------------------------------------------
# Back button
# -------------------------------------------------------------------

MAIN_APP_URL = _get_secret("MAIN_APP_URL") or "http://localhost:8501"

st.markdown(
    f'<a class="back-link" href="{MAIN_APP_URL.rstrip("/")}/" target="_self">← Back to Report</a>',
    unsafe_allow_html=True,
)

# -------------------------------------------------------------------
# SQL
# -------------------------------------------------------------------

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
            CAST(product_id AS VARCHAR(50)) AS product_id,
            CAST(image_1920 AS VARCHAR(MAX)) AS image_1920,
            ROW_NUMBER() OVER (
                PARTITION BY product_id
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
sales_detail AS (
    SELECT
        po.company_id_name AS company,
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
        CAST(pol.product_id AS VARCHAR(50))
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


# -------------------------------------------------------------------
# Execute
# -------------------------------------------------------------------

if not all([SQL_ENDPOINT, DATABASE, TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
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
        df = run_query(conn_str, sql, params)
    except Exception as e:
        st.error(f"Drill-through query failed: {e}")
        st.stop()

# -------------------------------------------------------------------
# Header/context
# -------------------------------------------------------------------

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

# -------------------------------------------------------------------
# Summary
# -------------------------------------------------------------------

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
            "If a value shows as 'None' or has trailing spaces, the main "
            "app sent a malformed parameter. If the values look correct, "
            "the underlying table has no rows for this combination."
        )

    st.stop()

# -------------------------------------------------------------------
# Card rendering
# -------------------------------------------------------------------

def esc(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def image_html(image_value):
    if image_value is None:
        return '<div class="no-image">No image available</div>'
    src = str(image_value).strip()
    if src == "" or src.lower() in {"false", "none", "nan"}:
        return '<div class="no-image">No image available</div>'

    if src.startswith(("http://", "https://", "data:image/")):
        return (
            f'<img class="product-image" src="{esc(src)}" '
            f'alt="Product image" loading="lazy">'
        )

    return '<div class="no-image">Image path is not browser-accessible</div>'


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
        image_html(row.get("image_1920", "")),
        '<div class="card-body">',
        f'<div class="product-title">{esc(row.get("product_name", ""))}</div>',
        '<div class="product-subtitle">'
        f'{esc(row.get("category", ""))}<br>{esc(row.get("vendor", ""))}'
        '</div>',
        _detail_row("SKU", esc(row.get("sku", ""))),
        _detail_row("Lot No.", esc(row.get("lot_number", ""))),
        _detail_row("Location", esc(row.get("location", ""))),
        _detail_row("Age (days)", esc(row.get("overall_age", ""))),
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
        image_html(row.get("image_1920", "")),
        '<div class="card-body">',
        f'<div class="product-title">{esc(row.get("product_name", ""))}</div>',
        '<div class="product-subtitle">'
        f'{esc(row.get("category", ""))}<br>{esc(row.get("vendor", ""))}'
        '</div>',
        _detail_row("SKU", esc(row.get("sku", ""))),
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


# -------------------------------------------------------------------
# Grid
# -------------------------------------------------------------------

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