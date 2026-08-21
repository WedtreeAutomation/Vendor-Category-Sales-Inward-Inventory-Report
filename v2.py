"""
Streamlit app: Vendor / Category Sales, Inward & Inventory report
-------------------------------------------------------------------
Connects to a Microsoft Fabric / Synapse Lakehouse SQL analytics endpoint
using a Service Principal (Azure AD app registration).

Left sidebar:
  - One date range (Start / End) that drives BOTH the sales date filter
    (po.date_order) and the inward date filter (pk.date_done).
  - Company / Vendor / Category multi-select filters. Their options are
    fetched automatically from the Lakehouse the moment the app loads
    (via DISTINCT queries), before the user ever clicks "Fetch Data".
    Vendor list includes a synthetic "None" option for products with no
    vendor assigned.

Requirements (see requirements.txt):
    streamlit
    pyodbc
    pandas

You also need a "SQL Server" ODBC driver installed on the machine
running this app (not a pip package — install separately). The app
auto-detects whichever version you have installed.

  Windows:
    Download & run the installer:
    https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

  macOS (Homebrew):
    brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
    brew update
    brew install msodbcsql18 mssql-tools18

  Linux (Debian/Ubuntu):
    curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
    curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
    sudo apt-get update
    sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

  Streamlit Community Cloud (Debian-based):
    packages.txt (included alongside this file) installs unixodbc /
    unixodbc-dev, which pyodbc needs to even import. HOWEVER: Microsoft's
    actual "msodbcsql18" driver requires accepting a EULA during install
    (ACCEPT_EULA=Y), which packages.txt's plain `apt-get install` cannot
    do — so Community Cloud's default builder generally CANNOT install
    the Microsoft SQL Server ODBC driver itself. In practice this means:
      - Community Cloud (as of this writing) is NOT a reliable place to
        run this app unless you deploy via a custom Dockerfile where you
        control the install steps (so you can set ACCEPT_EULA=Y).
      - For a no-Docker option, look at Streamlit's "Community Cloud +
        custom Docker" style deployment, or host this on a VM / App
        Service / Container App / on-prem, or Snowflake Streamlit, etc.
        where you can install the driver yourself.
    If you deploy anyway and this fails, the error will be the same
    "Data source name not found" / driver-not-installed error described
    below — that confirms it's this limitation, not a code bug.

If you still get "Data source name not found" after installing, restart
the app process (the driver list is only refreshed on process start),
and check Python actually sees it:
    python -c "import pyodbc; print(pyodbc.drivers())"
"""

import streamlit as st
import pyodbc
import pandas as pd
from datetime import date

st.set_page_config(page_title="Vendor / Category Report", page_icon="📊", layout="wide")

# =========================================================
# Connection settings — pulled from Streamlit secrets, NOT
# hardcoded in source. This is what makes it safe to deploy
# on Streamlit Community Cloud / push app.py to a repo.
#
# Locally: create .streamlit/secrets.toml next to app.py:
#
#   SQL_ENDPOINT   = "xxxxxxxx.datawarehouse.fabric.microsoft.com"
#   DATABASE       = "WT_LH_Silver"
#   TENANT_ID      = "your-tenant-guid"
#   CLIENT_ID      = "your-service-principal-app-id"
#   CLIENT_SECRET  = "your-service-principal-secret"
#
# On Streamlit Community Cloud: go to your app -> Settings ->
# Secrets, and paste the same TOML content there. Never commit
# .streamlit/secrets.toml to git — add it to .gitignore.
# =========================================================
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

# Companies excluded from the underlying data everywhere in the main query.
EXCLUDED_COMPANIES = ["Saree Trails", "Wedtree eStore Private Limited - HO"]

# Companies that should simply not be offered as a filter choice in the
# sidebar (they still exist in the data / aren't excluded from the query
# itself — they're just hidden from the Company dropdown).
COMPANIES_HIDDEN_FROM_FILTER = ["Wedtree eStore Private Limited - Online"]

NONE_VENDOR_LABEL = "None"

# =========================================================
# Custom styling
# =========================================================
st.markdown(
    """
    <style>
    .block-container { padding-top: 2rem; }
    [data-testid="stMetric"] {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 14px 16px;
    }
    [data-testid="stMetricLabel"] { font-weight: 600; color: #475569; }
    section[data-testid="stSidebar"] { border-right: 1px solid #e2e8f0; }
    </style>
    """,
    unsafe_allow_html=True,
)


def get_sql_server_driver():
    """Pick the best available 'SQL Server' ODBC driver installed on this
    machine. Prefers newer versions. Returns None if none are installed."""
    installed = pyodbc.drivers()
    preferred_order = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for name in preferred_order:
        if name in installed:
            return name
    for name in installed:
        if "SQL Server" in name:
            return name
    return None


def build_connection_string(server, db, client_id_, client_secret_, tenant_id_):
    driver = get_sql_server_driver()
    if driver is None:
        raise RuntimeError(
            "No SQL Server ODBC driver is installed on this machine. "
            f"Drivers currently visible to pyodbc: {pyodbc.drivers() or 'none'}. "
            "Install 'ODBC Driver 18 for SQL Server' and restart the app "
            "(see install notes at the top of app.py)."
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
    return all([SQL_ENDPOINT, DATABASE, TENANT_ID, CLIENT_ID, CLIENT_SECRET])


# =========================================================
# Query template
# Date placeholders (4x ?): sales_start, sales_end, inward_start, inward_end
# — the app always passes the SAME start/end pair for both, since the
# sidebar exposes a single shared date range.
#
# {vendor_clause} / {category_clause} / {company_clause} are optional
# filters injected into the final SELECT's WHERE clause. Each is either
# "1=1" (no filter) or a matching IN(...)/IS NULL fragment, with params
# appended in that order: vendor params, then category params, then
# company params.
#
# NOTE: available_qty / available_value (inventory columns) reflect
# CURRENT stock on hand — they are NOT scoped to the selected date range,
# only sale_qty/sale_value and inward_qty/inward_value are.
# =========================================================
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
    """Returns a SQL fragment ('1=1' or 'column IN (?,?,...)') and appends
    the corresponding parameter values (in order) to `params`."""
    if not values:
        return "1=1"
    placeholders = ",".join(["?"] * len(values))
    params.extend(values)
    return f"{column} IN ({placeholders})"


def build_vendor_clause(selected_vendors, params):
    """Like build_in_clause, but supports a synthetic 'None' entry meaning
    'vendor IS NULL' (no vendor assigned), alongside real vendor names."""
    if not selected_vendors:
        return "1=1"
    include_none = NONE_VENDOR_LABEL in selected_vendors
    real_vendors = [v for v in selected_vendors if v != NONE_VENDOR_LABEL]

    conditions = []
    if real_vendors:
        placeholders = ",".join(["?"] * len(real_vendors))
        params.extend(real_vendors)
        conditions.append(f"vendor IN ({placeholders})")
    if include_none:
        conditions.append("vendor IS NULL")

    if not conditions:
        return "1=1"
    return "(" + " OR ".join(conditions) + ")"


def build_query_and_params(start_str, end_str, vendors, categories, companies):
    params = [start_str, end_str, start_str, end_str]  # date filters (sales + inward)
    vendor_clause = build_vendor_clause(vendors, params)
    category_clause = build_in_clause("category", categories, params)
    company_clause = build_in_clause("company", companies, params)
    sql = QUERY_TEMPLATE.format(
        vendor_clause=vendor_clause,
        category_clause=category_clause,
        company_clause=company_clause,
    )
    return sql, params


@st.cache_data(ttl=3600, show_spinner=True)
def get_filter_options(_conn_str):
    """Fetch distinct Company / Vendor / Category values straight from the
    source tables, so the sidebar filters are populated automatically as
    soon as the app loads — before the user clicks Fetch Data."""
    excluded_sql = ",".join(["?"] * len(EXCLUDED_COMPANIES))

    with pyodbc.connect(_conn_str) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            SELECT DISTINCT categ_id_name
            FROM WT_LH_Silver.Odoo.product_product
            WHERE categ_id_name IS NOT NULL
              AND LOWER(categ_id_name) NOT LIKE '%admin%'
            ORDER BY categ_id_name
            """
        )
        categories = [r[0] for r in cur.fetchall()]

        cur.execute(
            """
            SELECT DISTINCT vendor_id_name
            FROM WT_LH_Silver.Odoo.product_template
            WHERE vendor_id_name IS NOT NULL
            ORDER BY vendor_id_name
            """
        )
        vendors = [r[0] for r in cur.fetchall()]

        cur.execute(
            f"""
            SELECT DISTINCT company_id_name FROM (
                SELECT company_id_name FROM WT_LH_Silver.Odoo.pos_order
                UNION
                SELECT company_id_name FROM WT_LH_Silver.Odoo.stock_picking
                UNION
                SELECT company_id_name FROM WT_LH_Silver.Odoo.stock_quant_n1
            ) t
            WHERE company_id_name IS NOT NULL
              AND company_id_name NOT IN ({excluded_sql})
            ORDER BY company_id_name
            """,
            EXCLUDED_COMPANIES,
        )
        companies = [r[0] for r in cur.fetchall()]

    companies = [c for c in companies if c not in COMPANIES_HIDDEN_FROM_FILTER]
    vendors = [NONE_VENDOR_LABEL] + vendors

    return companies, vendors, categories


@st.cache_data(ttl=600, show_spinner=False)
def run_query(_conn_str, sql, params):
    # underscore prefix on _conn_str tells st.cache_data not to hash it
    # (it can contain a secret) — caching is keyed on sql + params instead.
    with pyodbc.connect(_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
    return df


# =========================================================
# Sidebar — filters
# =========================================================
st.sidebar.markdown("## 🔎 Filters")
st.sidebar.markdown("---")

st.sidebar.markdown("**📅 Date Range**")
start_date = st.sidebar.date_input("Start Date", value=date(2026, 8, 1))
end_date = st.sidebar.date_input("End Date", value=date(2026, 8, 9))
# st.sidebar.caption("Applies to both Sales (date_order) and Inward (date_done).")

st.sidebar.markdown("---")

if not connection_is_configured():
    st.sidebar.warning(
        "Connection details aren't configured yet. Add them to "
        ".streamlit/secrets.toml locally (or the app's Secrets settings "
        "on Streamlit Community Cloud) — see the top of app.py for the "
        "required keys."
    )
    company_options, vendor_options, category_options = [], [], []
else:
    conn_str_for_options = build_connection_string(
        SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID
    )
    try:
        company_options, vendor_options, category_options = get_filter_options(
            conn_str_for_options
        )
    except Exception as e:
        st.sidebar.error(f"Couldn't load filter options: {e}")
        company_options, vendor_options, category_options = [], [], []

st.sidebar.markdown("**🏢 Company**")
selected_companies = st.sidebar.multiselect(
    "Company", options=company_options, label_visibility="collapsed"
)

st.sidebar.markdown("**🏷️ Vendor**")
selected_vendors = st.sidebar.multiselect(
    "Vendor", options=vendor_options, label_visibility="collapsed"
)

st.sidebar.markdown("**📦 Category**")
selected_categories = st.sidebar.multiselect(
    "Category", options=category_options, label_visibility="collapsed"
)

st.sidebar.markdown("---")
run_clicked = st.sidebar.button("▶️  Fetch Data", type="primary", use_container_width=True)

col_a, col_b = st.sidebar.columns(2)
with col_a:
    if st.button("↻ Refresh filters", use_container_width=True):
        get_filter_options.clear()
        st.rerun()
with col_b:
    if st.button("🗑️ Clear cache", use_container_width=True):
        run_query.clear()
        st.sidebar.info("Cache cleared.")

# =========================================================
# Main
# =========================================================
st.markdown("# 📊 Vendor / Category Sales, Inward & Inventory Report")
st.caption("Live data from the Fabric Lakehouse — filtered by the panel on the left.")

query_sql, query_params = build_query_and_params(
    start_date.isoformat(),
    end_date.isoformat(),
    selected_vendors,
    selected_categories,
    selected_companies,
)

if run_clicked:
    if start_date > end_date:
        st.error("Start Date must be on or before End Date.")
    elif not connection_is_configured():
        st.error(
            "Connection details aren't configured. Add SQL_ENDPOINT, DATABASE, "
            "TENANT_ID, CLIENT_ID and CLIENT_SECRET to .streamlit/secrets.toml "
            "(locally) or the app's Secrets settings (on Streamlit Community Cloud)."
        )
    else:
        conn_str = build_connection_string(
            SQL_ENDPOINT, DATABASE, CLIENT_ID, CLIENT_SECRET, TENANT_ID
        )
        with st.spinner("Running query against the Lakehouse..."):
            try:
                df = run_query(conn_str, query_sql, query_params)

                # ---- KPI summary cards ----
                k1, k2, k3, k4 = st.columns(4)
                k1.metric("Rows", f"{len(df):,}")
                k2.metric("Sale Value", f"₹{df['sale_value'].sum():,.0f}" if "sale_value" in df else "—")
                k3.metric("Inward Value", f"₹{df['inward_value'].sum():,.0f}" if "inward_value" in df else "—")
                k4.metric("Available Value", f"₹{df['available_value'].sum():,.0f}" if "available_value" in df else "—")

                st.write("")

                # ---- Polished inventory-snapshot notice ----
                st.markdown(
                    """
                    <div style="
                        display:flex; align-items:flex-start; gap:14px;
                        background:linear-gradient(135deg,#fff7ed,#fffbeb);
                        border:1px solid #fcd34d; border-left:5px solid #f59e0b;
                        border-radius:12px; padding:14px 18px; margin:6px 0 20px 0;
                        box-shadow:0 1px 3px rgba(0,0,0,0.04);">
                        <div style="font-size:22px; line-height:1;">📦</div>
                        <div>
                            <div style="font-weight:700; color:#92400e; font-size:14.5px;">
                                Inventory Snapshot Notice
                            </div>
                            <div style="color:#78350f; font-size:13.5px; margin-top:2px;">
                                <b>Quantity</b> and <b>stock value</b> figures reflect
                                <b>current inventory on hand</b> — they are <u>not</u>
                                scoped to the selected date range. Only Sales and
                                Inward figures are filtered by date.
                            </div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

                filter_bits = []
                if selected_companies:
                    filter_bits.append(f"Company: {', '.join(selected_companies)}")
                if selected_vendors:
                    filter_bits.append(f"Vendor: {', '.join(selected_vendors)}")
                if selected_categories:
                    filter_bits.append(f"Category: {', '.join(selected_categories)}")
                if filter_bits:
                    st.caption(" | ".join(filter_bits))

                st.dataframe(
                    df,
                    use_container_width=True,
                    height=520,
                    column_config={
                        "sale_qty": st.column_config.NumberColumn("Sale Qty", format="%d"),
                        "sale_value": st.column_config.NumberColumn("Sale Value", format="₹%.2f"),
                        "available_qty": st.column_config.NumberColumn("Available Qty", format="%d"),
                        "available_value": st.column_config.NumberColumn("Available Value", format="₹%.2f"),
                        "inward_qty": st.column_config.NumberColumn("Inward Qty", format="%d"),
                        "inward_value": st.column_config.NumberColumn("Inward Value", format="₹%.2f"),
                    },
                )

                csv_bytes = df.to_csv(index=False).encode("utf-8")
                st.download_button(
                    "⬇️ Download CSV",
                    data=csv_bytes,
                    file_name=f"vendor_category_report_{start_date}_{end_date}.csv",
                    mime="text/csv",
                )
            except Exception as e:
                st.error(f"Query failed: {e}")
else:
    st.info("Set your filters on the left, then click **Fetch Data** to load the report.")
