import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json

# ─── IMPORT FROM db.py ───────────────────────────────────────────────────────
from db import get_all_products, get_product, get_low_stock, get_out_of_stock, get_top_products, invalidate_product, get_pg_conn, release_pg_conn, place_order, restock_product, add_product, remove_product

# ─── PAGE CONFIG ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title=" MSME Inventory",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CUSTOM CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap');

:root {
    --brand:        #875BF7;
    --brand-light:  #EEE9FE;
    --brand-dark:   #6741D9;
    --accent:       #F79009;
    --accent-light: #FEF0C7;
    --danger:       #F04438;
    --danger-light: #FEE4E2;
    --success:      #12B76A;
    --success-light:#D1FADF;
    --info:         #0BA5EC;
    --info-light:   #E0F2FE;
    --bg:           #F8F7FF;
    --surface:      #FFFFFF;
    --border:       #E9E8F5;
    --text:         #1A1523;
    --text-muted:   #6B7280;
    --sidebar-bg:   #1A1523;
    --sidebar-text: #C9C4D4;
}

html, body, [class*="css"] {
    font-family: 'Plus Jakarta Sans', sans-serif;
    background-color: var(--bg);
    color: var(--text);
}

#MainMenu, footer, header { visibility: hidden; }
.block-container { padding: 1.5rem 2rem 2rem 2rem !important; max-width: 100% !important; }

[data-testid="stSidebar"] {
    background: var(--sidebar-bg) !important;
    border-right: none;
}
[data-testid="stSidebar"] * { color: var(--sidebar-text) !important; }
[data-testid="stSidebar"] .stRadio label {
    font-size: 0.85rem !important;
    font-weight: 500 !important;
    padding: 0.5rem 1rem;
    border-radius: 8px;
    margin: 2px 0;
    transition: all 0.2s;
}
[data-testid="stSidebar"] .stRadio label:hover {
    background: rgba(255,255,255,0.08) !important;
    color: #fff !important;
}

.kpi-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 1.25rem 1.5rem;
    box-shadow: 0 1px 4px rgba(26,21,35,0.06);
    transition: box-shadow 0.2s;
    margin-bottom: 0.5rem;
}
.kpi-card:hover { box-shadow: 0 4px 16px rgba(135,91,247,0.12); }
.kpi-label { font-size: 0.75rem; font-weight: 600; color: var(--text-muted); letter-spacing: 0.05em; text-transform: uppercase; }
.kpi-value { font-size: 1.85rem; font-weight: 800; color: var(--text); line-height: 1.1; margin: 0.2rem 0; }
.kpi-delta { font-size: 0.78rem; font-weight: 600; }
.kpi-delta.up   { color: var(--success); }
.kpi-delta.down { color: var(--danger); }
.kpi-icon { font-size: 1.6rem; float: right; margin-top: -0.2rem; }

.section-header {
    font-size: 1.15rem;
    font-weight: 700;
    color: var(--text);
    margin: 1.5rem 0 0.75rem 0;
    padding-bottom: 0.5rem;
    border-bottom: 2px solid var(--brand-light);
    display: flex;
    align-items: center;
    gap: 0.5rem;
}

.top-bar {
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    padding: 0.75rem 1.5rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-radius: 12px;
    margin-bottom: 1.5rem;
    box-shadow: 0 1px 3px rgba(26,21,35,0.05);
}
.top-bar-title { font-size: 1.4rem; font-weight: 800; color: var(--brand); letter-spacing: -0.02em; }
.top-bar-sub   { font-size: 0.78rem; color: var(--text-muted); font-weight: 500; }
.badge {
    display: inline-block;
    padding: 0.2rem 0.65rem;
    border-radius: 999px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.03em;
}
.badge-purple { background: var(--brand-light); color: var(--brand-dark); }
.badge-orange { background: var(--accent-light); color: #B45309; }
.badge-red    { background: var(--danger-light); color: #B42318; }
.badge-green  { background: var(--success-light); color: #027A48; }
.badge-blue   { background: var(--info-light);  color: #026AA2; }

[data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; border: 1px solid var(--border); }
[data-testid="stDataFrame"] table { font-family: 'Plus Jakarta Sans', sans-serif !important; }

.stTabs [data-baseweb="tab-list"] {
    gap: 0;
    background: var(--bg);
    border-radius: 10px;
    padding: 4px;
    border: 1px solid var(--border);
    width: fit-content;
}
.stTabs [data-baseweb="tab"] {
    border-radius: 7px !important;
    padding: 0.4rem 1.1rem !important;
    font-size: 0.83rem !important;
    font-weight: 600 !important;
    color: var(--text-muted) !important;
    background: transparent !important;
    border: none !important;
    transition: all 0.15s;
}
.stTabs [aria-selected="true"] {
    background: var(--surface) !important;
    color: var(--brand) !important;
    box-shadow: 0 1px 4px rgba(26,21,35,0.1) !important;
}

.stButton > button {
    background: var(--brand) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 600 !important;
    font-size: 0.83rem !important;
    padding: 0.45rem 1.2rem !important;
    transition: all 0.2s !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
}
.stButton > button:hover {
    background: var(--brand-dark) !important;
    box-shadow: 0 4px 12px rgba(135,91,247,0.3) !important;
    transform: translateY(-1px) !important;
}

.stTextInput > div > div > input,
.stNumberInput > div > div > input,
.stSelectbox > div > div,
.stTextArea > div > textarea {
    color: black !important;
    border-radius: 8px !important;
    border: 1.5px solid var(--border) !important;
    font-family: 'Plus Jakarta Sans', sans-serif !important;
    font-size: 0.85rem !important;
    background: var(--surface) !important;
    transition: border-color 0.2s !important;
}
.stTextInput > div > div > input:focus,
.stNumberInput > div > div > input:focus {
    border-color: var(--brand) !important;
    box-shadow: 0 0 0 3px rgba(135,91,247,0.15) !important;
}

[data-testid="stMetric"] {
    background: var(--surface);
    padding: 1rem 1.2rem;
    border-radius: 12px;
    border: 1px solid var(--border);
}

.alert-box {
    padding: 0.75rem 1rem;
    border-radius: 10px;
    margin: 0.4rem 0;
    font-size: 0.83rem;
    font-weight: 500;
    display: flex;
    align-items: center;
    gap: 0.5rem;
}
.alert-box.danger  { background: var(--danger-light);  color: #B42318; border-left: 4px solid var(--danger); }
.alert-box.warning { background: var(--accent-light);  color: #B45309; border-left: 4px solid var(--accent); }
.alert-box.success { background: var(--success-light); color: #027A48; border-left: 4px solid var(--success); }

.db-pill {
    display: inline-flex; align-items: center; gap: 5px;
    background: #1A1523; color: #C9C4D4;
    border-radius: 999px; padding: 4px 12px;
    font-size: 0.72rem; font-weight: 600; font-family: 'JetBrains Mono', monospace;
}
.dot { width: 7px; height: 7px; border-radius: 50%; display: inline-block; }
.dot.green  { background: #12B76A; box-shadow: 0 0 6px #12B76A; }
.dot.red    { background: #F79009; box-shadow: 0 0 6px #F79009; }
</style>
""", unsafe_allow_html=True)


# ─── LOAD REAL DATA ──────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_data():
    df = get_all_products()
    df.columns = [c.strip() for c in df.columns]
    df["Stock_Value"] = (df["Products in Store"] * df["Price ($)"]).round(2)
    df["Status"] = df["Products in Store"].apply(
        lambda x: "Out of Stock" if x == 0 else ("Low Stock" if x <= 15 else "In Stock")
    )
    return df

df = load_data()

# ─── SIDEBAR ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:1.2rem 0.5rem 1.5rem 0.5rem;">
        <div style="font-size:1.35rem;font-weight:800;color:#875BF7;letter-spacing:-0.02em;">StockSeva</div>
        <div style="font-size:0.72rem;color:#6B7280;margin-top:2px;font-weight:500;">MSME Inventory Platform</div>
        <div style="margin-top:0.8rem;">Welcome to StockSeva</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("<div style='font-size:0.68rem;font-weight:700;letter-spacing:0.1em;color:#6B7280;padding:0 0.5rem;margin-bottom:0.4rem;text-transform:uppercase;'>Navigation</div>", unsafe_allow_html=True)

    page = st.radio(
        "Navigation",
        ["🏠  Dashboard", "📦  Inventory", "🚚  Orders", "⚠️  Alerts", "📊  Reports", "⚙️  Settings"],
        label_visibility="collapsed"
    )

    st.markdown("---")

    # ─── ADMIN PASSWORD ───
    admin_pass = st.text_input("🔑 Admin Password", type="password", label_visibility="visible")
    is_admin = admin_pass == "admin123"  # Change this to your preferred password
    if admin_pass and is_admin:
        st.success("✅ Admin access granted")
    elif admin_pass and not is_admin:
        st.error("❌ Wrong password")

    st.markdown("---")
    st.markdown("""
    <div style="padding:0.75rem;background:rgba(135,91,247,0.12);border-radius:10px;border:1px solid rgba(135,91,247,0.2);">
        <div style="font-size:0.72rem;font-weight:700;color:#875BF7;margin-bottom:4px;">🏭 Active Business</div>
        <div style="font-size:0.82rem;font-weight:600;color:#C9C4D4;">StockSeva</div>
    </div>
    """, unsafe_allow_html=True)


# ─── TOP BAR ────────────────────────────────────────────────────────────────
today_str = datetime.now().strftime("%A, %d %B %Y")
low_count = len(df[df["Status"] == "Low Stock"])
out_count = len(df[df["Status"] == "Out of Stock"])

st.markdown(f"""
<div class="top-bar">
    <div>
        <div class="top-bar-title">StockSeva</div>
        <div class="top-bar-sub">MSME Inventory Management · {today_str}</div>
    </div>
    <div style="display:flex;gap:10px;align-items:center;">
        <span class="badge badge-purple"> </span>
    </div>
</div>
""", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ════════════════════════════════════════════════════════════════════════════
if "Dashboard" in page:

    total_val  = df["Stock_Value"].sum()
    total_skus = len(df)
    in_stock   = len(df[df["Status"] == "In Stock"])
    avg_price  = df["Price ($)"].mean()

    c1, c2, c3, c4 = st.columns(4)
    kpis = [
        (c1, "Total Products",     str(total_skus),                   "From PostgreSQL",              "up",  "🗂️"),
        (c2, "Avg Price",          f"${avg_price:.2f}",               "Across all products",          "up",  "💰"),
        (c3, "Low / Out of Stock", f"{low_count} / {out_count}",      "Needs attention",              "down","⚠️"),
        (c4, "In-Stock Items",     str(in_stock),                     f"{in_stock/total_skus*100:.0f}% availability","up","✅"),
    ]
    for col, label, val, delta, direction, icon in kpis:
        with col:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-label">{label} <span class="kpi-icon">{icon}</span></div>
                <div class="kpi-value">{val}</div>
                <div class="kpi-delta {direction}">{delta}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('<div class="section-header">📈 Analytics Overview</div>', unsafe_allow_html=True)
    ch1, ch2 = st.columns([1.5, 1])

    with ch1:
        top_sold = df.nlargest(10, "Products Sold")[["Title of Products", "Products Sold"]]
        top_sold["Title of Products"] = top_sold["Title of Products"].str[:40]
        fig = px.bar(top_sold, x="Products Sold", y="Title of Products", orientation="h",
                     color="Products Sold", color_continuous_scale=["#EEE9FE","#875BF7"],
                     labels={"Products Sold": "Units Sold", "Title of Products": ""})
        fig.update_layout(
            title="Top 10 Products by Units Sold",
            title_font=dict(size=13, family="Plus Jakarta Sans"),
            margin=dict(l=0,r=0,t=35,b=0), height=300,
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            coloraxis_showscale=False, font=dict(family="Plus Jakarta Sans"),
            xaxis=dict(showgrid=True, gridcolor="#E9E8F5"),
            yaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig, use_container_width=True)

    with ch2:
        status_cnt = df["Status"].value_counts().reset_index()
        status_cnt.columns = ["Status", "Count"]
        colors = {"In Stock": "#12B76A", "Low Stock": "#F79009", "Out of Stock": "#F04438"}
        fig2 = px.pie(status_cnt, names="Status", values="Count", hole=0.62,
                      color="Status", color_discrete_map=colors)
        fig2.update_traces(textinfo="percent+label", textfont_size=10)
        fig2.update_layout(
            title="Stock Status Split",
            title_font=dict(size=13, family="Plus Jakarta Sans"),
            margin=dict(l=0,r=0,t=35,b=0), height=300,
            paper_bgcolor="rgba(0,0,0,0)", showlegend=False,
            font=dict(family="Plus Jakarta Sans"),
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown('<div class="section-header">🚨 Stock Alerts Preview</div>', unsafe_allow_html=True)
    alert_df = df[df["Status"] != "In Stock"].nsmallest(8, "Products in Store")[["Product ID","Title of Products","Products in Store","Status"]]
    for _, row in alert_df.iterrows():
        cls  = "danger" if row["Status"] == "Out of Stock" else "warning"
        icon = "🚫" if row["Status"] == "Out of Stock" else "⚠️"
        st.markdown(f"""
        <div class="alert-box {cls}">
            {icon} <strong>ID: {row['Product ID']}</strong> — {str(row['Title of Products'])[:50]}
            &nbsp;|&nbsp; In Store: <strong>{row['Products in Store']}</strong>
        </div>
        """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE: INVENTORY
# ════════════════════════════════════════════════════════════════════════════
elif "Inventory" in page:

    tab1, tab2, tab3, tab4 = st.tabs(["📋  Product List", "🔍  Lookup by ID", "➕  Add Product", "🗑️  Remove Product"])

    with tab1:
        st.markdown('<div class="section-header">📋 Inventory Master</div>', unsafe_allow_html=True)

        f1, f2 = st.columns([2.5, 1.2])
        with f1: search  = st.text_input("🔍 Search Product", placeholder="e.g. Fan, Massager…")
        with f2: st_filt = st.selectbox("Status", ["All", "In Stock", "Low Stock", "Out of Stock"])

        filt = df.copy()
        if search:           filt = filt[filt["Title of Products"].str.contains(search, case=False, na=False)]
        if st_filt != "All": filt = filt[filt["Status"] == st_filt]

        def style_status(val):
            c = {"In Stock":"color:#027A48;font-weight:700", "Low Stock":"color:#B45309;font-weight:700", "Out of Stock":"color:#B42318;font-weight:700"}
            return c.get(val, "")

        display_cols = ["Product ID", "Title of Products", "Price ($)", "Discount (%)", "Products in Store", "Products Sold", "Stock_Value", "Status"]
        st.dataframe(
            filt[display_cols].style.map(style_status, subset=["Status"]),
            use_container_width=True, hide_index=True, height=420
        )
        st.caption(f"Showing {len(filt)} of {len(df)} products")

    with tab2:
        st.markdown('<div class="section-header">🔍 Lookup Product by ID</div>', unsafe_allow_html=True)
        product_id = st.number_input("Enter Product ID", min_value=1, step=1)

        if st.button("Fetch Product"):
            product, from_cache = get_product(int(product_id))
            if product:
                badge = "🟢 Served from Redis Cache" if from_cache else "🔵 Fetched from PostgreSQL"
                st.caption(badge)
                st.json({k: v for k, v in product.items()})
            else:
                st.error("Product not found.")

    with tab3:
        st.markdown('<div class="section-header">➕ Add New Product</div>', unsafe_allow_html=True)
        if is_admin:
            with st.form("add_product_form"):
                a1, a2 = st.columns(2)
                with a1:
                    prod_title = st.text_input("Title of Product *")
                    price      = st.number_input("Price ($) *", min_value=0.0, format="%.2f")
                    discount   = st.number_input("Discount (%)", min_value=0, max_value=100)
                with a2:
                    in_store = st.number_input("Products in Store *", min_value=0)
                    sold     = st.number_input("Products Sold", min_value=0)

                if st.form_submit_button("💾 Add to PostgreSQL"):
                    if prod_title and price:
                        success, msg = add_product(prod_title, price, discount, in_store, sold)
                        if success:
                            st.success(msg)
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.error("Title and Price are required.")
        else:
            st.warning("🔒 Admin access required. Enter the admin password in the sidebar.")

    with tab4:
        st.markdown('<div class="section-header">🗑️ Remove Product</div>', unsafe_allow_html=True)
        if is_admin:
            search_del = st.text_input("🔍 Search product to find its ID", placeholder="e.g. Fan…")
            if search_del:
                results = df[df["Title of Products"].str.contains(search_del, case=False, na=False)]
                if not results.empty:
                    st.dataframe(results[["Product ID", "Title of Products", "Products in Store"]], use_container_width=True, hide_index=True)

            st.markdown("---")
            with st.form("remove_product_form"):
                del_id = st.number_input("Product ID to Remove *", min_value=1, step=1)

                if del_id:
                    match = df[df["Product ID"] == del_id]
                    if not match.empty:
                        st.warning(f"You are about to remove: **{match.iloc[0]['Title of Products'][:60]}**")

                if st.form_submit_button("🗑️ Remove Product"):
                    success, msg = remove_product(int(del_id))
                    if success:
                        st.success(msg)
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error(msg)
        else:
            st.warning("🔒 Admin access required. Enter the admin password in the sidebar.")


# ════════════════════════════════════════════════════════════════════════════
# PAGE: ORDERS
# ════════════════════════════════════════════════════════════════════════════
elif "Orders" in page:
    st.markdown('<div class="section-header">🚚 Orders</div>', unsafe_allow_html=True)

    tab_o1, tab_o2 = st.tabs(["📦  Place Order", "🔄  Restock"])

    with tab_o1:
        st.markdown('<div class="section-header">📦 Place an Order</div>', unsafe_allow_html=True)

        search_order = st.text_input("🔍 Search product by name", placeholder="e.g. Fan, Massager…")
        if search_order:
            results = df[df["Title of Products"].str.contains(search_order, case=False, na=False)]
            if not results.empty:
                st.dataframe(
                    results[["Product ID", "Title of Products", "Products in Store", "Price ($)"]],
                    use_container_width=True, hide_index=True, height=200
                )
            else:
                st.warning("No products found.")

        st.markdown("---")
        with st.form("order_form"):
            o1, o2 = st.columns(2)
            with o1:
                order_id = st.number_input("Product ID *", min_value=1, step=1)
            with o2:
                order_qty = st.number_input("Quantity *", min_value=1, step=1)

            if order_id:
                match = df[df["Product ID"] == order_id]
                if not match.empty:
                    stock = int(match.iloc[0]["Products in Store"])
                    title = match.iloc[0]["Title of Products"]
                    if stock == 0:
                        st.error(f"🚫 '{title[:50]}' is out of stock.")
                    elif stock <= 15:
                        st.warning(f"⚠️ Low stock: only {stock} units left for '{title[:50]}'.")
                    else:
                        st.success(f"✅ {stock} units in stock for '{title[:50]}'.")

            submitted = st.form_submit_button("🚚 Place Order")
            if submitted:
                success, msg = place_order(int(order_id), int(order_qty))
                if success:
                    st.success(msg)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg)

    with tab_o2:
        st.markdown('<div class="section-header">🔄 Restock Product</div>', unsafe_allow_html=True)

        with st.form("restock_form"):
            r1, r2 = st.columns(2)
            with r1:
                restock_id = st.number_input("Product ID *", min_value=1, step=1, key="restock_id")
            with r2:
                restock_qty = st.number_input("Units to Add *", min_value=1, step=1, key="restock_qty")

            if restock_id:
                match = df[df["Product ID"] == restock_id]
                if not match.empty:
                    st.info(f"Current stock: {int(match.iloc[0]['Products in Store'])} units — '{match.iloc[0]['Title of Products'][:50]}'")

            if st.form_submit_button("🔄 Restock"):
                success, msg = restock_product(int(restock_id), int(restock_qty))
                if success:
                    st.success(msg)
                    st.cache_data.clear()
                    st.rerun()
                else:
                    st.error(msg)


# ════════════════════════════════════════════════════════════════════════════
# PAGE: ALERTS
# ════════════════════════════════════════════════════════════════════════════
elif "Alerts" in page:

    st.markdown('<div class="section-header">🚨 Stock Alerts & Notifications</div>', unsafe_allow_html=True)

    out_df = get_out_of_stock()
    low_df = get_low_stock(threshold=15)
    in_stock_count = len(df[df["Status"] == "In Stock"])

    a1, a2, a3 = st.columns(3)
    with a1:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">Out of Stock <span class="kpi-icon">🚫</span></div>
            <div class="kpi-value" style="color:#F04438;">{len(out_df)}</div>
            <div class="kpi-delta down">Immediate action needed</div>
        </div>""", unsafe_allow_html=True)
    with a2:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">Low Stock <span class="kpi-icon">⚠️</span></div>
            <div class="kpi-value" style="color:#F79009;">{len(low_df)}</div>
            <div class="kpi-delta down">Below reorder level</div>
        </div>""", unsafe_allow_html=True)
    with a3:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">Healthy Stock <span class="kpi-icon">✅</span></div>
            <div class="kpi-value" style="color:#12B76A;">{in_stock_count}</div>
            <div class="kpi-delta up">No action needed</div>
        </div>""", unsafe_allow_html=True)

    st.markdown('<div class="section-header">🚫 Out of Stock Items</div>', unsafe_allow_html=True)
    if not out_df.empty:
        st.dataframe(out_df[["Product ID", "Title of Products", "Products in Store", "Products Sold"]], use_container_width=True, hide_index=True, height=220)
    else:
        st.success("No out of stock items!")

    st.markdown('<div class="section-header">⚠️ Low Stock Items (≤ 15 units)</div>', unsafe_allow_html=True)
    if not low_df.empty:
        st.dataframe(low_df[["Product ID", "Title of Products", "Products in Store", "Products Sold"]], use_container_width=True, hide_index=True, height=220)
    else:
        st.success("No low stock items!")


# ════════════════════════════════════════════════════════════════════════════
# PAGE: REPORTS
# ════════════════════════════════════════════════════════════════════════════
elif "Reports" in page:

    st.markdown('<div class="section-header">📊 Reports</div>', unsafe_allow_html=True)

    r1, r2 = st.columns(2)

    with r1:
        top10 = df.nlargest(10, "Stock_Value")[["Title of Products", "Stock_Value"]]
        top10["Title of Products"] = top10["Title of Products"].str[:35]
        fig_t = px.bar(top10, x="Stock_Value", y="Title of Products", orientation="h",
                       color_discrete_sequence=["#875BF7"],
                       labels={"Stock_Value": "Stock Value", "Title of Products": ""})
        fig_t.update_layout(
            title="Top 10 Products by Stock Value",
            title_font=dict(size=13, family="Plus Jakarta Sans"),
            height=340, margin=dict(l=0,r=0,t=35,b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Plus Jakarta Sans"),
            xaxis=dict(showgrid=True, gridcolor="#E9E8F5"),
            yaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig_t, use_container_width=True)

    with r2:
        top_sold = df.nlargest(10, "Products Sold")[["Title of Products", "Products Sold"]]
        top_sold["Title of Products"] = top_sold["Title of Products"].str[:35]
        fig_s = px.bar(top_sold, x="Products Sold", y="Title of Products", orientation="h",
                       color_discrete_sequence=["#F79009"],
                       labels={"Products Sold": "Units Sold", "Title of Products": ""})
        fig_s.update_layout(
            title="Top 10 Products by Units Sold",
            title_font=dict(size=13, family="Plus Jakarta Sans"),
            height=340, margin=dict(l=0,r=0,t=35,b=0),
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font=dict(family="Plus Jakarta Sans"),
            xaxis=dict(showgrid=True, gridcolor="#E9E8F5"),
            yaxis=dict(showgrid=False),
        )
        st.plotly_chart(fig_s, use_container_width=True)

    st.markdown('<div class="section-header">⚡ Most Accessed Products (Redis)</div>', unsafe_allow_html=True)
    top_cached = get_top_products(10)
    if top_cached:
        st.dataframe(pd.DataFrame(top_cached), use_container_width=True, hide_index=True)
    else:
        st.info("No cache hits yet. Use the Inventory lookup to start tracking.")

    st.markdown('<div class="section-header">💰 Price Distribution</div>', unsafe_allow_html=True)
    fig_price = px.histogram(df, x="Price ($)", nbins=30,
                             color_discrete_sequence=["#875BF7"],
                             labels={"Price ($)": "Price ($)"})
    fig_price.update_layout(
        height=250, margin=dict(l=0,r=0,t=10,b=0),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Plus Jakarta Sans"),
        xaxis=dict(showgrid=True, gridcolor="#E9E8F5"),
        yaxis=dict(showgrid=True, gridcolor="#E9E8F5"),
    )
    st.plotly_chart(fig_price, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# PAGE: SETTINGS
# ════════════════════════════════════════════════════════════════════════════
elif "Settings" in page:

    st.markdown('<div class="section-header">⚙️ System Configuration</div>', unsafe_allow_html=True)

    st1, st2 = st.columns(2)

    with st1:
        st.markdown("**🐘 PostgreSQL Configuration**")
        with st.form("pg_form"):
            pg_host = st.text_input("Host", value="localhost")
            pg_port = st.number_input("Port", value=5433)
            pg_db   = st.text_input("Database", value="mydatabase")
            pg_user = st.text_input("Username", value="myuser")
            pg_pass = st.text_input("Password", type="password")
            if st.form_submit_button("🔗 Test Connection"):
                try:
                    import psycopg2
                    conn = psycopg2.connect(host=pg_host, database=pg_db, user=pg_user, password=pg_pass, port=pg_port)
                    conn.close()
                    st.success("✅ PostgreSQL connection successful!")
                except Exception as e:
                    st.error(f"❌ Connection failed: {e}")

        st.markdown("**⚡ Redis Configuration**")
        with st.form("redis_form"):
            r_host = st.text_input("Redis Host", value="localhost")
            r_port = st.number_input("Redis Port", value=6379)
            if st.form_submit_button("🔗 Test Redis Connection"):
                try:
                    import redis
                    r = redis.Redis(host=r_host, port=int(r_port), decode_responses=True)
                    r.ping()
                    st.success("✅ Redis PONG received!")
                except Exception as e:
                    st.error(f"❌ Redis connection failed: {e}")

    with st2:
        st.markdown("**📦 Database Info**")
        st.code(f"""
Total Products : {len(df)}
Out of Stock   : {out_count}
Low Stock      : {low_count}
In Stock       : {len(df[df['Status'] == 'In Stock'])}
        """)
