import os
import sys
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dashboard.queries import (
    category_breakdown,
    customer_segments,
    kpis_today,
    payment_method_failure,
    revenue_trend,
    top_customers,
)

load_dotenv()

SEGMENT_COLORS = {"VIP": "#FFD700", "Regular": "#636EFA", "Casual": "#EF553B"}
SEGMENT_ICONS = {"VIP": "🥇", "Regular": "🥈", "Casual": "🥉"}

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="StreamFlow Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## StreamFlow")
    st.caption("E-Commerce Data Pipeline")
    st.divider()

    gcp_project = st.text_input(
        "GCP Project ID",
        value=os.getenv("GCP_PROJECT_ID", ""),
        help="Your Google Cloud project ID",
    )
    credentials_path = st.text_input(
        "Service Account JSON path",
        value=os.getenv("GCP_CREDENTIALS_PATH", ""),
        type="password",
        help="Path to your GCP service account keyfile",
    )
    dataset_prefix = st.text_input(
        "dbt dataset prefix",
        value=os.getenv("BIGQUERY_DATASET", "streamflow"),
        help="Base name dbt uses when creating datasets (e.g. 'streamflow' → 'streamflow_marts')",
    )
    days_back = st.selectbox("Time window (days)", [7, 14, 30, 90], index=2)

    st.divider()
    if st.button("Refresh", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Updated: {datetime.now().strftime('%H:%M:%S')}")

marts = f"{gcp_project}.{dataset_prefix}_marts"

# ── BigQuery helpers ──────────────────────────────────────────────────────────
@st.cache_resource
def get_client(project: str, creds_path: str) -> bigquery.Client:
    if creds_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    return bigquery.Client(project=project)


@st.cache_data(ttl=300, show_spinner=False)
def run_query(_client: bigquery.Client, sql: str) -> pd.DataFrame:
    return _client.query(sql).to_dataframe()


def safe_query(client, sql: str, label: str = "data"):
    try:
        df = run_query(client, sql)
        return df if not df.empty else None
    except NotFound:
        st.warning(f"**{label}** table not found — run `dbt run` first.", icon="⚠️")
        return None
    except Exception as exc:
        st.error(f"Error loading {label}: {exc}")
        return None


# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("# StreamFlow Analytics")
st.caption("Live view of your e-commerce pipeline · BigQuery + dbt")

if not gcp_project:
    st.info("Enter your GCP Project ID in the sidebar to connect.", icon="ℹ️")
    st.stop()

try:
    client = get_client(gcp_project, credentials_path)
except Exception as exc:
    st.error(f"BigQuery connection failed: {exc}")
    st.stop()

# ── KPI row ───────────────────────────────────────────────────────────────────
with st.spinner("Loading…"):
    kpi = safe_query(client, kpis_today(marts), "daily revenue")

col1, col2, col3, col4 = st.columns(4)
if kpi is not None:
    col1.metric("Revenue Today", f"${kpi['revenue_today'][0]:,.2f}")
    col2.metric("Orders Today", f"{int(kpi['orders_today'][0]):,}")
    col3.metric("Avg Order Value", f"${kpi['avg_order_value'][0]:,.2f}")
    col4.metric("Failure Rate", f"{kpi['avg_failure_rate'][0]:.1f}%")
else:
    for col in (col1, col2, col3, col4):
        col.metric("—", "No data yet")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Revenue Trends",
    "🗂 Category Breakdown",
    "👥 Top Customers",
    "💳 Payment Methods",
    "🏷 Customer Segments",
])

# ── Tab 1: Revenue Trends ─────────────────────────────────────────────────────
with tab1:
    with st.spinner("Loading…"):
        trend_df = safe_query(client, revenue_trend(marts, days_back), "revenue trend")

    if trend_df is not None:
        daily = trend_df.groupby("transaction_date", as_index=False)["daily_revenue"].sum()

        fig = px.area(
            daily,
            x="transaction_date",
            y="daily_revenue",
            title=f"Total Daily Revenue — Last {days_back} Days",
            labels={"transaction_date": "Date", "daily_revenue": "Revenue (USD)"},
            color_discrete_sequence=["#636EFA"],
        )
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)

        fig2 = px.area(
            trend_df,
            x="transaction_date",
            y="daily_revenue",
            color="product_category",
            title="Revenue by Category Over Time",
            labels={
                "transaction_date": "Date",
                "daily_revenue": "Revenue (USD)",
                "product_category": "Category",
            },
        )
        fig2.update_layout(hovermode="x unified")
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No revenue data for the selected time window.")

# ── Tab 2: Category Breakdown ─────────────────────────────────────────────────
with tab2:
    with st.spinner("Loading…"):
        cat_df = safe_query(client, category_breakdown(marts, days_back), "categories")

    if cat_df is not None:
        col_a, col_b = st.columns(2)
        with col_a:
            fig = px.bar(
                cat_df,
                x="revenue",
                y="product_category",
                orientation="h",
                title=f"Revenue by Category — Last {days_back} Days",
                labels={"revenue": "Revenue (USD)", "product_category": "Category"},
                color="revenue",
                color_continuous_scale="Blues",
            )
            fig.update_layout(
                yaxis={"categoryorder": "total ascending"},
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            fig2 = px.bar(
                cat_df,
                x="failure_rate_pct",
                y="product_category",
                orientation="h",
                title="Failure Rate by Category (%)",
                labels={"failure_rate_pct": "Failure Rate (%)", "product_category": "Category"},
                color="failure_rate_pct",
                color_continuous_scale="Reds",
            )
            fig2.update_layout(
                yaxis={"categoryorder": "total ascending"},
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(
            cat_df.rename(columns={
                "product_category": "Category",
                "revenue": "Revenue ($)",
                "orders": "Orders",
                "failure_rate_pct": "Failure Rate (%)",
                "avg_order_value": "Avg Order ($)",
            }),
            use_container_width=True,
            hide_index=True,
        )

# ── Tab 3: Top Customers ──────────────────────────────────────────────────────
with tab3:
    with st.spinner("Loading…"):
        cust_df = safe_query(client, top_customers(marts), "customers")

    if cust_df is not None:
        cust_df["Segment"] = cust_df["customer_segment"].map(
            lambda s: f"{SEGMENT_ICONS.get(s, '')} {s}"
        )

        st.dataframe(
            cust_df[[
                "Segment", "customer_name", "customer_country",
                "total_spend", "total_transactions",
                "avg_transaction_value", "failure_rate_pct", "active_days",
            ]].rename(columns={
                "customer_name": "Name",
                "customer_country": "Country",
                "total_spend": "Lifetime Spend ($)",
                "total_transactions": "Transactions",
                "avg_transaction_value": "Avg Value ($)",
                "failure_rate_pct": "Failure Rate (%)",
                "active_days": "Active Days",
            }),
            use_container_width=True,
            hide_index=True,
        )

        fig = px.scatter(
            cust_df,
            x="total_transactions",
            y="total_spend",
            color="customer_segment",
            size="avg_transaction_value",
            hover_data=["customer_name", "customer_country"],
            title="Lifetime Spend vs Transaction Count",
            labels={
                "total_transactions": "Total Transactions",
                "total_spend": "Lifetime Spend ($)",
                "customer_segment": "Segment",
            },
            color_discrete_map=SEGMENT_COLORS,
        )
        st.plotly_chart(fig, use_container_width=True)

# ── Tab 4: Payment Methods ────────────────────────────────────────────────────
with tab4:
    with st.spinner("Loading…"):
        pay_df = safe_query(client, payment_method_failure(marts), "payment methods")

    if pay_df is not None:
        col_a, col_b = st.columns(2)
        with col_a:
            fig = px.bar(
                pay_df,
                x="payment_method",
                y="failure_rate_pct",
                title="Failure Rate by Payment Method (last 30 days)",
                labels={
                    "payment_method": "Payment Method",
                    "failure_rate_pct": "Failure Rate (%)",
                },
                color="failure_rate_pct",
                color_continuous_scale="Reds",
            )
            fig.update_layout(coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            fig2 = px.pie(
                pay_df,
                names="payment_method",
                values="total_orders",
                title="Order Volume by Payment Method",
                hole=0.4,
            )
            st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(
            pay_df.rename(columns={
                "payment_method": "Payment Method",
                "total_orders": "Total Orders",
                "failed_orders": "Failed",
                "failure_rate_pct": "Failure Rate (%)",
                "total_revenue": "Revenue ($)",
                "avg_order_value": "Avg Order ($)",
            }),
            use_container_width=True,
            hide_index=True,
        )

# ── Tab 5: Customer Segments ──────────────────────────────────────────────────
with tab5:
    with st.spinner("Loading…"):
        seg_df = safe_query(client, customer_segments(marts), "segments")

    if seg_df is not None:
        col_a, col_b = st.columns(2)
        with col_a:
            fig = px.pie(
                seg_df,
                names="customer_segment",
                values="customer_count",
                title="Customer Count by Segment",
                hole=0.4,
                color="customer_segment",
                color_discrete_map=SEGMENT_COLORS,
            )
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            fig2 = px.bar(
                seg_df,
                x="customer_segment",
                y="segment_revenue",
                color="customer_segment",
                title="Revenue by Segment",
                labels={
                    "customer_segment": "Segment",
                    "segment_revenue": "Revenue ($)",
                },
                color_discrete_map=SEGMENT_COLORS,
            )
            fig2.update_layout(showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

        st.dataframe(
            seg_df.rename(columns={
                "customer_segment": "Segment",
                "customer_count": "Customers",
                "segment_revenue": "Total Revenue ($)",
                "avg_spend": "Avg Spend ($)",
            }),
            use_container_width=True,
            hide_index=True,
        )
