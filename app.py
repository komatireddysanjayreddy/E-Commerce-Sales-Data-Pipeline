"""
app.py  -  Local Streamlit Dashboard
=====================================
Simulates the full AWS Sales ETL pipeline in-browser using pandas.

Data input options
------------------
  1. Generate new data   - synthetic 1M+ rows
  2. Load existing CSV   - pre-generated sales_data.csv
  3. Upload CSV file     - drag-and-drop any CSV with the required columns
  4. Manual entry        - add rows one-by-one via a form OR edit a live grid

Run:
    streamlit run app.py
"""

import io
import os
import time
import random
import uuid
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config  (must be first Streamlit call)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="AWS Sales ETL Pipeline - Live Demo",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PRODUCT_CATEGORIES = [
    "Electronics", "Clothing", "Books", "Home & Garden",
    "Sports", "Toys", "Automotive", "Food & Beverage",
    "Health & Beauty", "Office Supplies",
]
CATEGORY_WEIGHTS = [0.20, 0.15, 0.10, 0.12, 0.08, 0.08, 0.07, 0.10, 0.06, 0.04]

REGIONS = [
    "us-east-1", "us-west-2", "eu-west-1",
    "ap-southeast-1", "ap-northeast-1", "sa-east-1", "ca-central-1",
]
REGION_WEIGHTS = [0.30, 0.20, 0.15, 0.12, 0.10, 0.07, 0.06]

START_DATE = datetime(2022, 1, 1)
END_DATE   = datetime(2024, 12, 31, 23, 59, 59)
DELTA_SECS = int((END_DATE - START_DATE).total_seconds())

VALID_CATEGORIES = set(PRODUCT_CATEGORIES)
VALID_REGIONS    = set(REGIONS)
AMOUNT_MIN, AMOUNT_MAX = 1.0, 50_000.0

DATA_PATH = Path("data_generation/sales_data.csv")

REQUIRED_COLUMNS = {"transaction_id", "customer_id", "product_category",
                    "amount", "timestamp", "region"}

# ---------------------------------------------------------------------------
# Session-state initialisation
# ---------------------------------------------------------------------------
def _init_state():
    if "manual_rows" not in st.session_state:
        st.session_state.manual_rows = _empty_manual_df()

def _empty_manual_df() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "transaction_id", "customer_id", "product_category",
        "amount", "timestamp", "region",
    ])

def _new_row(customer_id, category, amount, ts, region) -> dict:
    return {
        "transaction_id":   str(uuid.uuid4()),
        "customer_id":      customer_id,
        "product_category": category,
        "amount":           float(amount),
        "timestamp":        pd.Timestamp(ts),
        "region":           region,
    }

# ---------------------------------------------------------------------------
# Stage 1 helpers  -  data loading / generation
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def generate_data(num_records: int, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    random.seed(seed)

    transaction_ids = [str(uuid.uuid4()) for _ in range(num_records)]
    customer_ids    = [f"CUST_{random.randint(1, 100_000):06d}" for _ in range(num_records)]
    categories      = np.random.choice(PRODUCT_CATEGORIES, num_records, p=CATEGORY_WEIGHTS)
    regions         = np.random.choice(REGIONS,             num_records, p=REGION_WEIGHTS)
    amounts         = np.round(np.random.lognormal(mean=4.0, sigma=1.2, size=num_records), 2)
    amounts         = np.clip(amounts, AMOUNT_MIN, AMOUNT_MAX)
    random_secs     = np.random.randint(0, DELTA_SECS, size=num_records)
    timestamps      = [START_DATE + timedelta(seconds=int(s)) for s in random_secs]

    df = pd.DataFrame({
        "transaction_id":   transaction_ids,
        "customer_id":      customer_ids,
        "product_category": categories,
        "amount":           amounts,
        "timestamp":        timestamps,
        "region":           regions,
    })

    n = num_records
    df.loc[np.random.choice(n, int(n * 0.01), replace=False), "amount"]      = None
    df.loc[np.random.choice(n, int(n * 0.01), replace=False), "customer_id"] = None
    n_dups   = int(n * 0.005)
    dup_rows = df.sample(n=n_dups, random_state=seed)
    df       = pd.concat([df, dup_rows], ignore_index=True)
    return df


@st.cache_data(show_spinner=False)
def load_existing_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, parse_dates=["timestamp"])


def parse_uploaded_csv(file_obj) -> tuple[pd.DataFrame, list[str]]:
    """Read an uploaded file, validate columns, return (df, errors)."""
    errors = []
    try:
        df = pd.read_csv(file_obj)
    except Exception as exc:
        return pd.DataFrame(), [f"Could not parse CSV: {exc}"]

    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        errors.append(f"Missing columns: {sorted(missing)}")
        return df, errors

    try:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    except Exception:
        errors.append("Could not parse 'timestamp' column as datetime.")

    return df, errors


# ---------------------------------------------------------------------------
# Manual entry UI  -  returns DataFrame (or None if nothing entered yet)
# ---------------------------------------------------------------------------
def render_manual_entry() -> pd.DataFrame | None:
    """
    Renders two sub-sections:
      A) Single-record form (Add Row button)
      B) Editable grid (st.data_editor with dynamic rows)

    Returns the current DataFrame from the grid, or None if empty.
    """
    _init_state()

    st.markdown("### Manual Data Entry")
    st.caption(
        "Use the form below to add records one at a time, "
        "or edit the table directly. Click **Run Pipeline** when ready."
    )

    # ---- A) Per-record form --------------------------------------------------
    with st.expander("Add a single record via form", expanded=True):
        with st.form("add_row_form", clear_on_submit=True):
            c1, c2, c3 = st.columns(3)
            customer_id = c1.text_input(
                "Customer ID",
                value=f"CUST_{random.randint(1, 100_000):06d}",
                help="Format: CUST_XXXXXX",
            )
            category = c2.selectbox("Product Category", PRODUCT_CATEGORIES)
            region   = c3.selectbox("Region", REGIONS)

            c4, c5 = st.columns(2)
            amount = c4.number_input(
                "Amount ($)",
                min_value=1.0, max_value=50_000.0,
                value=250.0, step=0.01,
                format="%.2f",
            )
            ts_date = c5.date_input(
                "Transaction Date",
                value=date(2024, 1, 15),
                min_value=date(2022, 1, 1),
                max_value=date(2024, 12, 31),
            )

            add_submitted = st.form_submit_button(
                "Add Row", type="primary", use_container_width=True
            )

        if add_submitted:
            new_row = _new_row(
                customer_id.strip() or "CUST_000000",
                category, amount,
                pd.Timestamp(ts_date),
                region,
            )
            st.session_state.manual_rows = pd.concat(
                [st.session_state.manual_rows,
                 pd.DataFrame([new_row])],
                ignore_index=True,
            )
            st.success(f"Row added. Total: {len(st.session_state.manual_rows)}")

    # ---- B) Editable grid ---------------------------------------------------
    st.markdown("#### Live Data Grid")
    st.caption(
        "Edit cells directly. Use the **+** button at the bottom to add a blank row. "
        "Select a row and press **Delete** to remove it."
    )

    edited_df = st.data_editor(
        st.session_state.manual_rows,
        num_rows="dynamic",
        use_container_width=True,
        hide_index=True,
        column_config={
            "transaction_id": st.column_config.TextColumn(
                "Transaction ID",
                help="Auto-generated UUID (leave blank to auto-fill)",
                width="large",
            ),
            "customer_id": st.column_config.TextColumn(
                "Customer ID",
                help="e.g. CUST_012345",
                width="medium",
            ),
            "product_category": st.column_config.SelectboxColumn(
                "Category",
                options=PRODUCT_CATEGORIES,
                required=True,
                width="medium",
            ),
            "amount": st.column_config.NumberColumn(
                "Amount ($)",
                min_value=1.0, max_value=50_000.0,
                step=0.01, format="$%.2f",
                required=True,
            ),
            "timestamp": st.column_config.DatetimeColumn(
                "Timestamp",
                min_value=datetime(2022, 1, 1),
                max_value=datetime(2024, 12, 31),
                format="YYYY-MM-DD HH:mm:ss",
                required=True,
            ),
            "region": st.column_config.SelectboxColumn(
                "Region",
                options=REGIONS,
                required=True,
                width="medium",
            ),
        },
        key="manual_grid",
    )

    # Auto-fill missing transaction_ids from the grid
    if edited_df is not None and len(edited_df) > 0:
        mask = edited_df["transaction_id"].isna() | (edited_df["transaction_id"].astype(str).str.strip() == "")
        edited_df.loc[mask, "transaction_id"] = [str(uuid.uuid4()) for _ in range(mask.sum())]
        st.session_state.manual_rows = edited_df.reset_index(drop=True)

    row_count = len(st.session_state.manual_rows)
    col_a, col_b = st.columns([3, 1])
    col_a.info(f"{row_count} row(s) in the grid")

    if col_b.button("Clear all rows", use_container_width=True):
        st.session_state.manual_rows = _empty_manual_df()
        st.rerun()

    # ---- CSV template download ----------------------------------------------
    template = pd.DataFrame([{
        "transaction_id":   str(uuid.uuid4()),
        "customer_id":      "CUST_001234",
        "product_category": "Electronics",
        "amount":           499.99,
        "timestamp":        "2024-03-15 10:30:00",
        "region":           "us-east-1",
    }])
    st.download_button(
        "Download CSV template (1 sample row)",
        template.to_csv(index=False).encode(),
        "sales_template.csv", "text/csv",
        use_container_width=True,
    )

    if row_count == 0:
        return None
    return st.session_state.manual_rows.copy()


# ---------------------------------------------------------------------------
# Stage 2 - ETL Cleaning  (mirrors Glue Job 1)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def run_etl_cleaning(df: pd.DataFrame):
    stats = {"raw": len(df)}

    df = df[df["transaction_id"].notna()].copy()
    stats["after_pk_null"] = len(df)

    df = df.drop_duplicates(subset=["transaction_id"], keep="first")
    stats["after_dedup"] = len(df)

    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df[df["amount"].notna() & (df["amount"] >= AMOUNT_MIN) & (df["amount"] <= AMOUNT_MAX)]
    stats["after_amount"] = len(df)

    df = df[df["timestamp"].notna()].copy()
    stats["after_ts"] = len(df)

    df["product_category"] = df["product_category"].apply(
        lambda c: c if c in VALID_CATEGORIES else "UNKNOWN"
    )
    df["region"] = df["region"].apply(
        lambda r: r if r in VALID_REGIONS else "UNKNOWN"
    )
    df["customer_id"] = df["customer_id"].fillna("UNKNOWN")

    df["_ingestion_ts"]     = pd.Timestamp.now()
    df["_source_system"]    = "sales_csv_landing"
    df["_pipeline_version"] = "1.0.0"

    stats["final"] = len(df)
    return df, stats


# ---------------------------------------------------------------------------
# Stage 3 - KPI Computation  (mirrors Glue Job 2)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def run_kpi_computation(df: pd.DataFrame):
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["year"]  = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month
    df["day"]   = df["timestamp"].dt.day

    # KPI 1 - Revenue per Category
    grand_total = df["amount"].sum()
    kpi1 = (
        df.groupby("product_category")
          .agg(
              total_revenue         =("amount", "sum"),
              transaction_count     =("transaction_id", "count"),
              avg_transaction_value =("amount", "mean"),
              min_amount            =("amount", "min"),
              max_amount            =("amount", "max"),
              unique_customers      =("customer_id", "nunique"),
          )
          .reset_index()
    )
    kpi1["revenue_pct"]           = (kpi1["total_revenue"] / grand_total * 100).round(2)
    kpi1["total_revenue"]         = kpi1["total_revenue"].round(2)
    kpi1["avg_transaction_value"] = kpi1["avg_transaction_value"].round(2)
    kpi1 = kpi1.sort_values("total_revenue", ascending=False).reset_index(drop=True)

    # KPI 2 - Monthly Sales Growth
    monthly = (
        df.groupby(["year", "month"])
          .agg(
              total_revenue         =("amount", "sum"),
              transaction_count     =("transaction_id", "count"),
              unique_customers      =("customer_id", "nunique"),
              avg_transaction_value =("amount", "mean"),
          )
          .reset_index()
          .sort_values(["year", "month"])
    )
    monthly["month_label"]           = (monthly["year"].astype(str) + "-"
                                        + monthly["month"].astype(str).str.zfill(2))
    monthly["prev_month_revenue"]    = monthly["total_revenue"].shift(1)
    monthly["mom_growth_pct"]        = (
        (monthly["total_revenue"] - monthly["prev_month_revenue"])
        / monthly["prev_month_revenue"] * 100
    ).round(2)
    monthly["total_revenue"]         = monthly["total_revenue"].round(2)
    monthly["avg_transaction_value"] = monthly["avg_transaction_value"].round(2)
    kpi2 = monthly.reset_index(drop=True)

    # KPI 3 - Top 5 Regions by Volume
    total_txns = len(df)
    total_rev  = df["amount"].sum()
    region_agg = (
        df.groupby("region")
          .agg(
              transaction_count   =("transaction_id", "count"),
              total_revenue       =("amount", "sum"),
              avg_revenue_per_txn =("amount", "mean"),
              unique_customers    =("customer_id", "nunique"),
          )
          .reset_index()
    )
    region_agg["volume_share_pct"]   = (region_agg["transaction_count"] / total_txns * 100).round(2)
    region_agg["revenue_share_pct"]  = (region_agg["total_revenue"]     / total_rev  * 100).round(2)
    region_agg["total_revenue"]      = region_agg["total_revenue"].round(2)
    region_agg["avg_revenue_per_txn"]= region_agg["avg_revenue_per_txn"].round(2)
    region_agg = (region_agg.sort_values("transaction_count", ascending=False)
                             .head(5).reset_index(drop=True))
    region_agg.insert(0, "rank", range(1, len(region_agg) + 1))
    kpi3 = region_agg

    return kpi1, kpi2, kpi3, df


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def metric_card(col, label, value, delta="", delta_color="normal"):
    col.metric(label=label, value=value,
               delta=delta if delta else None, delta_color=delta_color)


def fmt_currency(val: float) -> str:
    if val >= 1_000_000:
        return f"${val/1_000_000:.2f}M"
    if val >= 1_000:
        return f"${val/1_000:.1f}K"
    return f"${val:.2f}"


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------
def main():
    _init_state()

    # ---- Sidebar ------------------------------------------------------------
    st.sidebar.title("Pipeline Controls")
    st.sidebar.markdown("---")

    data_source = st.sidebar.radio(
        "Data Source",
        [
            "Generate new data",
            "Load existing CSV",
            "Upload CSV file",
            "Manual entry",
        ],
        help=(
            "Generate synthetic data, load a pre-existing CSV, "
            "upload your own CSV, or enter records manually."
        ),
    )

    # Source-specific sidebar controls
    num_records = 200_000
    seed = 42
    uploaded_file = None

    if data_source == "Generate new data":
        num_records = st.sidebar.select_slider(
            "Records to generate",
            options=[10_000, 50_000, 100_000, 200_000, 500_000, 1_000_000],
            value=200_000,
            format_func=lambda x: f"{x:,}",
        )
        seed = st.sidebar.number_input("Random seed", value=42, min_value=0)

    elif data_source == "Upload CSV file":
        uploaded_file = st.sidebar.file_uploader(
            "Choose a CSV file",
            type=["csv"],
            help=(
                f"Required columns: {sorted(REQUIRED_COLUMNS)}\n"
                "Download a template from the Manual Entry section."
            ),
        )

    st.sidebar.markdown("---")
    st.sidebar.markdown(
        """
        **Required CSV columns**
        - `transaction_id`
        - `customer_id`
        - `product_category`
        - `amount`
        - `timestamp`
        - `region`
        """
    )

    run_btn = st.sidebar.button(
        "Run Pipeline", type="primary", use_container_width=True
    )

    # ---- Header -------------------------------------------------------------
    st.title("AWS Sales ETL Pipeline")
    st.markdown(
        "**Local simulation** of the full AWS pipeline: "
        "S3 Landing -> Glue ETL Cleaning -> Glue KPI Jobs -> Redshift-ready Parquet"
    )

    with st.expander("Architecture Overview", expanded=False):
        st.markdown("""
```
Data Input (any of the 4 sources)
    |
    v
[S3 Landing Zone  -  raw CSV]
    |
    v  Glue Job 1 (PySpark)
[S3 Cleansed Zone -  Parquet]  schema cast, null removal, dedup, validation
    |
    v  Glue Job 2 (PySpark)
[S3 Curated Zone  -  Parquet]  partitioned year/month/day + KPI tables
    |
    v  COPY command
[Amazon Redshift]  fact_sales | kpi_revenue_per_category
                               | kpi_monthly_sales_growth
                               | kpi_top_regions_by_volume
```
        """)

    # ---- Manual-entry section is shown BEFORE Run Pipeline ------------------
    manual_df = None
    if data_source == "Manual entry":
        manual_df = render_manual_entry()
        st.markdown("---")

    # ---- Gate on Run Pipeline button ----------------------------------------
    if not run_btn:
        st.info("Configure options in the sidebar and click **Run Pipeline** to start.")
        st.stop()

    # ---- Stage 1: Ingest data -----------------------------------------------
    st.markdown("---")
    st.subheader("Stage 1 - Data Ingestion (S3 Landing Zone)")

    with st.spinner("Loading / preparing data..."):
        t0 = time.time()

        if data_source == "Generate new data":
            raw_df    = generate_data(num_records, seed=seed)
            data_info = f"Generated {len(raw_df):,} rows (incl. ~0.5% duplicates + ~1% nulls)"

        elif data_source == "Load existing CSV":
            if not DATA_PATH.exists():
                st.error(f"CSV not found at `{DATA_PATH}`. Switch to 'Generate new data'.")
                st.stop()
            raw_df    = load_existing_csv(DATA_PATH)
            data_info = f"Loaded {len(raw_df):,} rows from `{DATA_PATH}`"

        elif data_source == "Upload CSV file":
            if uploaded_file is None:
                st.warning("Please upload a CSV file in the sidebar first.")
                st.stop()
            raw_df, errors = parse_uploaded_csv(uploaded_file)
            if errors:
                for err in errors:
                    st.error(err)
                st.stop()
            raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"], errors="coerce")
            data_info = f"Uploaded: **{uploaded_file.name}** - {len(raw_df):,} rows"

        else:  # Manual entry
            if manual_df is None or len(manual_df) == 0:
                st.warning("No rows entered yet. Add at least one record in the grid above.")
                st.stop()
            raw_df    = manual_df.copy()
            raw_df["timestamp"] = pd.to_datetime(raw_df["timestamp"], errors="coerce")
            data_info = f"Manual input: {len(raw_df):,} row(s) entered"

        t_load = time.time() - t0

    c1, c2, c3, c4 = st.columns(4)
    metric_card(c1, "Total Raw Rows",    f"{len(raw_df):,}")
    metric_card(c2, "Null amounts",      f"{raw_df['amount'].isna().sum():,}")
    metric_card(c3, "Null customer_ids", f"{raw_df['customer_id'].isna().sum():,}")
    metric_card(c4, "Load time",         f"{t_load:.2f}s")
    st.caption(data_info)

    with st.expander("Raw data sample (first 10 rows)"):
        st.dataframe(raw_df.head(10), use_container_width=True)

    # ---- Stage 2: ETL Cleaning ----------------------------------------------
    st.markdown("---")
    st.subheader("Stage 2 - ETL Cleaning (Glue Job 1 simulation)")

    with st.spinner("Running cleaning pipeline..."):
        t1 = time.time()
        clean_df, stats = run_etl_cleaning(raw_df.copy())
        t_clean = time.time() - t1

    removed      = stats["raw"] - stats["final"]
    retained_pct = stats["final"] / stats["raw"] * 100 if stats["raw"] > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    metric_card(c1, "Clean rows",     f"{stats['final']:,}")
    metric_card(c2, "Rows removed",   f"{removed:,}", delta=f"-{removed:,}", delta_color="inverse")
    metric_card(c3, "Retention rate", f"{retained_pct:.1f}%")
    metric_card(c4, "Cleaning time",  f"{t_clean:.2f}s")

    with st.expander("Cleaning pipeline step-by-step"):
        steps = [
            ("Raw input",              stats["raw"]),
            ("After PK null drop",     stats["after_pk_null"]),
            ("After deduplication",    stats["after_dedup"]),
            ("After amount filter",    stats["after_amount"]),
            ("After timestamp filter", stats["after_ts"]),
            ("Final clean rows",       stats["final"]),
        ]
        step_df = pd.DataFrame(steps, columns=["Step", "Row Count"])
        step_df["Dropped at this step"] = (
            step_df["Row Count"].shift(1, fill_value=stats["raw"]) - step_df["Row Count"]
        ).clip(lower=0).astype(int)

        fig = px.bar(
            step_df, x="Step", y="Row Count", text="Row Count",
            color_discrete_sequence=["#1f77b4"],
            title="Row count after each cleaning step",
        )
        fig.update_traces(texttemplate="%{text:,}", textposition="outside")
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(step_df, use_container_width=True, hide_index=True)

    if stats["final"] == 0:
        st.error("No rows remain after cleaning. Check your input data.")
        st.stop()

    # ---- Stage 3: KPI Computation -------------------------------------------
    st.markdown("---")
    st.subheader("Stage 3 - KPI Computation (Glue Job 2 simulation)")

    with st.spinner("Computing KPIs..."):
        t2 = time.time()
        kpi1, kpi2, kpi3, enriched_df = run_kpi_computation(clean_df)
        t_kpi = time.time() - t2

    c1, c2, c3, c4 = st.columns(4)
    metric_card(c1, "Total Revenue",      fmt_currency(kpi1["total_revenue"].sum()))
    metric_card(c2, "Total Transactions", f"{kpi1['transaction_count'].sum():,}")
    metric_card(c3, "Unique Customers",   f"{clean_df['customer_id'].nunique():,}")
    metric_card(c4, "KPI compute time",   f"{t_kpi:.2f}s")

    # ---- Stage 4: KPI Results -----------------------------------------------
    st.markdown("---")
    st.subheader("Stage 4 - Results (Redshift KPI Tables)")

    tab1, tab2, tab3, tab4 = st.tabs([
        "KPI 1 - Revenue per Category",
        "KPI 2 - Monthly Sales Growth",
        "KPI 3 - Top 5 Regions",
        "Fact Table Preview",
    ])

    # KPI 1 ------------------------------------------------------------------
    with tab1:
        st.markdown("#### Total Revenue per Product Category")
        col_l, col_r = st.columns(2)
        with col_l:
            fig1 = px.bar(
                kpi1, x="total_revenue", y="product_category",
                orientation="h", text="revenue_pct",
                color="total_revenue", color_continuous_scale="Blues",
                labels={"total_revenue": "Revenue ($)", "product_category": "Category"},
                title="Revenue by Category",
            )
            fig1.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig1.update_layout(showlegend=False, height=400, coloraxis_showscale=False)
            st.plotly_chart(fig1, use_container_width=True)
        with col_r:
            fig1b = px.pie(
                kpi1, values="transaction_count", names="product_category",
                title="Transaction Volume Share", hole=0.4,
            )
            fig1b.update_layout(height=400)
            st.plotly_chart(fig1b, use_container_width=True)

        st.dataframe(
            kpi1.rename(columns={
                "product_category":     "Category",
                "total_revenue":        "Revenue ($)",
                "transaction_count":    "Transactions",
                "avg_transaction_value":"Avg Order ($)",
                "min_amount":           "Min ($)",
                "max_amount":           "Max ($)",
                "unique_customers":     "Unique Customers",
                "revenue_pct":          "Revenue %",
            }),
            use_container_width=True, hide_index=True,
        )

    # KPI 2 ------------------------------------------------------------------
    with tab2:
        st.markdown("#### Monthly Sales Revenue & Month-over-Month Growth")
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(
            x=kpi2["month_label"], y=kpi2["total_revenue"],
            name="Revenue ($)", marker_color="#1f77b4", yaxis="y1",
        ))
        fig2.add_trace(go.Scatter(
            x=kpi2["month_label"], y=kpi2["mom_growth_pct"],
            name="MoM Growth %", mode="lines+markers",
            marker=dict(color="orange", size=6),
            line=dict(color="orange", width=2),
            yaxis="y2",
        ))
        fig2.update_layout(
            title="Monthly Revenue + MoM Growth %",
            yaxis=dict(title="Revenue ($)"),
            yaxis2=dict(title="MoM Growth (%)", overlaying="y", side="right", zeroline=True),
            legend=dict(x=0, y=1.1, orientation="h"),
            height=420,
        )
        st.plotly_chart(fig2, use_container_width=True)

        kpi2_disp = kpi2[["month_label", "total_revenue", "transaction_count",
                           "unique_customers", "prev_month_revenue", "mom_growth_pct"]].copy()
        kpi2_disp.columns = ["Month", "Revenue ($)", "Transactions",
                              "Unique Customers", "Prev Month Revenue ($)", "MoM Growth %"]

        def _color_growth(val):
            if pd.isna(val):   return ""
            if val > 0:        return "color: green"
            if val < 0:        return "color: red"
            return ""

        st.dataframe(
            kpi2_disp.style.map(_color_growth, subset=["MoM Growth %"]),
            use_container_width=True, hide_index=True,
        )

    # KPI 3 ------------------------------------------------------------------
    with tab3:
        st.markdown("#### Top 5 Regions by Transaction Volume")
        col_l, col_r = st.columns(2)
        with col_l:
            fig3a = px.bar(
                kpi3, x="transaction_count", y="region",
                orientation="h", text="volume_share_pct",
                color="transaction_count", color_continuous_scale="Teal",
                title="Transaction Volume by Region",
            )
            fig3a.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig3a.update_layout(showlegend=False, height=360, coloraxis_showscale=False)
            st.plotly_chart(fig3a, use_container_width=True)
        with col_r:
            fig3b = px.bar(
                kpi3, x="region", y="total_revenue",
                text="revenue_share_pct", color="region",
                title="Revenue Share by Region",
            )
            fig3b.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig3b.update_layout(showlegend=False, height=360)
            st.plotly_chart(fig3b, use_container_width=True)

        st.dataframe(
            kpi3.rename(columns={
                "rank":               "Rank",
                "region":             "Region",
                "transaction_count":  "Transactions",
                "total_revenue":      "Revenue ($)",
                "avg_revenue_per_txn":"Avg Revenue ($)",
                "unique_customers":   "Unique Customers",
                "volume_share_pct":   "Volume Share %",
                "revenue_share_pct":  "Revenue Share %",
            }),
            use_container_width=True, hide_index=True,
        )

    # Fact table preview -----------------------------------------------------
    with tab4:
        st.markdown("#### Enriched Fact Table (partitioned by year/month/day)")
        c1, c2, c3 = st.columns(3)
        c1.metric("Partitions",  f"{enriched_df.groupby(['year','month','day']).ngroups:,}")
        c2.metric("Year range",
                  f"{int(enriched_df['year'].min())} - {int(enriched_df['year'].max())}")
        c3.metric("Columns",     f"{len(enriched_df.columns)}")

        cf1, cf2 = st.columns(2)
        years = sorted(enriched_df["year"].unique().tolist())
        sel_years = cf1.multiselect("Filter by year",     years,                    default=years)
        cats      = sorted(enriched_df["product_category"].unique().tolist())
        sel_cats  = cf2.multiselect("Filter by category", cats, default=cats[:3])

        preview = enriched_df[
            enriched_df["year"].isin(sel_years) &
            enriched_df["product_category"].isin(sel_cats)
        ].head(500)

        st.dataframe(
            preview[[
                "transaction_id", "customer_id", "product_category",
                "amount", "timestamp", "region", "year", "month", "day",
                "_source_system", "_pipeline_version",
            ]],
            use_container_width=True, height=320,
        )
        st.caption(f"Showing up to 500 of {len(preview):,} filtered rows")

    # ---- Export -------------------------------------------------------------
    st.markdown("---")
    st.subheader("Export Results")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("Download KPI 1 (Revenue/Category)",
                           kpi1.to_csv(index=False).encode(),
                           "kpi_revenue_per_category.csv", "text/csv",
                           use_container_width=True)
    with c2:
        st.download_button("Download KPI 2 (Monthly Growth)",
                           kpi2.to_csv(index=False).encode(),
                           "kpi_monthly_sales_growth.csv", "text/csv",
                           use_container_width=True)
    with c3:
        st.download_button("Download KPI 3 (Top Regions)",
                           kpi3.to_csv(index=False).encode(),
                           "kpi_top_regions_by_volume.csv", "text/csv",
                           use_container_width=True)

    # ---- Save Parquet -------------------------------------------------------
    st.markdown("---")
    st.subheader("Save Parquet Output (mirrors S3 Curated Zone)")
    if st.button("Save all outputs as Parquet", use_container_width=True):
        out = Path("output")
        out.mkdir(exist_ok=True)
        (out / "kpis").mkdir(exist_ok=True)
        with st.spinner("Writing Parquet files..."):
            fact_path = out / "sales"
            fact_path.mkdir(exist_ok=True)
            for (y, m, d), grp in enriched_df.groupby(["year", "month", "day"]):
                part = fact_path / f"year={y}" / f"month={m}" / f"day={d}"
                part.mkdir(parents=True, exist_ok=True)
                grp.drop(columns=["year","month","day"]).to_parquet(
                    part / "data.parquet", index=False, compression="snappy"
                )
            kpi1.to_parquet(out / "kpis" / "revenue_per_category.parquet",  index=False, compression="snappy")
            kpi2.to_parquet(out / "kpis" / "monthly_sales_growth.parquet",  index=False, compression="snappy")
            kpi3.to_parquet(out / "kpis" / "top_regions_by_volume.parquet", index=False, compression="snappy")
        st.success(f"Parquet output written to `{out.resolve()}/`")
        st.code("\n".join(
            str(f.relative_to(out)) for f in sorted(out.rglob("*.parquet"))
        ))

    # ---- Footer -------------------------------------------------------------
    st.markdown("---")
    total_time = t_load + t_clean + t_kpi
    st.caption(
        f"Pipeline complete in {total_time:.2f}s  |  "
        f"{stats['final']:,} clean records  |  "
        f"Grand total revenue: {fmt_currency(kpi1['total_revenue'].sum())}  |  "
        f"Source: {data_source}  |  Powered by pandas + Streamlit"
    )


if __name__ == "__main__":
    main()
