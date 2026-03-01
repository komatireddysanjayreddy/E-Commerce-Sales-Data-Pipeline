"""
api/index.py  -  E-Commerce Sales ETL Pipeline  (Vercel deployment)
====================================================================
Flask single-page dashboard.  Handles 1 M+ records via vectorised
NumPy / Pandas operations; designed to complete within Vercel's
60-second function timeout.

Routes
------
GET  /          HTML dashboard (single-page app)
POST /api/run   Run full pipeline -> JSON (KPIs + Plotly charts)
"""

import io
import json
import time
import uuid
import os
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from flask import Flask, Response, jsonify, request
from plotly.utils import PlotlyJSONEncoder

app = Flask(__name__)

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

AMOUNT_MIN  = 1.0
AMOUNT_MAX  = 50_000.0
START_TS    = pd.Timestamp("2022-01-01")
END_TS      = pd.Timestamp("2024-12-31 23:59:59")
DELTA_SECS  = int((END_TS - START_TS).total_seconds())

MAX_RECORDS = 1_200_000   # 1 M+ records supported


# ---------------------------------------------------------------------------
# Data Generation  (vectorised — mirrors generate_sales_data.py)
# ---------------------------------------------------------------------------

def generate_data(num_records: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n   = min(num_records, MAX_RECORDS)

    # --- Transaction IDs: fully vectorised string concat (no Python loop) ---
    prefix      = int(rng.integers(0, 999_999))
    idx_series  = pd.RangeIndex(n)
    idx_str     = pd.array(idx_series).astype(str)
    txn_ids     = pd.array(pd.Series(idx_str).str.zfill(9).radd(f"TXN-{prefix:06d}-"))

    # --- Customer IDs: vectorised ---
    cust_nums   = rng.integers(1, 500_001, n)
    cust_ids    = pd.array(pd.Series(cust_nums).astype(str).str.zfill(6).radd("CUST-"))

    # --- Categories & Regions: vectorised NumPy choice ---
    categories  = rng.choice(PRODUCT_CATEGORIES, n, p=CATEGORY_WEIGHTS)
    regions     = rng.choice(REGIONS,             n, p=REGION_WEIGHTS)

    # --- Amounts: fully vectorised ---
    amounts     = np.clip(
        np.round(rng.lognormal(4.0, 1.2, n), 2), AMOUNT_MIN, AMOUNT_MAX
    )

    # --- Timestamps: vectorised nanosecond arithmetic ---
    secs        = rng.integers(0, DELTA_SECS, n).astype(np.int64)
    timestamps  = pd.to_datetime(START_TS.value + secs * 1_000_000_000)

    df = pd.DataFrame({
        "transaction_id":   txn_ids,
        "customer_id":      cust_ids,
        "product_category": categories,
        "amount":           amounts,
        "timestamp":        timestamps,
        "region":           regions,
    })

    # --- Inject data-quality issues (vectorised index selection) ---
    null_amt  = rng.choice(n, int(n * 0.01), replace=False)
    null_cust = rng.choice(n, int(n * 0.01), replace=False)
    df.loc[null_amt,  "amount"]      = np.nan
    df.loc[null_cust, "customer_id"] = pd.NA

    n_dups = int(n * 0.005)
    dups   = df.iloc[rng.choice(n, n_dups, replace=False)]
    return pd.concat([df, dups], ignore_index=True)


# ---------------------------------------------------------------------------
# ETL Cleaning  (mirrors 01_landing_to_cleansed.py)
# ---------------------------------------------------------------------------

def run_etl(df: pd.DataFrame):
    t0    = time.time()
    stats = {"raw": len(df)}

    if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    df = df[df["transaction_id"].notna()].copy()
    stats["after_pk_null_drop"] = len(df)

    df = df.drop_duplicates(subset=["transaction_id"])
    stats["after_dedup"] = len(df)

    mask = df["amount"].notna() & df["amount"].between(AMOUNT_MIN, AMOUNT_MAX)
    df   = df[mask].copy()
    stats["after_amount"] = len(df)

    df = df[df["timestamp"].notna()].copy()
    stats["after_ts"] = len(df)

    df["product_category"] = df["product_category"].where(
        df["product_category"].isin(PRODUCT_CATEGORIES), "UNKNOWN")
    df["region"]      = df["region"].where(df["region"].isin(REGIONS), "UNKNOWN")
    df["customer_id"] = df["customer_id"].fillna("UNKNOWN")

    df["year"]  = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    stats["final"]       = len(df)
    stats["etl_secs"]    = round(time.time() - t0, 2)
    stats["retained_pct"]= round(stats["final"] / max(stats["raw"], 1) * 100, 1)
    return df, stats


# ---------------------------------------------------------------------------
# KPI Computation  (mirrors 02_cleansed_to_curated_kpis.py)
# ---------------------------------------------------------------------------

def compute_kpis(df: pd.DataFrame) -> dict:
    grand_total = df["amount"].sum()

    # KPI 1 – Revenue per Category
    kpi1 = (
        df.groupby("product_category", sort=False)
        .agg(
            total_revenue        =("amount",         "sum"),
            transaction_count    =("transaction_id", "count"),
            avg_transaction_value=("amount",         "mean"),
            unique_customers     =("customer_id",    "nunique"),
        )
        .reset_index()
    )
    kpi1["revenue_pct"]           = (kpi1["total_revenue"] / grand_total * 100).round(2)
    kpi1["total_revenue"]         = kpi1["total_revenue"].round(2)
    kpi1["avg_transaction_value"] = kpi1["avg_transaction_value"].round(2)
    kpi1 = kpi1.sort_values("total_revenue", ascending=False).reset_index(drop=True)

    # KPI 2 – Monthly Sales Growth
    kpi2 = (
        df.groupby(["year", "month"], sort=True)
        .agg(
            total_revenue        =("amount",         "sum"),
            transaction_count    =("transaction_id", "count"),
            unique_customers     =("customer_id",    "nunique"),
            avg_transaction_value=("amount",         "mean"),
        )
        .reset_index()
    )
    kpi2["month_label"]        = (kpi2["year"].astype(str) + "-"
                                  + kpi2["month"].astype(str).str.zfill(2))
    kpi2["prev_month_revenue"] = kpi2["total_revenue"].shift(1)
    kpi2["mom_growth_pct"]     = (
        (kpi2["total_revenue"] - kpi2["prev_month_revenue"])
        / kpi2["prev_month_revenue"] * 100
    ).round(2)
    kpi2["total_revenue"]         = kpi2["total_revenue"].round(2)
    kpi2["avg_transaction_value"] = kpi2["avg_transaction_value"].round(2)
    kpi2["prev_month_revenue"]    = kpi2["prev_month_revenue"].round(2)

    # KPI 3 – Top 5 Regions by Volume
    total_txns = len(df)
    total_rev  = grand_total
    kpi3 = (
        df.groupby("region", sort=False)
        .agg(
            transaction_count  =("transaction_id", "count"),
            total_revenue      =("amount",         "sum"),
            avg_revenue_per_txn=("amount",         "mean"),
            unique_customers   =("customer_id",    "nunique"),
        )
        .reset_index()
    )
    kpi3["volume_share_pct"]   = (kpi3["transaction_count"] / total_txns * 100).round(2)
    kpi3["revenue_share_pct"]  = (kpi3["total_revenue"]     / total_rev  * 100).round(2)
    kpi3["total_revenue"]      = kpi3["total_revenue"].round(2)
    kpi3["avg_revenue_per_txn"]= kpi3["avg_revenue_per_txn"].round(2)
    kpi3 = (kpi3.sort_values("transaction_count", ascending=False)
               .head(5).reset_index(drop=True))
    kpi3.insert(0, "rank", range(1, len(kpi3) + 1))

    return {
        "kpi1": kpi1.to_dict("records"),
        "kpi2": kpi2.to_dict("records"),
        "kpi3": kpi3.to_dict("records"),
    }


# ---------------------------------------------------------------------------
# Chart Builder  (premium dark theme)
# ---------------------------------------------------------------------------

_PALETTE   = ["#F97316","#818CF8","#34D399","#FB7185","#38BDF8",
               "#FBBF24","#A78BFA","#6EE7B7","#F472B6","#67E8F9"]
_BG        = "#080C18"
_CARD_BG   = "#0D1426"
_GRID      = "#1E2D4A"
_FONT      = dict(family="Inter, Segoe UI, sans-serif", color="#94A3B8")


def _layout(**kw):
    base = dict(
        template    ="plotly_dark",
        paper_bgcolor=_BG,
        plot_bgcolor =_CARD_BG,
        font         =_FONT,
        margin       =dict(t=55, b=45, l=10, r=10),
        hoverlabel   =dict(bgcolor="#1E2D4A", font_size=12),
        showlegend   =True,
    )
    base.update(kw)   # kw wins, so callers can override showlegend etc.
    return base


def build_charts(kpis: dict) -> dict:
    k1 = pd.DataFrame(kpis["kpi1"])
    k2 = pd.DataFrame(kpis["kpi2"])
    k3 = pd.DataFrame(kpis["kpi3"])

    # --- Chart 1: Revenue by Category (horizontal bar) ---
    fig1 = go.Figure(go.Bar(
        y=k1["product_category"],
        x=k1["total_revenue"],
        orientation="h",
        marker=dict(
            color=k1["revenue_pct"],
            colorscale=[[0,"#1E2D4A"],[0.5,"#F97316"],[1,"#FBBF24"]],
            showscale=True,
            colorbar=dict(title="% Share", thickness=12, tickfont=dict(size=10)),
        ),
        text=k1["revenue_pct"].apply(lambda v: f"{v}%"),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>Revenue: $%{x:,.0f}<extra></extra>",
    ))
    fig1.update_layout(**_layout(
        title=dict(text="Revenue by Product Category", font=dict(size=15, color="#F1F5F9")),
        xaxis=dict(title="Revenue (USD)", gridcolor=_GRID, zeroline=False),
        yaxis=dict(autorange="reversed", gridcolor=_GRID),
        height=400,
    ))

    # --- Chart 2: Revenue Share Donut ---
    fig2 = go.Figure(go.Pie(
        labels=k1["product_category"],
        values=k1["total_revenue"],
        hole=0.55,
        marker=dict(colors=_PALETTE, line=dict(color=_BG, width=2)),
        textinfo="percent",
        hovertemplate="<b>%{label}</b><br>$%{value:,.0f}<br>%{percent}<extra></extra>",
    ))
    fig2.update_layout(**_layout(
        title=dict(text="Revenue Share", font=dict(size=15, color="#F1F5F9")),
        showlegend=True,
        legend=dict(orientation="v", x=1, y=0.5, font=dict(size=11)),
        height=400,
    ))
    fig2.add_annotation(
        text="Revenue<br>Split",
        x=0.5, y=0.5,
        font=dict(size=13, color="#94A3B8"),
        showarrow=False,
    )

    # --- Chart 3: Monthly Revenue + MoM Growth (dual-axis) ---
    fig3 = go.Figure()
    colors_mom = k2["mom_growth_pct"].apply(
        lambda v: "#34D399" if (v and v >= 0) else "#FB7185"
    )
    fig3.add_trace(go.Bar(
        x=k2["month_label"], y=k2["total_revenue"],
        name="Monthly Revenue",
        marker=dict(color="#F97316", opacity=0.85,
                    line=dict(color="#FBBF24", width=0.5)),
        yaxis="y",
        hovertemplate="<b>%{x}</b><br>Revenue: $%{y:,.0f}<extra></extra>",
    ))
    fig3.add_trace(go.Scatter(
        x=k2["month_label"], y=k2["mom_growth_pct"],
        name="MoM Growth %",
        mode="lines+markers",
        line=dict(color="#818CF8", width=2.5, dash="solid"),
        marker=dict(size=6, color=colors_mom, line=dict(color="#818CF8", width=1)),
        yaxis="y2",
        hovertemplate="<b>%{x}</b><br>MoM: %{y:.2f}%<extra></extra>",
    ))
    fig3.add_hline(y=0, yref="y2", line=dict(color=_GRID, dash="dash", width=1))
    fig3.update_layout(**_layout(
        title=dict(text="Monthly Revenue & Month-over-Month Growth",
                   font=dict(size=15, color="#F1F5F9")),
        yaxis =dict(title="Revenue (USD)", gridcolor=_GRID, zeroline=False),
        yaxis2=dict(title="MoM Growth %", overlaying="y", side="right",
                    zeroline=True, zerolinecolor=_GRID, gridcolor="rgba(0,0,0,0)"),
        xaxis =dict(tickangle=-45, gridcolor=_GRID),
        legend=dict(orientation="h", x=0, y=1.08),
        barmode="overlay",
        height=430,
    ))

    # --- Chart 4: Top Regions (grouped bar) ---
    fig4 = go.Figure()
    fig4.add_trace(go.Bar(
        name="Transactions",
        x=k3["region"], y=k3["transaction_count"],
        marker_color="#38BDF8",
        yaxis="y",
        hovertemplate="<b>%{x}</b><br>%{y:,} transactions<extra></extra>",
    ))
    fig4.add_trace(go.Bar(
        name="Revenue ($)",
        x=k3["region"], y=k3["total_revenue"],
        marker_color="#F97316",
        yaxis="y2",
        hovertemplate="<b>%{x}</b><br>$%{y:,.0f}<extra></extra>",
    ))
    fig4.update_layout(**_layout(
        title=dict(text="Top 5 Regions — Volume vs Revenue",
                   font=dict(size=15, color="#F1F5F9")),
        yaxis =dict(title="Transactions", gridcolor=_GRID, zeroline=False),
        yaxis2=dict(title="Revenue (USD)", overlaying="y", side="right",
                    gridcolor="rgba(0,0,0,0)"),
        xaxis =dict(gridcolor=_GRID),
        barmode="group",
        legend=dict(orientation="h", x=0, y=1.08),
        height=400,
    ))

    # --- Chart 5: Bubble — Revenue vs Volume vs Customers ---
    fig5 = go.Figure(go.Scatter(
        x=k3["transaction_count"],
        y=k3["total_revenue"],
        mode="markers+text",
        text=k3["region"],
        textposition="top center",
        marker=dict(
            size=k3["unique_customers"] / k3["unique_customers"].max() * 60 + 20,
            color=_PALETTE[:len(k3)],
            opacity=0.85,
            line=dict(color="#F1F5F9", width=1),
        ),
        customdata=k3[["unique_customers","volume_share_pct","revenue_share_pct"]].values,
        hovertemplate=(
            "<b>%{text}</b><br>"
            "Transactions: %{x:,}<br>"
            "Revenue: $%{y:,.0f}<br>"
            "Customers: %{customdata[0]:,}<br>"
            "Vol share: %{customdata[1]:.1f}%<extra></extra>"
        ),
    ))
    fig5.update_layout(**_layout(
        title=dict(text="Revenue vs Volume  (bubble = unique customers)",
                   font=dict(size=15, color="#F1F5F9")),
        xaxis=dict(title="Transaction Count", gridcolor=_GRID, zeroline=False),
        yaxis=dict(title="Revenue (USD)",     gridcolor=_GRID, zeroline=False),
        showlegend=False,
        height=400,
    ))

    def _j(fig):
        return json.loads(json.dumps(fig, cls=PlotlyJSONEncoder))

    return {
        "revenue_bar":     _j(fig1),
        "revenue_pie":     _j(fig2),
        "monthly_growth":  _j(fig3),
        "regions_bar":     _j(fig4),
        "regions_scatter": _j(fig5),
    }


# ---------------------------------------------------------------------------
# HTML  (premium dark dashboard — served as plain string, no Jinja2)
# ---------------------------------------------------------------------------

def _html() -> str:          # noqa: PLR0915
    return r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>E-Commerce Sales Pipeline</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
/* ===================== TOKENS ===================== */
:root{
  --bg:       #080C18;
  --bg2:      #0D1426;
  --bg3:      #111827;
  --border:   #1E2D4A;
  --border2:  #253350;
  --accent:   #F97316;
  --accent-g: linear-gradient(135deg,#F97316,#FBBF24);
  --indigo:   #818CF8;
  --green:    #34D399;
  --rose:     #FB7185;
  --sky:      #38BDF8;
  --amber:    #FBBF24;
  --text:     #F1F5F9;
  --muted:    #64748B;
  --muted2:   #94A3B8;
  --radius:   14px;
  --radius-sm:8px;
  --shadow:   0 4px 24px rgba(0,0,0,.45);
  --glow:     0 0 24px rgba(249,115,22,.18);
}
/* ===================== RESET ===================== */
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%}
body{
  font-family:Inter,system-ui,sans-serif;
  background:var(--bg);
  color:var(--text);
  font-size:14px;
  line-height:1.55;
  overflow-x:hidden;
}
/* ===================== SCROLLBAR ===================== */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:99px}
/* ===================== LAYOUT ===================== */
.layout{display:flex;min-height:100vh}
/* ---- SIDEBAR ---- */
.sidebar{
  width:300px;min-width:300px;
  background:linear-gradient(180deg,#0B1120 0%,#080C18 100%);
  border-right:1px solid var(--border);
  display:flex;flex-direction:column;
  padding:1.5rem 1rem 1rem;
  position:sticky;top:0;height:100vh;overflow-y:auto;
  gap:.35rem;
}
.logo-wrap{
  display:flex;align-items:center;gap:10px;
  padding:.6rem .75rem;
  background:linear-gradient(135deg,rgba(249,115,22,.12),rgba(251,191,36,.06));
  border:1px solid rgba(249,115,22,.25);
  border-radius:var(--radius);
  margin-bottom:.75rem;
}
.logo-icon{
  width:38px;height:38px;border-radius:10px;
  background:var(--accent-g);
  display:flex;align-items:center;justify-content:center;
  font-size:1.1rem;color:#fff;flex-shrink:0;
  box-shadow:var(--glow);
}
.logo-text{line-height:1.2}
.logo-title{font-weight:800;font-size:.88rem;color:var(--text);letter-spacing:-.01em}
.logo-sub{font-size:.68rem;color:var(--muted2)}

/* section labels */
.sec{
  font-size:.65rem;font-weight:700;text-transform:uppercase;
  letter-spacing:1.8px;color:var(--muted);
  padding:.65rem .5rem .3rem;
  display:flex;align-items:center;gap:6px;
}
.sec::after{content:'';flex:1;height:1px;background:var(--border)}

/* source tabs */
.stabs{display:flex;gap:4px;margin-bottom:.65rem}
.stab{
  flex:1;text-align:center;padding:.38rem .2rem;
  border-radius:var(--radius-sm);font-size:.78rem;font-weight:500;
  cursor:pointer;border:1px solid var(--border);color:var(--muted2);
  background:transparent;transition:all .2s;
}
.stab:hover{border-color:var(--border2);color:var(--text)}
.stab.on{
  background:var(--accent-g);border-color:transparent;
  color:#fff;font-weight:600;box-shadow:var(--glow);
}
.spanel{display:none}.spanel.on{display:flex;flex-direction:column;gap:.55rem}

/* form controls */
.lbl{font-size:.75rem;font-weight:500;color:var(--muted2);display:flex;justify-content:space-between}
.lbl b{color:var(--accent)}
input[type=range]{
  width:100%;accent-color:var(--accent);
  height:4px;cursor:pointer;
}
.inp{
  width:100%;background:#0F1A2E;border:1px solid var(--border);
  color:var(--text);border-radius:var(--radius-sm);
  padding:.45rem .65rem;font-size:.82rem;font-family:inherit;
  transition:border .2s;outline:none;
}
.inp:focus{border-color:var(--accent);box-shadow:0 0 0 3px rgba(249,115,22,.12)}
select.inp{cursor:pointer}

/* drop zone */
.drop-zone{
  border:2px dashed var(--border);border-radius:var(--radius);
  padding:1.25rem .5rem;text-align:center;cursor:pointer;
  font-size:.8rem;color:var(--muted);
  transition:all .2s;
}
.drop-zone:hover{border-color:var(--accent);color:var(--accent);
  background:rgba(249,115,22,.04)}
.drop-zone i{font-size:1.5rem;display:block;margin-bottom:.4rem;color:var(--border2)}
.drop-zone:hover i{color:var(--accent)}

/* manual rows */
.mrow{display:grid;grid-template-columns:2fr 1fr 1.5fr 28px;gap:4px;align-items:center}
.mrow .inp{font-size:.75rem;padding:.3rem .45rem}
.btn-del{
  background:rgba(251,113,133,.1);border:1px solid rgba(251,113,133,.25);
  color:var(--rose);border-radius:6px;cursor:pointer;
  font-size:.78rem;padding:3px 6px;transition:all .15s;
}
.btn-del:hover{background:rgba(251,113,133,.2)}
.btn-add{
  width:100%;background:#0F1A2E;border:1px solid var(--border);
  color:var(--muted2);border-radius:var(--radius-sm);
  padding:.4rem;font-size:.78rem;cursor:pointer;
  transition:all .2s;font-family:inherit;
}
.btn-add:hover{border-color:var(--accent);color:var(--accent)}

/* pipeline steps */
.pstep{
  display:flex;align-items:center;gap:9px;
  font-size:.76rem;color:var(--muted);
  padding:.25rem .5rem;border-radius:6px;
  transition:all .25s;
}
.pstep.done{color:var(--green)}
.pstep.active{color:var(--accent);background:rgba(249,115,22,.07)}
.dot{
  width:8px;height:8px;border-radius:50%;
  background:var(--border);flex-shrink:0;
  transition:all .25s;
}
.pstep.done  .dot{background:var(--green);box-shadow:0 0 8px rgba(52,211,153,.5)}
.pstep.active .dot{background:var(--accent);animation:pulse .9s infinite;
  box-shadow:0 0 8px rgba(249,115,22,.5)}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.5;transform:scale(.8)}}

/* RUN button */
.btn-run{
  width:100%;padding:.8rem;border:none;border-radius:var(--radius);
  background:var(--accent-g);color:#fff;font-weight:700;
  font-size:.95rem;cursor:pointer;font-family:inherit;
  margin-top:.5rem;
  box-shadow:var(--glow);
  transition:opacity .2s,transform .15s;
  display:flex;align-items:center;justify-content:center;gap:8px;
}
.btn-run:hover:not(:disabled){opacity:.92;transform:translateY(-1px)}
.btn-run:active:not(:disabled){transform:translateY(0)}
.btn-run:disabled{opacity:.45;cursor:not-allowed;transform:none}

/* ---- MAIN ---- */
.main{flex:1;padding:1.75rem 2rem;overflow:auto}

/* hero */
.hero{margin-bottom:1.75rem}
.hero-badge{
  display:inline-flex;align-items:center;gap:6px;
  background:rgba(249,115,22,.1);border:1px solid rgba(249,115,22,.3);
  color:var(--accent);font-size:.72rem;font-weight:600;
  padding:.25rem .7rem;border-radius:99px;margin-bottom:.75rem;
  letter-spacing:.5px;text-transform:uppercase;
}
.hero-badge i{font-size:.8rem}
h1{
  font-size:2rem;font-weight:800;letter-spacing:-.03em;
  line-height:1.1;margin-bottom:.35rem;
}
h1 .hi{
  background:var(--accent-g);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
}
.subtitle{color:var(--muted2);font-size:.88rem}

/* error */
.err-box{
  background:rgba(251,113,133,.1);border:1px solid rgba(251,113,133,.3);
  color:#FDA4AF;border-radius:var(--radius);padding:1rem 1.25rem;
  margin-bottom:1.25rem;display:none;
  display:flex;align-items:center;gap:10px;
}
.err-box i{font-size:1.1rem;flex-shrink:0}

/* spinner */
.spin-wrap{display:none;padding:3rem 0;text-align:center}
.spin-ring{
  width:52px;height:52px;border-radius:50%;
  border:4px solid var(--border);
  border-top-color:var(--accent);
  animation:spin 1s linear infinite;
  margin:0 auto 1.25rem;
}
@keyframes spin{to{transform:rotate(360deg)}}
.spin-msg{color:var(--muted2);font-size:.88rem}

/* progress bar */
.prog-wrap{margin:.75rem auto 0;max-width:320px}
.prog-track{background:var(--border);border-radius:99px;height:5px;overflow:hidden}
.prog-bar{
  height:100%;border-radius:99px;
  background:var(--accent-g);width:0%;
  transition:width .4s ease;
}
.prog-label{font-size:.72rem;color:var(--muted);margin-top:.4rem}

/* results */
.results{display:none}

/* KPI cards */
.kpi-grid{
  display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;
  margin-bottom:1.25rem;
}
.kpi-card{
  background:linear-gradient(135deg,var(--bg2),var(--bg3));
  border:1px solid var(--border);border-radius:var(--radius);
  padding:1.1rem 1.25rem;
  position:relative;overflow:hidden;
  transition:border-color .2s,box-shadow .2s;
}
.kpi-card:hover{border-color:var(--border2);box-shadow:var(--shadow)}
.kpi-card::before{
  content:'';position:absolute;inset:0;
  background:linear-gradient(135deg,transparent 60%,rgba(249,115,22,.04));
  pointer-events:none;
}
.kpi-icon{
  width:34px;height:34px;border-radius:9px;
  display:flex;align-items:center;justify-content:center;
  font-size:1rem;margin-bottom:.65rem;
}
.kpi-lbl{font-size:.68rem;font-weight:600;text-transform:uppercase;
  letter-spacing:1px;color:var(--muted)}
.kpi-val{
  font-size:1.65rem;font-weight:800;letter-spacing:-.02em;
  margin:.2rem 0 .15rem;color:var(--text);
}
.kpi-sub{font-size:.73rem;color:var(--muted)}
.kpi-trend{
  position:absolute;top:1.1rem;right:1.1rem;
  font-size:.72rem;font-weight:600;
  display:flex;align-items:center;gap:3px;
}

/* cleaning pipeline badges */
.etl-strip{
  display:flex;align-items:center;flex-wrap:wrap;gap:6px;
  margin-bottom:1.25rem;padding:.85rem 1rem;
  background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);
}
.etl-chip{
  display:flex;align-items:center;gap:5px;
  background:#0F1A2E;border:1px solid var(--border);border-radius:6px;
  padding:.22rem .6rem;font-size:.72rem;color:var(--muted2);
}
.etl-chip b{color:var(--text)}
.etl-arr{color:var(--border2);font-size:.8rem}
.etl-meta{
  margin-left:auto;font-size:.72rem;color:var(--muted);
  display:flex;align-items:center;gap:12px;
}
.etl-meta span{display:flex;align-items:center;gap:4px}

/* chart section */
.ctabs{display:flex;gap:3px;border-bottom:1px solid var(--border);margin-bottom:0}
.ctab{
  display:flex;align-items:center;gap:6px;
  padding:.55rem 1.1rem;font-size:.82rem;font-weight:500;
  color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;
  transition:all .15s;border-radius:var(--radius-sm) var(--radius-sm) 0 0;
}
.ctab:hover{color:var(--text);background:rgba(255,255,255,.02)}
.ctab.on{color:var(--accent);border-bottom-color:var(--accent);background:rgba(249,115,22,.05)}
.ctab i{font-size:.95rem}

.cpanel{
  display:none;
  background:var(--bg2);border:1px solid var(--border);
  border-top:none;border-radius:0 0 var(--radius) var(--radius);
  padding:1.25rem;
}
.cpanel.on{display:block}

/* chart grid */
.ch-grid-2{display:grid;grid-template-columns:3fr 2fr;gap:1rem}
.ch-full{width:100%}

/* table */
.tbl-wrap{overflow-x:auto;margin-top:1.1rem;border-radius:var(--radius-sm);
  border:1px solid var(--border)}
table.gt{width:100%;border-collapse:collapse;font-size:.78rem}
table.gt thead th{
  background:#0B1120;color:var(--muted);font-size:.67rem;font-weight:700;
  text-transform:uppercase;letter-spacing:.8px;
  padding:.6rem .85rem;border-bottom:1px solid var(--border);
  white-space:nowrap;
}
table.gt tbody td{
  padding:.5rem .85rem;border-bottom:1px solid rgba(30,45,74,.5);
  color:var(--text);
}
table.gt tbody tr:last-child td{border-bottom:none}
table.gt tbody tr:hover td{background:rgba(249,115,22,.04)}

/* pills */
.pill{
  display:inline-flex;align-items:center;gap:4px;
  padding:.18rem .55rem;border-radius:99px;font-size:.7rem;font-weight:600;
}
.pill-orange{background:rgba(249,115,22,.15);color:var(--accent)}
.pill-green {background:rgba(52,211,153,.15);color:var(--green)}
.pill-rose  {background:rgba(251,113,133,.15);color:var(--rose)}
.pill-blue  {background:rgba(56,189,248,.15); color:var(--sky)}
.pill-indigo{background:rgba(129,140,248,.15);color:var(--indigo)}

/* download btn */
.dl-btn{
  display:inline-flex;align-items:center;gap:5px;
  background:#0F1A2E;border:1px solid var(--border);
  color:var(--muted2);border-radius:var(--radius-sm);
  padding:.32rem .75rem;font-size:.75rem;cursor:pointer;
  transition:all .2s;font-family:inherit;margin-bottom:.65rem;
}
.dl-btn:hover{border-color:var(--accent);color:var(--accent)}

/* responsive */
@media(max-width:900px){
  .sidebar{width:100%;height:auto;position:relative;min-width:unset}
  .layout{flex-direction:column}
  .kpi-grid{grid-template-columns:repeat(2,1fr)}
  .ch-grid-2{grid-template-columns:1fr}
  .main{padding:1rem}
}
@media(max-width:520px){
  .kpi-grid{grid-template-columns:1fr 1fr}
  h1{font-size:1.5rem}
  .ctab span{display:none}
}
</style>
</head>
<body>
<div class="layout">

<!-- ==================== SIDEBAR ==================== -->
<aside class="sidebar">
  <div class="logo-wrap">
    <div class="logo-icon"><i class="bi bi-bar-chart-fill"></i></div>
    <div class="logo-text">
      <div class="logo-title">Sales Pipeline</div>
      <div class="logo-sub">AWS Glue &amp; Redshift Sim</div>
    </div>
  </div>

  <div class="sec"><i class="bi bi-database"></i> Data Source</div>

  <div class="stabs">
    <button class="stab on" onclick="setSrc('gen',this)"><i class="bi bi-cpu"></i> Generate</button>
    <button class="stab"    onclick="setSrc('up',this)"><i class="bi bi-cloud-upload"></i> Upload</button>
    <button class="stab"    onclick="setSrc('man',this)"><i class="bi bi-pencil-square"></i> Manual</button>
  </div>

  <!-- Generate -->
  <div class="spanel on" id="sp-gen">
    <div>
      <div class="lbl">Records &nbsp;<b id="recLbl">10,000</b></div>
      <input type="range" id="recSlider" min="10000" max="1200000" step="10000" value="10000"
        oninput="document.getElementById('recLbl').textContent=Number(this.value).toLocaleString()">
      <div style="display:flex;justify-content:space-between;font-size:.65rem;color:var(--muted);margin-top:2px">
        <span>10 K</span><span>600 K</span><span>1.2 M</span>
      </div>
    </div>
    <div>
      <div class="lbl">Random Seed</div>
      <input class="inp" type="number" id="seedInp" value="42" min="0">
    </div>
  </div>

  <!-- Upload -->
  <div class="spanel" id="sp-up">
    <div class="drop-zone" onclick="document.getElementById('csvFile').click()">
      <i class="bi bi-cloud-arrow-up"></i>
      Click or drag to upload CSV<br>
      <span style="font-size:.68rem;margin-top:4px;display:block">
        transaction_id · customer_id · product_category<br>
        amount · timestamp · region
      </span>
    </div>
    <input type="file" id="csvFile" accept=".csv" style="display:none" onchange="onFile(this)">
    <div id="fileInfo" style="font-size:.73rem;color:var(--muted2)"></div>
  </div>

  <!-- Manual -->
  <div class="spanel" id="sp-man">
    <div style="font-size:.72rem;color:var(--muted);
      display:grid;grid-template-columns:2fr 1fr 1.5fr 28px;gap:4px;
      padding:0 0 4px;border-bottom:1px solid var(--border)">
      <span>Category</span><span>Amount</span><span>Region</span><span></span>
    </div>
    <div id="manRows"></div>
    <button class="btn-add" onclick="addRow()"><i class="bi bi-plus-circle"></i> Add Row</button>
  </div>

  <div class="sec" style="margin-top:.25rem"><i class="bi bi-activity"></i> Pipeline</div>

  <div id="ps1" class="pstep"><div class="dot"></div> Load &amp; Validate Data</div>
  <div id="ps2" class="pstep"><div class="dot"></div> Clean &amp; Deduplicate</div>
  <div id="ps3" class="pstep"><div class="dot"></div> Compute KPIs (3 metrics)</div>
  <div id="ps4" class="pstep"><div class="dot"></div> Render 5 Interactive Charts</div>

  <button class="btn-run" id="runBtn" onclick="runPipeline()">
    <i class="bi bi-play-fill"></i> Run Pipeline
  </button>
</aside>

<!-- ==================== MAIN ==================== -->
<main class="main">

  <!-- Hero -->
  <div class="hero">
    <div class="hero-badge"><i class="bi bi-lightning-charge-fill"></i> 1 M+ Record ETL</div>
    <h1>E-Commerce Sales <span class="hi">Dashboard</span></h1>
    <p class="subtitle">
      End-to-end ETL simulation &nbsp;&bull;&nbsp; AWS Glue + Redshift architecture &nbsp;&bull;&nbsp;
      3 KPIs &nbsp;&bull;&nbsp; 5 interactive charts
    </p>
  </div>

  <!-- Error -->
  <div class="err-box" id="errBox" style="display:none">
    <i class="bi bi-exclamation-triangle-fill"></i>
    <span id="errMsg">Something went wrong.</span>
  </div>

  <!-- Spinner -->
  <div class="spin-wrap" id="spinWrap">
    <div class="spin-ring"></div>
    <div class="spin-msg" id="spinMsg">Initialising pipeline&hellip;</div>
    <div class="prog-wrap">
      <div class="prog-track"><div class="prog-bar" id="progBar"></div></div>
      <div class="prog-label" id="progLabel">0%</div>
    </div>
  </div>

  <!-- Results -->
  <div class="results" id="resultsWrap">

    <!-- KPI Cards -->
    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-icon" style="background:rgba(249,115,22,.15);color:var(--accent)"><i class="bi bi-receipt"></i></div>
        <div class="kpi-lbl">Total Records</div>
        <div class="kpi-val" id="cRec">—</div>
        <div class="kpi-sub">after ETL cleaning</div>
        <div class="kpi-trend" id="cRecBadge"></div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background:rgba(52,211,153,.15);color:var(--green)"><i class="bi bi-currency-dollar"></i></div>
        <div class="kpi-lbl">Total Revenue</div>
        <div class="kpi-val" id="cRev">—</div>
        <div class="kpi-sub">USD · all categories</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background:rgba(129,140,248,.15);color:var(--indigo)"><i class="bi bi-graph-up"></i></div>
        <div class="kpi-lbl">Avg Transaction</div>
        <div class="kpi-val" id="cAvg">—</div>
        <div class="kpi-sub">per order</div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background:rgba(56,189,248,.15);color:var(--sky)"><i class="bi bi-people-fill"></i></div>
        <div class="kpi-lbl">Unique Customers</div>
        <div class="kpi-val" id="cCust">—</div>
        <div class="kpi-sub">distinct IDs</div>
      </div>
    </div>

    <!-- ETL Strip -->
    <div class="etl-strip" id="etlStrip"></div>

    <!-- Chart Tabs -->
    <div class="ctabs">
      <div class="ctab on"  onclick="switchTab(0,this)"><i class="bi bi-bar-chart-fill"></i><span> Revenue / Category</span></div>
      <div class="ctab"     onclick="switchTab(1,this)"><i class="bi bi-graph-up-arrow"></i><span> Monthly Growth</span></div>
      <div class="ctab"     onclick="switchTab(2,this)"><i class="bi bi-globe2"></i><span> Top Regions</span></div>
      <div class="ctab"     onclick="switchTab(3,this)"><i class="bi bi-table"></i><span> Raw Data</span></div>
    </div>

    <!-- Tab 0 -->
    <div class="cpanel on" id="tp0">
      <button class="dl-btn" onclick="dlCSV('kpi1','revenue_by_category.csv')"><i class="bi bi-download"></i> Download CSV</button>
      <div class="ch-grid-2">
        <div id="chRevBar"></div>
        <div id="chRevPie"></div>
      </div>
      <div class="tbl-wrap"><table class="gt" id="t1"></table></div>
    </div>

    <!-- Tab 1 -->
    <div class="cpanel" id="tp1">
      <button class="dl-btn" onclick="dlCSV('kpi2','monthly_growth.csv')"><i class="bi bi-download"></i> Download CSV</button>
      <div id="chMonthly"></div>
      <div class="tbl-wrap"><table class="gt" id="t2"></table></div>
    </div>

    <!-- Tab 2 -->
    <div class="cpanel" id="tp2">
      <button class="dl-btn" onclick="dlCSV('kpi3','top_regions.csv')"><i class="bi bi-download"></i> Download CSV</button>
      <div class="ch-grid-2">
        <div id="chRegBar"></div>
        <div id="chRegScatter"></div>
      </div>
      <div class="tbl-wrap"><table class="gt" id="t3"></table></div>
    </div>

    <!-- Tab 3 -->
    <div class="cpanel" id="tp3">
      <button class="dl-btn" onclick="dlCSV('raw','raw_preview.csv')"><i class="bi bi-download"></i> Download CSV</button>
      <p style="font-size:.76rem;color:var(--muted);margin-bottom:.65rem">
        <i class="bi bi-info-circle"></i> Showing first 100 cleaned rows
      </p>
      <div class="tbl-wrap"><table class="gt" id="t4"></table></div>
    </div>

  </div><!-- /results -->
</main>
</div><!-- /layout -->

<script>
// ===================== STATE =====================
let src       = 'gen';
let csvText   = null;
let manRows   = [];
let lastKPIs  = {};
let lastRaw   = [];

const CATS = ["Electronics","Clothing","Books","Home & Garden","Sports",
              "Toys","Automotive","Food & Beverage","Health & Beauty","Office Supplies"];
const REGS = ["us-east-1","us-west-2","eu-west-1",
              "ap-southeast-1","ap-northeast-1","sa-east-1","ca-central-1"];

// ===================== SOURCE TABS =====================
function setSrc(id, btn) {
  src = id;
  document.querySelectorAll('.stab').forEach(b => b.classList.remove('on'));
  document.querySelectorAll('.spanel').forEach(p => p.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById('sp-' + id).classList.add('on');
}

// ===================== FILE UPLOAD =====================
function onFile(inp) {
  const f = inp.files[0]; if (!f) return;
  const r = new FileReader();
  r.onload = e => {
    csvText = e.target.result;
    document.getElementById('fileInfo').innerHTML =
      '<i class="bi bi-check-circle-fill" style="color:var(--green)"></i> ' +
      f.name + ' &nbsp;(' + (f.size/1024).toFixed(1) + ' KB)';
  };
  r.readAsText(f);
}

// ===================== MANUAL ROWS =====================
function addRow() {
  manRows.push({
    transaction_id:   'TXN-' + Math.random().toString(36).slice(2,11).toUpperCase(),
    customer_id:      'CUST-000001',
    product_category: 'Electronics',
    amount:           '',
    timestamp:        '2024-06-15 10:00:00',
    region:           'us-east-1',
  });
  renderRows();
}

function delRow(i) { manRows.splice(i, 1); renderRows(); }

function renderRows() {
  const c = document.getElementById('manRows');
  if (!manRows.length) {
    c.innerHTML = '<div style="color:var(--muted);font-size:.76rem;text-align:center;padding:.6rem">Click Add Row to enter records</div>';
    return;
  }
  c.innerHTML = manRows.map((r, i) => `
    <div class="mrow" style="margin-bottom:5px">
      <select class="inp" onchange="manRows[${i}].product_category=this.value">
        ${CATS.map(c=>`<option value="${c}"${c===r.product_category?' selected':''}>${c}</option>`).join('')}
      </select>
      <input class="inp" type="number" placeholder="$" value="${r.amount}"
        oninput="manRows[${i}].amount=this.value">
      <select class="inp" onchange="manRows[${i}].region=this.value">
        ${REGS.map(rg=>`<option value="${rg}"${rg===r.region?' selected':''}>${rg}</option>`).join('')}
      </select>
      <button class="btn-del" onclick="delRow(${i})"><i class="bi bi-x"></i></button>
    </div>`).join('');
}

// ===================== PIPELINE STEPS =====================
function setStep(n) {
  ['ps1','ps2','ps3','ps4'].forEach((id, i) => {
    const el = document.getElementById(id);
    el.className = 'pstep' + (i < n ? ' done' : i === n ? ' active' : '');
  });
}

// ===================== FAKE PROGRESS BAR =====================
let progTimer = null;
function startProgress(durationMs) {
  clearInterval(progTimer);
  const bar   = document.getElementById('progBar');
  const label = document.getElementById('progLabel');
  const msgs  = [
    'Loading & validating data…',
    'Cleaning & deduplicating…',
    'Computing revenue KPIs…',
    'Computing growth metrics…',
    'Building interactive charts…',
    'Almost there…',
  ];
  let pct = 0, msgIdx = 0;
  const step = 100 / (durationMs / 120);
  progTimer = setInterval(() => {
    pct = Math.min(pct + step * (0.5 + Math.random()), 92);
    bar.style.width = pct + '%';
    label.textContent = Math.round(pct) + '%';
    const newIdx = Math.min(Math.floor(pct / 17), msgs.length - 1);
    if (newIdx !== msgIdx) {
      msgIdx = newIdx;
      document.getElementById('spinMsg').textContent = msgs[msgIdx];
      setStep(msgIdx < 2 ? 0 : msgIdx < 3 ? 1 : msgIdx < 4 ? 2 : 3);
    }
  }, 120);
}
function finishProgress() {
  clearInterval(progTimer);
  document.getElementById('progBar').style.width = '100%';
  document.getElementById('progLabel').textContent = '100%';
}

// ===================== FORMAT HELPERS =====================
const fN   = n => n == null ? '—' : Number(n).toLocaleString(undefined, {maximumFractionDigits:0});
const fUSD = n => n == null ? '—' : '$' + Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const fPct = n => n == null ? '—' : (Number(n) >= 0 ? '+' : '') + Number(n).toFixed(2) + '%';
const fRaw = (v, fmt) => fmt==='usd'?fUSD(v):fmt==='num'?fN(v):fmt==='pct'?fPct(v):v??'—';

function pctPill(v) {
  if (v == null) return '<span style="color:var(--muted)">—</span>';
  const cls = v >= 0 ? 'pill-green' : 'pill-rose';
  const ico = v >= 0 ? 'bi-arrow-up-right' : 'bi-arrow-down-right';
  return `<span class="pill ${cls}"><i class="bi ${ico}"></i>${Math.abs(v).toFixed(2)}%</span>`;
}

// animated counter
function animateVal(el, end, formatter, duration=800) {
  const start = 0, startTime = performance.now();
  const step = ts => {
    const p = Math.min((ts - startTime) / duration, 1);
    const ease = 1 - Math.pow(1 - p, 3);
    el.textContent = formatter(start + (end - start) * ease);
    if (p < 1) requestAnimationFrame(step);
    else el.textContent = formatter(end);
  };
  requestAnimationFrame(step);
}

// ===================== BUILD TABLE =====================
function buildTable(el, rows, cols) {
  if (!rows?.length) { el.innerHTML = ''; return; }
  el.innerHTML =
    '<thead><tr>' + cols.map(c => `<th>${c.label||c.key}</th>`).join('') + '</tr></thead>' +
    '<tbody>' + rows.map(r =>
      '<tr>' + cols.map(c => {
        const v   = r[c.key];
        let cell  = fRaw(v, c.fmt);
        if (c.fmt === 'pct' && c.key === 'mom_growth_pct') cell = pctPill(v);
        if (c.key === 'rank') cell = `<span class="pill pill-orange">#${v}</span>`;
        return `<td>${cell}</td>`;
      }).join('') + '</tr>'
    ).join('') + '</tbody>';
}

// ===================== TAB SWITCH =====================
function switchTab(idx, btn) {
  document.querySelectorAll('.ctab').forEach(t => t.classList.remove('on'));
  document.querySelectorAll('.cpanel').forEach(p => p.classList.remove('on'));
  btn.classList.add('on');
  document.getElementById('tp' + idx).classList.add('on');
  window.dispatchEvent(new Event('resize'));
}

// ===================== CSV DOWNLOAD =====================
function dlCSV(key, filename) {
  let rows;
  if (key === 'raw')  rows = lastRaw;
  else if (key === 'kpi1') rows = lastKPIs.kpi1;
  else if (key === 'kpi2') rows = lastKPIs.kpi2;
  else if (key === 'kpi3') rows = lastKPIs.kpi3;
  if (!rows?.length) return;
  const cols = Object.keys(rows[0]);
  const csv  = [cols.join(','), ...rows.map(r => cols.map(c => JSON.stringify(r[c]??'')).join(','))].join('\n');
  const a    = document.createElement('a');
  a.href     = 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv);
  a.download = filename; a.click();
}

// ===================== RUN PIPELINE =====================
async function runPipeline() {
  const btn     = document.getElementById('runBtn');
  const errBox  = document.getElementById('errBox');
  const spinWrap= document.getElementById('spinWrap');
  const results = document.getElementById('resultsWrap');

  // hide error / previous results
  errBox.style.display  = 'none';
  results.style.display = 'none';
  spinWrap.style.display= 'block';
  btn.disabled = true;
  setStep(-1);

  // estimate duration for fake progress
  const nRec  = src === 'gen' ? parseInt(document.getElementById('recSlider').value) : 50000;
  const estMs = Math.max(4000, nRec * 0.012);
  startProgress(estMs);
  document.getElementById('spinMsg').textContent = 'Initialising pipeline…';

  // build request body
  let body;
  if (src === 'gen') {
    body = { source:'generate',
             records: parseInt(document.getElementById('recSlider').value),
             seed:    parseInt(document.getElementById('seedInp').value) };
  } else if (src === 'up') {
    if (!csvText) { showErr('Please upload a CSV file first.'); return; }
    body = { source:'upload', csv: csvText };
  } else {
    if (!manRows.length) { showErr('Please add at least one row.'); return; }
    body = { source:'manual', rows: manRows };
  }

  try {
    const resp = await fetch('/api/run', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body),
    });
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'Server error ' + resp.status);
    finishProgress();
    setStep(4);
    await new Promise(r => setTimeout(r, 300));  // brief pause for UX
    spinWrap.style.display = 'none';
    renderResults(data);
    results.style.display = 'block';
    results.scrollIntoView({behavior:'smooth', block:'start'});
  } catch(e) {
    showErr(e.message);
  } finally {
    btn.disabled = false;
    clearInterval(progTimer);
  }
}

function showErr(msg) {
  document.getElementById('errMsg').textContent = msg;
  document.getElementById('errBox').style.display = 'flex';
  document.getElementById('spinWrap').style.display = 'none';
  document.getElementById('runBtn').disabled = false;
  clearInterval(progTimer);
}

// ===================== RENDER RESULTS =====================
function renderResults(data) {
  const s = data.summary;
  lastKPIs= data.kpis;
  lastRaw = data.raw_preview;

  // KPI animated counters
  animateVal(document.getElementById('cRec'),  s.total_records,    fN);
  animateVal(document.getElementById('cRev'),  s.total_revenue,    fUSD);
  animateVal(document.getElementById('cAvg'),  s.avg_transaction,  fUSD);
  animateVal(document.getElementById('cCust'), s.unique_customers, fN);

  // retained % badge
  const st = data.stats;
  const retained = (st.final / st.raw * 100).toFixed(1);
  document.getElementById('cRecBadge').innerHTML =
    `<span class="pill pill-green"><i class="bi bi-funnel-fill"></i>${retained}% kept</span>`;

  // ETL strip
  const etlSteps = [
    ['Raw Input',   st.raw,              'pill-blue'],
    ['After PK',    st.after_pk_null_drop,'pill-blue'],
    ['After Dedup', st.after_dedup,       'pill-blue'],
    ['After Amount',st.after_amount,      'pill-blue'],
    ['After TS',    st.after_ts,          'pill-blue'],
    ['Cleaned',     st.final,             'pill-green'],
  ];
  document.getElementById('etlStrip').innerHTML =
    etlSteps.map((step, i) =>
      (i > 0 ? '<span class="etl-arr"><i class="bi bi-chevron-right"></i></span>' : '') +
      `<span class="etl-chip"><span class="pill ${step[2]}">${step[0]}</span><b>${fN(step[1])}</b></span>`
    ).join('') +
    `<span class="etl-meta">
       <span><i class="bi bi-clock"></i>${st.etl_secs}s ETL</span>
       <span><i class="bi bi-shield-check"></i>${st.retained_pct}% retained</span>
     </span>`;

  // Charts
  const cfg = {responsive:true};
  const C = data.charts;
  Plotly.react('chRevBar',     C.revenue_bar.data,     C.revenue_bar.layout,     cfg);
  Plotly.react('chRevPie',     C.revenue_pie.data,     C.revenue_pie.layout,     cfg);
  Plotly.react('chMonthly',    C.monthly_growth.data,  C.monthly_growth.layout,  cfg);
  Plotly.react('chRegBar',     C.regions_bar.data,     C.regions_bar.layout,     cfg);
  Plotly.react('chRegScatter', C.regions_scatter.data, C.regions_scatter.layout, cfg);

  // Tables
  buildTable(document.getElementById('t1'), data.kpis.kpi1, [
    {key:'product_category',     label:'Category'},
    {key:'total_revenue',        label:'Revenue',       fmt:'usd'},
    {key:'transaction_count',    label:'Transactions',  fmt:'num'},
    {key:'revenue_pct',          label:'Rev %',         fmt:'pct'},
    {key:'avg_transaction_value',label:'Avg Order',     fmt:'usd'},
    {key:'unique_customers',     label:'Customers',     fmt:'num'},
  ]);
  buildTable(document.getElementById('t2'), data.kpis.kpi2, [
    {key:'month_label',          label:'Month'},
    {key:'total_revenue',        label:'Revenue',       fmt:'usd'},
    {key:'transaction_count',    label:'Transactions',  fmt:'num'},
    {key:'mom_growth_pct',       label:'MoM Growth',    fmt:'pct'},
    {key:'unique_customers',     label:'Customers',     fmt:'num'},
    {key:'avg_transaction_value',label:'Avg Order',     fmt:'usd'},
  ]);
  buildTable(document.getElementById('t3'), data.kpis.kpi3, [
    {key:'rank',               label:'Rank'},
    {key:'region',             label:'Region'},
    {key:'transaction_count',  label:'Transactions',  fmt:'num'},
    {key:'total_revenue',      label:'Revenue',       fmt:'usd'},
    {key:'volume_share_pct',   label:'Vol %',         fmt:'pct'},
    {key:'revenue_share_pct',  label:'Rev %',         fmt:'pct'},
    {key:'unique_customers',   label:'Customers',     fmt:'num'},
  ]);
  buildTable(document.getElementById('t4'), data.raw_preview, [
    {key:'transaction_id',   label:'Transaction ID'},
    {key:'customer_id',      label:'Customer'},
    {key:'product_category', label:'Category'},
    {key:'amount',           label:'Amount',    fmt:'usd'},
    {key:'timestamp',        label:'Timestamp'},
    {key:'region',           label:'Region'},
  ]);
}

// init
renderRows();
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Flask Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return Response(_html(), mimetype="text/html")


@app.route("/api/run", methods=["POST"])
def api_run():
    t_start = time.time()
    try:
        body   = request.get_json(force=True)
        source = body.get("source", "generate")

        if source == "generate":
            n    = min(int(body.get("records", 10_000)), MAX_RECORDS)
            seed = int(body.get("seed", 42))
            df   = generate_data(n, seed)

        elif source == "upload":
            csv_str = body.get("csv", "")
            df = pd.read_csv(io.StringIO(csv_str))
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if len(df) > MAX_RECORDS:
                df = df.head(MAX_RECORDS)

        elif source == "manual":
            rows = body.get("rows", [])
            if not rows:
                return jsonify({"error": "No rows provided."}), 400
            df = pd.DataFrame(rows)
            df["amount"]    = pd.to_numeric(df.get("amount",    None), errors="coerce")
            df["timestamp"] = pd.to_datetime(df.get("timestamp", None), errors="coerce")
            df["timestamp"] = df["timestamp"].fillna(pd.Timestamp.now())
            if "transaction_id" not in df.columns:
                df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        else:
            return jsonify({"error": "Unknown source."}), 400

        # Ensure all required columns exist
        for col in ["transaction_id", "customer_id", "product_category",
                    "amount", "timestamp", "region"]:
            if col not in df.columns:
                df[col] = None

        df_clean, stats = run_etl(df)
        if len(df_clean) == 0:
            return jsonify({"error": "No records remained after cleaning. Check your data."}), 400

        kpis   = compute_kpis(df_clean)
        charts = build_charts(kpis)

        summary = {
            "total_records":    int(len(df_clean)),
            "total_revenue":    round(float(df_clean["amount"].sum()), 2),
            "avg_transaction":  round(float(df_clean["amount"].mean()), 2),
            "unique_customers": int(df_clean["customer_id"].nunique()),
            "pipeline_secs":    round(time.time() - t_start, 2),
        }

        preview = df_clean.head(100).copy()
        preview["timestamp"] = preview["timestamp"].astype(str)
        preview["amount"]    = preview["amount"].round(2)

        return jsonify({
            "stats":       stats,
            "summary":     summary,
            "kpis":        kpis,
            "charts":      charts,
            "raw_preview": preview[
                ["transaction_id", "customer_id", "product_category",
                 "amount", "timestamp", "region"]
            ].to_dict("records"),
        })

    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ---------------------------------------------------------------------------
# Local dev
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
