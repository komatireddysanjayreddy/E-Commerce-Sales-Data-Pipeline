"""
api/index.py
============
Flask dashboard for the E-Commerce Sales ETL Pipeline.
Deployed as a Vercel Python serverless function.

Routes
------
GET  /          -> HTML dashboard (single-page app)
POST /api/run   -> Run full pipeline, return JSON (KPIs + charts)
"""

import io
import json
import uuid
import os
from datetime import datetime, timedelta

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
START_DATE  = datetime(2022, 1, 1)
END_DATE    = datetime(2024, 12, 31, 23, 59, 59)
DELTA_SECS  = int((END_DATE - START_DATE).total_seconds())
MAX_RECORDS = 50_000   # Vercel Hobby timeout guard


# ---------------------------------------------------------------------------
# Data Generation  (mirrors data_generation/generate_sales_data.py)
# ---------------------------------------------------------------------------

def generate_data(num_records: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    n   = min(num_records, MAX_RECORDS)

    transaction_ids = [str(uuid.uuid4()) for _ in range(n)]
    customer_ids    = [f"CUST_{c:06d}" for c in rng.integers(1, 100_001, n)]
    categories      = rng.choice(PRODUCT_CATEGORIES, n, p=CATEGORY_WEIGHTS)
    regions         = rng.choice(REGIONS,             n, p=REGION_WEIGHTS)
    amounts         = np.clip(np.round(rng.lognormal(4.0, 1.2, n), 2), AMOUNT_MIN, AMOUNT_MAX)
    timestamps      = [START_DATE + timedelta(seconds=int(s))
                       for s in rng.integers(0, DELTA_SECS, n)]

    df = pd.DataFrame({
        "transaction_id":   transaction_ids,
        "customer_id":      customer_ids,
        "product_category": categories,
        "amount":           amounts,
        "timestamp":        timestamps,
        "region":           regions,
    })

    # Inject ~1 % nulls and ~0.5 % duplicates
    df.loc[rng.choice(n, int(n * 0.01), replace=False), "amount"]      = None
    df.loc[rng.choice(n, int(n * 0.01), replace=False), "customer_id"] = None
    dups = df.sample(int(n * 0.005), random_state=seed)
    return pd.concat([df, dups], ignore_index=True)


# ---------------------------------------------------------------------------
# ETL Cleaning  (mirrors glue_jobs/01_landing_to_cleansed.py)
# ---------------------------------------------------------------------------

def run_etl(df: pd.DataFrame):
    stats = {"raw": len(df)}

    if df["timestamp"].dtype == object:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    df = df[df["transaction_id"].notna()]
    stats["after_pk_null_drop"] = len(df)

    df = df.drop_duplicates(subset=["transaction_id"])
    stats["after_dedup"] = len(df)

    df = df[df["amount"].notna()
            & (df["amount"] >= AMOUNT_MIN)
            & (df["amount"] <= AMOUNT_MAX)]
    stats["after_amount"] = len(df)

    df = df[df["timestamp"].notna()]
    stats["after_ts"] = len(df)

    df = df.copy()
    df["product_category"] = df["product_category"].where(
        df["product_category"].isin(PRODUCT_CATEGORIES), "UNKNOWN")
    df["region"]      = df["region"].where(df["region"].isin(REGIONS), "UNKNOWN")
    df["customer_id"] = df["customer_id"].fillna("UNKNOWN")
    df["year"]  = df["timestamp"].dt.year
    df["month"] = df["timestamp"].dt.month

    stats["final"] = len(df)
    return df, stats


# ---------------------------------------------------------------------------
# KPI Computation  (mirrors glue_jobs/02_cleansed_to_curated_kpis.py)
# ---------------------------------------------------------------------------

def compute_kpis(df: pd.DataFrame) -> dict:
    grand_total = df["amount"].sum()

    # KPI 1 - Revenue per Category
    kpi1 = (
        df.groupby("product_category")
        .agg(
            total_revenue        = ("amount",         "sum"),
            transaction_count    = ("transaction_id", "count"),
            avg_transaction_value= ("amount",         "mean"),
            unique_customers     = ("customer_id",    "nunique"),
        )
        .reset_index()
    )
    kpi1["revenue_pct"]           = (kpi1["total_revenue"] / grand_total * 100).round(2)
    kpi1["total_revenue"]         = kpi1["total_revenue"].round(2)
    kpi1["avg_transaction_value"] = kpi1["avg_transaction_value"].round(2)
    kpi1 = kpi1.sort_values("total_revenue", ascending=False)

    # KPI 2 - Monthly Sales Growth
    kpi2 = (
        df.groupby(["year", "month"])
        .agg(
            total_revenue        = ("amount",         "sum"),
            transaction_count    = ("transaction_id", "count"),
            unique_customers     = ("customer_id",    "nunique"),
            avg_transaction_value= ("amount",         "mean"),
        )
        .reset_index()
        .sort_values(["year", "month"])
    )
    kpi2["month_label"]        = kpi2["year"].astype(str) + "-" + kpi2["month"].astype(str).str.zfill(2)
    kpi2["prev_month_revenue"] = kpi2["total_revenue"].shift(1)
    kpi2["mom_growth_pct"]     = (
        (kpi2["total_revenue"] - kpi2["prev_month_revenue"])
        / kpi2["prev_month_revenue"] * 100
    ).round(2)
    kpi2["total_revenue"]          = kpi2["total_revenue"].round(2)
    kpi2["avg_transaction_value"]  = kpi2["avg_transaction_value"].round(2)
    kpi2["prev_month_revenue"]     = kpi2["prev_month_revenue"].round(2)

    # KPI 3 - Top 5 Regions by Volume
    total_txns = len(df)
    total_rev  = df["amount"].sum()
    kpi3 = (
        df.groupby("region")
        .agg(
            transaction_count  = ("transaction_id", "count"),
            total_revenue      = ("amount",         "sum"),
            avg_revenue_per_txn= ("amount",         "mean"),
            unique_customers   = ("customer_id",    "nunique"),
        )
        .reset_index()
    )
    kpi3["volume_share_pct"]  = (kpi3["transaction_count"] / total_txns * 100).round(2)
    kpi3["revenue_share_pct"] = (kpi3["total_revenue"] / total_rev * 100).round(2)
    kpi3["total_revenue"]     = kpi3["total_revenue"].round(2)
    kpi3["avg_revenue_per_txn"] = kpi3["avg_revenue_per_txn"].round(2)
    kpi3 = kpi3.sort_values("transaction_count", ascending=False).head(5).reset_index(drop=True)
    kpi3.insert(0, "rank", range(1, len(kpi3) + 1))

    return {
        "kpi1": kpi1.to_dict("records"),
        "kpi2": kpi2.to_dict("records"),
        "kpi3": kpi3.to_dict("records"),
    }


# ---------------------------------------------------------------------------
# Chart Builder
# ---------------------------------------------------------------------------

def build_charts(kpis: dict) -> dict:
    bg = "#1A1F2E"
    k1 = pd.DataFrame(kpis["kpi1"])
    k2 = pd.DataFrame(kpis["kpi2"])
    k3 = pd.DataFrame(kpis["kpi3"])

    fig1 = px.bar(
        k1, x="product_category", y="total_revenue",
        color="revenue_pct", color_continuous_scale="Oranges",
        title="Total Revenue by Product Category",
        labels={"product_category": "Category", "total_revenue": "Revenue ($)",
                "revenue_pct": "% of Total"},
    )
    fig1.update_layout(template="plotly_dark", paper_bgcolor=bg, plot_bgcolor=bg,
                       margin=dict(t=50, b=40, l=10, r=10))

    fig2 = px.pie(
        k1, values="total_revenue", names="product_category",
        title="Revenue Share by Category",
        color_discrete_sequence=px.colors.sequential.Oranges_r,
    )
    fig2.update_layout(template="plotly_dark", paper_bgcolor=bg,
                       margin=dict(t=50, b=10, l=10, r=10))

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(
        x=k2["month_label"], y=k2["total_revenue"],
        name="Monthly Revenue", marker_color="#FF6B35", yaxis="y",
    ))
    fig3.add_trace(go.Scatter(
        x=k2["month_label"], y=k2["mom_growth_pct"],
        name="MoM Growth %", line=dict(color="#00D4FF", width=2),
        mode="lines+markers", yaxis="y2",
    ))
    fig3.update_layout(
        title="Monthly Revenue & Growth (MoM %)",
        template="plotly_dark", paper_bgcolor=bg, plot_bgcolor=bg,
        yaxis  =dict(title="Revenue ($)"),
        yaxis2 =dict(title="MoM Growth %", overlaying="y", side="right"),
        xaxis  =dict(tickangle=-45),
        legend =dict(x=0, y=1.1, orientation="h"),
        margin =dict(t=60, b=60, l=10, r=10),
    )

    fig4 = px.bar(
        k3, x="region", y="transaction_count",
        color="volume_share_pct", color_continuous_scale="Blues",
        title="Top 5 Regions by Transaction Volume",
        labels={"region": "Region", "transaction_count": "Transactions",
                "volume_share_pct": "Volume Share %"},
    )
    fig4.update_layout(template="plotly_dark", paper_bgcolor=bg, plot_bgcolor=bg,
                       margin=dict(t=50, b=40, l=10, r=10))

    fig5 = px.scatter(
        k3, x="transaction_count", y="total_revenue",
        size="unique_customers", color="region", text="region",
        title="Revenue vs Volume (bubble = unique customers)",
        labels={"transaction_count": "Transactions", "total_revenue": "Revenue ($)"},
    )
    fig5.update_traces(textposition="top center")
    fig5.update_layout(template="plotly_dark", paper_bgcolor=bg, plot_bgcolor=bg,
                       margin=dict(t=50, b=10, l=10, r=10))

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
# HTML Dashboard (single-page, served as a string to avoid Jinja2 conflicts)
# ---------------------------------------------------------------------------

def _html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>E-Commerce Sales Pipeline</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/css/bootstrap.min.css" rel="stylesheet">
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
:root{
  --bg:#0E1117; --card:#1A1F2E; --border:#2D3447;
  --accent:#FF6B35; --accent2:#00D4FF;
  --text:#FAFAFA; --muted:#8B9AB1;
}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',sans-serif;margin:0}

/* ---- layout ---- */
.wrap{display:flex;min-height:100vh}
.sidebar{
  width:280px;min-width:280px;background:var(--card);
  border-right:1px solid var(--border);
  padding:1.25rem 1rem;display:flex;flex-direction:column;gap:.6rem;
  position:sticky;top:0;height:100vh;overflow-y:auto;
}
.main{flex:1;padding:1.5rem;overflow:auto}

/* ---- sidebar ---- */
.logo{color:var(--accent);font-size:1rem;font-weight:700;margin-bottom:.5rem}
.logo small{color:var(--muted);font-size:.72rem;font-weight:400;display:block;margin-top:2px}
.sec-label{font-size:.68rem;text-transform:uppercase;letter-spacing:1.4px;color:var(--muted);margin:.8rem 0 .3rem}

/* source tabs */
.src-tabs{display:flex;gap:4px;margin-bottom:.75rem}
.src-tab{flex:1;text-align:center;padding:.35rem .3rem;border-radius:6px;
  font-size:.78rem;cursor:pointer;border:1px solid var(--border);color:var(--muted);
  background:transparent;transition:all .15s}
.src-tab.active{background:var(--accent);border-color:var(--accent);color:#fff;font-weight:600}
.src-panel{display:none}.src-panel.active{display:block}

/* form controls */
label.fl{color:var(--muted);font-size:.8rem;display:block;margin-bottom:3px}
input[type=range]{width:100%;accent-color:var(--accent);margin-bottom:6px}
.inp{width:100%;background:#252B3B;border:1px solid var(--border);color:var(--text);
  border-radius:6px;padding:.45rem .6rem;font-size:.85rem}
.inp:focus{outline:2px solid var(--accent);border-color:transparent}

/* buttons */
.btn-run{width:100%;padding:.75rem;border:none;border-radius:8px;
  background:linear-gradient(135deg,#FF6B35,#FF8C42);
  color:#fff;font-weight:700;font-size:.95rem;cursor:pointer;
  transition:opacity .2s;margin-top:.5rem}
.btn-run:disabled{opacity:.45;cursor:not-allowed}
.btn-run:hover:not(:disabled){opacity:.9}

/* pipeline steps */
.step{display:flex;align-items:center;gap:8px;font-size:.78rem;color:var(--muted);margin:.2rem 0}
.dot{width:8px;height:8px;border-radius:50%;background:var(--border);flex-shrink:0}
.dot.done{background:#22C55E}
.dot.active{background:var(--accent);animation:pulse 1s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}

/* upload zone */
.drop-zone{border:2px dashed var(--border);border-radius:8px;padding:1.25rem .5rem;
  text-align:center;cursor:pointer;font-size:.8rem;color:var(--muted);transition:border-color .2s}
.drop-zone:hover{border-color:var(--accent);color:var(--accent)}

/* manual rows */
.mrow{display:grid;grid-template-columns:2fr 1fr 1.5fr auto;gap:4px;margin-bottom:5px}
.mrow .inp{font-size:.75rem;padding:.3rem .4rem}
.btn-del{background:#2D1515;border:1px solid #5c2323;color:#ff6b6b;border-radius:4px;
  padding:2px 7px;cursor:pointer;font-size:.8rem}
.btn-add{width:100%;background:#252B3B;border:1px solid var(--border);color:var(--muted);
  border-radius:6px;padding:.4rem;font-size:.8rem;cursor:pointer;margin-bottom:5px}
.btn-add:hover{border-color:var(--accent);color:var(--accent)}

/* ---- main area ---- */
h1{font-size:1.6rem;font-weight:800;margin-bottom:.1rem}
h1 span{color:var(--accent)}
.subtitle{color:var(--muted);font-size:.88rem;margin-bottom:1.25rem}

/* kpi cards */
.kpi-row{display:grid;grid-template-columns:repeat(4,1fr);gap:1rem;margin-bottom:1.25rem}
.kpi-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:1rem}
.kpi-card .lbl{font-size:.7rem;text-transform:uppercase;letter-spacing:1px;color:var(--muted)}
.kpi-card .val{font-size:1.7rem;font-weight:800;color:var(--accent);margin:.2rem 0 .1rem}
.kpi-card .sub{font-size:.74rem;color:var(--muted)}

/* cleaning badges */
.badge-row{margin-bottom:1rem}
.cbadge{background:#252B3B;border:1px solid var(--border);border-radius:6px;
  padding:.25rem .55rem;font-size:.74rem;color:var(--muted);display:inline-block;margin:2px}
.cbadge b{color:var(--accent2)}

/* chart tabs */
.ctabs{display:flex;gap:4px;border-bottom:1px solid var(--border);margin-bottom:0}
.ctab{padding:.5rem 1rem;font-size:.84rem;color:var(--muted);cursor:pointer;
  border-bottom:2px solid transparent;transition:color .15s}
.ctab.active{color:var(--accent);border-bottom-color:var(--accent)}
.ctab-panel{display:none;background:var(--card);border:1px solid var(--border);
  border-top:none;border-radius:0 0 12px 12px;padding:1rem}
.ctab-panel.active{display:block}

/* tables */
.tbl{width:100%;border-collapse:collapse;font-size:.8rem}
.tbl th{color:var(--muted);font-size:.7rem;text-transform:uppercase;letter-spacing:.5px;
  border-bottom:1px solid var(--border);padding:.5rem .6rem;text-align:left}
.tbl td{padding:.45rem .6rem;border-bottom:1px solid var(--border);color:var(--text)}
.tbl tr:hover td{background:rgba(255,107,53,.04)}

/* spinner / error */
.spinner-wrap{display:none;text-align:center;padding:3rem}
.spinner{width:3rem;height:3rem;border:4px solid var(--border);
  border-top-color:var(--accent);border-radius:50%;
  animation:spin 1s linear infinite;margin:0 auto}
@keyframes spin{to{transform:rotate(360deg)}}
.err-box{background:#2D1515;border:1px solid #7f1d1d;color:#fca5a5;
  border-radius:8px;padding:1rem;margin-bottom:1rem;display:none}
.results{display:none}

@media(max-width:768px){
  .wrap{flex-direction:column}
  .sidebar{width:100%;height:auto;position:relative}
  .kpi-row{grid-template-columns:repeat(2,1fr)}
}
</style>
</head>
<body>
<div class="wrap">

<!-- ======================== SIDEBAR ======================== -->
<aside class="sidebar">
  <div class="logo">
    E-Commerce Sales Pipeline
    <small>AWS Glue &amp; Redshift Simulation</small>
  </div>

  <div class="sec-label">Data Source</div>
  <div class="src-tabs">
    <button class="src-tab active" onclick="switchSrc('gen',this)">Generate</button>
    <button class="src-tab"       onclick="switchSrc('up',this)">Upload</button>
    <button class="src-tab"       onclick="switchSrc('man',this)">Manual</button>
  </div>

  <!-- Generate panel -->
  <div class="src-panel active" id="src-gen">
    <label class="fl">Records: <b id="recLbl">10,000</b></label>
    <input type="range" id="recSlider" min="1000" max="50000" step="1000" value="10000"
      oninput="document.getElementById('recLbl').textContent=Number(this.value).toLocaleString()">
    <label class="fl" style="margin-top:6px">Random Seed</label>
    <input class="inp" type="number" id="seedInp" value="42" min="0">
  </div>

  <!-- Upload panel -->
  <div class="src-panel" id="src-up">
    <div class="drop-zone" onclick="document.getElementById('csvUpload').click()">
      Click to upload CSV<br>
      <span style="font-size:.7rem">Required columns: transaction_id, customer_id,<br>product_category, amount, timestamp, region</span>
    </div>
    <input type="file" id="csvUpload" accept=".csv" style="display:none" onchange="onFileChosen(this)">
    <div id="fileInfo" style="font-size:.75rem;color:var(--muted);margin-top:6px"></div>
  </div>

  <!-- Manual panel -->
  <div class="src-panel" id="src-man">
    <div style="font-size:.72rem;color:var(--muted);margin-bottom:6px">
      Category / Amount / Region
    </div>
    <div id="manualRows"></div>
    <button class="btn-add" onclick="addRow()">+ Add Row</button>
  </div>

  <div class="sec-label">Pipeline Stages</div>
  <div class="step"><div class="dot" id="d1"></div> Load &amp; Validate</div>
  <div class="step"><div class="dot" id="d2"></div> Clean &amp; Deduplicate</div>
  <div class="step"><div class="dot" id="d3"></div> Compute KPIs</div>
  <div class="step"><div class="dot" id="d4"></div> Render Charts</div>

  <button class="btn-run" id="runBtn" onclick="runPipeline()">Run Pipeline</button>
</aside>

<!-- ======================== MAIN ======================== -->
<main class="main">
  <h1>E-Commerce Sales <span>Dashboard</span></h1>
  <p class="subtitle">End-to-end ETL &nbsp;|&nbsp; AWS Glue + Redshift simulation &nbsp;|&nbsp; 3 KPIs</p>

  <div class="err-box" id="errBox"></div>

  <div class="spinner-wrap" id="spinWrap">
    <div class="spinner"></div>
    <p style="margin-top:1rem;color:var(--muted)">Running pipeline&hellip;</p>
  </div>

  <div class="results" id="resultsWrap">

    <!-- KPI Cards -->
    <div class="kpi-row">
      <div class="kpi-card"><div class="lbl">Total Records</div><div class="val" id="cRec">-</div><div class="sub">after cleaning</div></div>
      <div class="kpi-card"><div class="lbl">Total Revenue</div><div class="val" id="cRev">-</div><div class="sub">USD</div></div>
      <div class="kpi-card"><div class="lbl">Avg Transaction</div><div class="val" id="cAvg">-</div><div class="sub">per order</div></div>
      <div class="kpi-card"><div class="lbl">Unique Customers</div><div class="val" id="cCust">-</div><div class="sub">distinct IDs</div></div>
    </div>

    <!-- Cleaning stats -->
    <div class="badge-row" id="cleanBadges"></div>

    <!-- Chart Tabs -->
    <div class="ctabs">
      <div class="ctab active" onclick="switchTab(0,this)">Revenue / Category</div>
      <div class="ctab"       onclick="switchTab(1,this)">Monthly Growth</div>
      <div class="ctab"       onclick="switchTab(2,this)">Top Regions</div>
      <div class="ctab"       onclick="switchTab(3,this)">Raw Data</div>
    </div>

    <!-- Tab 0: Revenue -->
    <div class="ctab-panel active" id="tp0">
      <div style="display:grid;grid-template-columns:3fr 2fr;gap:1rem">
        <div id="chRevBar" style="height:370px"></div>
        <div id="chRevPie" style="height:370px"></div>
      </div>
      <div style="overflow-x:auto;margin-top:1rem">
        <table class="tbl" id="t1"></table>
      </div>
    </div>

    <!-- Tab 1: Monthly -->
    <div class="ctab-panel" id="tp1">
      <div id="chMonthly" style="height:420px"></div>
      <div style="overflow-x:auto;margin-top:1rem">
        <table class="tbl" id="t2"></table>
      </div>
    </div>

    <!-- Tab 2: Regions -->
    <div class="ctab-panel" id="tp2">
      <div style="display:grid;grid-template-columns:3fr 2fr;gap:1rem">
        <div id="chRegBar"     style="height:370px"></div>
        <div id="chRegScatter" style="height:370px"></div>
      </div>
      <div style="overflow-x:auto;margin-top:1rem">
        <table class="tbl" id="t3"></table>
      </div>
    </div>

    <!-- Tab 3: Raw Data -->
    <div class="ctab-panel" id="tp3">
      <p style="font-size:.78rem;color:var(--muted)">Showing first 100 cleaned rows</p>
      <div style="overflow-x:auto">
        <table class="tbl" id="t4"></table>
      </div>
    </div>

  </div><!-- /results -->
</main>
</div><!-- /wrap -->

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.2/dist/js/bootstrap.bundle.min.js"></script>
<script>
// ---- state ----
let currentSrc = 'gen';
let uploadedCSV = null;
let manualRows  = [];
const CATS = ["Electronics","Clothing","Books","Home & Garden","Sports",
              "Toys","Automotive","Food & Beverage","Health & Beauty","Office Supplies"];
const REGS = ["us-east-1","us-west-2","eu-west-1",
              "ap-southeast-1","ap-northeast-1","sa-east-1","ca-central-1"];

// ---- source tab switch ----
function switchSrc(id, btn) {
  currentSrc = id;
  document.querySelectorAll('.src-tab').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.src-panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('src-' + id).classList.add('active');
}

// ---- file upload ----
function onFileChosen(input) {
  const f = input.files[0];
  if (!f) return;
  const r = new FileReader();
  r.onload = e => {
    uploadedCSV = e.target.result;
    document.getElementById('fileInfo').textContent =
      'Loaded: ' + f.name + '  (' + (f.size/1024).toFixed(1) + ' KB)';
  };
  r.readAsText(f);
}

// ---- manual rows ----
function addRow() {
  manualRows.push({
    transaction_id:   (typeof crypto !== 'undefined' && crypto.randomUUID) ? crypto.randomUUID() : Date.now().toString(36),
    customer_id:      'CUST_001',
    product_category: 'Electronics',
    amount:           '',
    timestamp:        '2024-06-15 10:00:00',
    region:           'us-east-1',
  });
  renderRows();
}

function removeRow(i) { manualRows.splice(i,1); renderRows(); }

function renderRows() {
  const c = document.getElementById('manualRows');
  if (!manualRows.length) {
    c.innerHTML = '<div style="color:var(--muted);font-size:.78rem;text-align:center;padding:.75rem">Click Add Row to enter data</div>';
    return;
  }
  c.innerHTML = manualRows.map((r,i) => `
    <div class="mrow">
      <select class="inp" onchange="manualRows[${i}].product_category=this.value">
        ${CATS.map(c=>`<option value="${c}" ${c===r.product_category?'selected':''}>${c}</option>`).join('')}
      </select>
      <input class="inp" type="number" placeholder="Amount" value="${r.amount}"
        onchange="manualRows[${i}].amount=this.value">
      <select class="inp" onchange="manualRows[${i}].region=this.value">
        ${REGS.map(rg=>`<option value="${rg}" ${rg===r.region?'selected':''}>${rg}</option>`).join('')}
      </select>
      <button class="btn-del" onclick="removeRow(${i})">x</button>
    </div>`).join('');
}

// ---- pipeline step indicator ----
function setStep(n) {
  ['d1','d2','d3','d4'].forEach((id,i) => {
    const el = document.getElementById(id);
    el.className = 'dot' + (i < n ? ' done' : i === n ? ' active' : '');
  });
}

// ---- chart tab switch ----
function switchTab(idx, btn) {
  document.querySelectorAll('.ctab').forEach(t => t.classList.remove('active'));
  document.querySelectorAll('.ctab-panel').forEach(p => p.classList.remove('active'));
  btn.classList.add('active');
  document.getElementById('tp' + idx).classList.add('active');
  // Trigger resize so Plotly redraws in newly visible div
  window.dispatchEvent(new Event('resize'));
}

// ---- helpers ----
const fmtN   = n => Number(n).toLocaleString(undefined,{maximumFractionDigits:0});
const fmtUSD = n => '$' + Number(n).toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
const fmtPct = n => (n == null ? '-' : Number(n).toFixed(2) + '%');

function buildTable(el, rows, cols) {
  if (!rows || !rows.length) { el.innerHTML=''; return; }
  el.innerHTML =
    '<thead><tr>' + cols.map(c=>`<th>${c.label||c.key}</th>`).join('') + '</tr></thead>' +
    '<tbody>' + rows.map(r =>
      '<tr>' + cols.map(c => {
        const v = r[c.key];
        let display = v == null ? '-' : v;
        if (c.fmt === 'usd') display = fmtUSD(v);
        else if (c.fmt === 'num') display = fmtN(v);
        else if (c.fmt === 'pct') display = fmtPct(v);
        return `<td>${display}</td>`;
      }).join('') + '</tr>'
    ).join('') + '</tbody>';
}

function showError(msg) {
  const b = document.getElementById('errBox');
  b.textContent = 'Error: ' + msg;
  b.style.display = 'block';
  document.getElementById('spinWrap').style.display = 'none';
  document.getElementById('runBtn').disabled = false;
}

// ---- main pipeline runner ----
async function runPipeline() {
  const btn     = document.getElementById('runBtn');
  const spinner = document.getElementById('spinWrap');
  const results = document.getElementById('resultsWrap');
  const errBox  = document.getElementById('errBox');

  errBox.style.display = 'none';
  spinner.style.display = 'block';
  results.style.display = 'none';
  btn.disabled = true;
  setStep(0);

  let body;
  if (currentSrc === 'gen') {
    body = {
      source:  'generate',
      records: parseInt(document.getElementById('recSlider').value),
      seed:    parseInt(document.getElementById('seedInp').value),
    };
  } else if (currentSrc === 'up') {
    if (!uploadedCSV) { showError('Please select a CSV file first.'); return; }
    body = { source: 'upload', csv: uploadedCSV };
  } else {
    if (!manualRows.length) { showError('Please add at least one row.'); return; }
    body = { source: 'manual', rows: manualRows };
  }

  try {
    setStep(1);
    const resp = await fetch('/api/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    setStep(2);
    const data = await resp.json();
    if (!resp.ok) throw new Error(data.error || 'Server error ' + resp.status);
    setStep(3);
    renderResults(data);
    setStep(4);
    results.style.display = 'block';
  } catch(e) {
    showError(e.message);
  } finally {
    spinner.style.display = 'none';
    btn.disabled = false;
  }
}

function renderResults(data) {
  // KPI cards
  const s = data.summary;
  document.getElementById('cRec').textContent  = fmtN(s.total_records);
  document.getElementById('cRev').textContent  = fmtUSD(s.total_revenue);
  document.getElementById('cAvg').textContent  = fmtUSD(s.avg_transaction);
  document.getElementById('cCust').textContent = fmtN(s.unique_customers);

  // Cleaning badges
  const st = data.stats;
  const steps = [
    ['Raw',          st.raw],
    ['After PK Drop',st.after_pk_null_drop],
    ['After Dedup',  st.after_dedup],
    ['After Amount', st.after_amount],
    ['After TS',     st.after_ts],
    ['Final',        st.final],
  ];
  document.getElementById('cleanBadges').innerHTML =
    steps.map(([l,v]) => `<span class="cbadge">${l}: <b>${fmtN(v)}</b></span>`).join('');

  // Charts
  const cfg = { responsive: true };
  const C = data.charts;
  Plotly.react('chRevBar',     C.revenue_bar.data,     C.revenue_bar.layout,     cfg);
  Plotly.react('chRevPie',     C.revenue_pie.data,     C.revenue_pie.layout,     cfg);
  Plotly.react('chMonthly',    C.monthly_growth.data,  C.monthly_growth.layout,  cfg);
  Plotly.react('chRegBar',     C.regions_bar.data,     C.regions_bar.layout,     cfg);
  Plotly.react('chRegScatter', C.regions_scatter.data, C.regions_scatter.layout, cfg);

  // Tables
  buildTable(document.getElementById('t1'), data.kpis.kpi1, [
    {key:'product_category', label:'Category'},
    {key:'total_revenue',    label:'Revenue',     fmt:'usd'},
    {key:'transaction_count',label:'Transactions',fmt:'num'},
    {key:'revenue_pct',      label:'% Share',     fmt:'pct'},
    {key:'avg_transaction_value',label:'Avg Order',fmt:'usd'},
    {key:'unique_customers', label:'Customers',   fmt:'num'},
  ]);
  buildTable(document.getElementById('t2'), data.kpis.kpi2, [
    {key:'month_label',      label:'Month'},
    {key:'total_revenue',    label:'Revenue',     fmt:'usd'},
    {key:'transaction_count',label:'Transactions',fmt:'num'},
    {key:'mom_growth_pct',   label:'MoM Growth',  fmt:'pct'},
    {key:'unique_customers', label:'Customers',   fmt:'num'},
  ]);
  buildTable(document.getElementById('t3'), data.kpis.kpi3, [
    {key:'rank',             label:'Rank'},
    {key:'region',           label:'Region'},
    {key:'transaction_count',label:'Transactions',fmt:'num'},
    {key:'total_revenue',    label:'Revenue',     fmt:'usd'},
    {key:'volume_share_pct', label:'Vol %',       fmt:'pct'},
    {key:'revenue_share_pct',label:'Rev %',       fmt:'pct'},
  ]);
  buildTable(document.getElementById('t4'), data.raw_preview, [
    {key:'transaction_id',   label:'Transaction ID'},
    {key:'customer_id',      label:'Customer'},
    {key:'product_category', label:'Category'},
    {key:'amount',           label:'Amount',fmt:'usd'},
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
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return Response(_html(), mimetype="text/html")


@app.route("/api/run", methods=["POST"])
def api_run():
    try:
        body   = request.get_json(force=True)
        source = body.get("source", "generate")

        if source == "generate":
            n    = min(int(body.get("records", 10_000)), MAX_RECORDS)
            seed = int(body.get("seed", 42))
            df   = generate_data(n, seed)

        elif source == "upload":
            csv_text = body.get("csv", "")
            df = pd.read_csv(io.StringIO(csv_text))
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if len(df) > MAX_RECORDS:
                df = df.head(MAX_RECORDS)

        elif source == "manual":
            rows = body.get("rows", [])
            if not rows:
                return jsonify({"error": "No rows provided"}), 400
            df = pd.DataFrame(rows)
            df["amount"]    = pd.to_numeric(df.get("amount",    None), errors="coerce")
            df["timestamp"] = pd.to_datetime(df.get("timestamp", None), errors="coerce")
            df["timestamp"] = df["timestamp"].fillna(pd.Timestamp.now())
            if "transaction_id" not in df.columns:
                df["transaction_id"] = [str(uuid.uuid4()) for _ in range(len(df))]

        else:
            return jsonify({"error": "Unknown source"}), 400

        # Ensure required columns exist
        for col in ["transaction_id", "customer_id", "product_category",
                    "amount", "timestamp", "region"]:
            if col not in df.columns:
                df[col] = None

        df_clean, stats = run_etl(df)
        if len(df_clean) == 0:
            return jsonify({"error": "No records remained after cleaning. Check your data."}), 400

        kpis    = compute_kpis(df_clean)
        charts  = build_charts(kpis)
        summary = {
            "total_records":    int(len(df_clean)),
            "total_revenue":    round(float(df_clean["amount"].sum()), 2),
            "avg_transaction":  round(float(df_clean["amount"].mean()), 2),
            "unique_customers": int(df_clean["customer_id"].nunique()),
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
# Local dev entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, port=5000)
