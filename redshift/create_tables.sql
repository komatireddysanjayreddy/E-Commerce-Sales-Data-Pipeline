-- =============================================================================
-- create_tables.sql
-- Redshift DDL for the Sales Data Warehouse
--
-- Run order:
--   1. psql / redshift-query-editor  →  execute this script once
--   2. Then run copy_to_redshift.py for each pipeline execution
--
-- Encoding strategy:
--   - Timestamps / integers : AZ64  (best for monotonically increasing data)
--   - Short strings          : ZSTD (general-purpose, high compression)
--   - Long free-text         : LZO  (fast decompression)
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Schema
-- ─────────────────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS sales_dw;

SET search_path TO sales_dw;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. FACT TABLE  –  Sales Transactions
--    DISTKEY  on region  → colocates joins with dimension tables
--    SORTKEY  on (transaction_ts, product_category) → common filter pattern
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS sales_dw.fact_sales;

CREATE TABLE sales_dw.fact_sales
(
    transaction_id        VARCHAR(36)     NOT NULL  ENCODE zstd,
    customer_id           VARCHAR(20)     NOT NULL  ENCODE zstd,
    product_category      VARCHAR(50)     NOT NULL  ENCODE zstd,
    amount                DECIMAL(12, 2)  NOT NULL  ENCODE az64,
    transaction_ts        TIMESTAMP       NOT NULL  ENCODE az64,
    region                VARCHAR(20)     NOT NULL  ENCODE zstd,
    year                  SMALLINT        NOT NULL  ENCODE az64,
    month                 SMALLINT        NOT NULL  ENCODE az64,
    day                   SMALLINT        NOT NULL  ENCODE az64,
    _ingestion_ts         TIMESTAMP                 ENCODE az64,
    _source_system        VARCHAR(60)               ENCODE zstd,
    _pipeline_version     VARCHAR(10)               ENCODE zstd
)
DISTSTYLE KEY
DISTKEY   (region)
COMPOUND SORTKEY (transaction_ts, product_category);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. KPI TABLE  –  Revenue per Product Category
--    DISTSTYLE ALL  → small table, broadcast to all slices
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS sales_dw.kpi_revenue_per_category;

CREATE TABLE sales_dw.kpi_revenue_per_category
(
    product_category       VARCHAR(50)     NOT NULL  ENCODE zstd,
    total_revenue          DECIMAL(18, 2)  NOT NULL  ENCODE az64,
    transaction_count      BIGINT          NOT NULL  ENCODE az64,
    avg_transaction_value  DECIMAL(12, 2)            ENCODE az64,
    min_amount             DECIMAL(12, 2)            ENCODE az64,
    max_amount             DECIMAL(12, 2)            ENCODE az64,
    unique_customers       BIGINT                    ENCODE az64,
    revenue_pct            DECIMAL(6,  2)            ENCODE az64,
    _computed_at           TIMESTAMP                 ENCODE az64
)
DISTSTYLE ALL
SORTKEY (total_revenue);


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. KPI TABLE  –  Monthly Sales Growth
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS sales_dw.kpi_monthly_sales_growth;

CREATE TABLE sales_dw.kpi_monthly_sales_growth
(
    year                   SMALLINT        NOT NULL  ENCODE az64,
    month                  SMALLINT        NOT NULL  ENCODE az64,
    month_label            CHAR(7)         NOT NULL  ENCODE zstd,
    total_revenue          DECIMAL(18, 2)  NOT NULL  ENCODE az64,
    transaction_count      BIGINT          NOT NULL  ENCODE az64,
    unique_customers       BIGINT                    ENCODE az64,
    avg_transaction_value  DECIMAL(12, 2)            ENCODE az64,
    prev_month_revenue     DECIMAL(18, 2)            ENCODE az64,
    mom_growth_pct         DECIMAL(8,  2)            ENCODE az64,
    _computed_at           TIMESTAMP                 ENCODE az64
)
DISTSTYLE ALL
SORTKEY (year, month);


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. KPI TABLE  –  Top 5 Regions by Volume
-- ─────────────────────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS sales_dw.kpi_top_regions_by_volume;

CREATE TABLE sales_dw.kpi_top_regions_by_volume
(
    rank                   SMALLINT        NOT NULL  ENCODE az64,
    region                 VARCHAR(20)     NOT NULL  ENCODE zstd,
    transaction_count      BIGINT          NOT NULL  ENCODE az64,
    total_revenue          DECIMAL(18, 2)  NOT NULL  ENCODE az64,
    avg_revenue_per_txn    DECIMAL(12, 2)            ENCODE az64,
    unique_customers       BIGINT                    ENCODE az64,
    volume_share_pct       DECIMAL(6,  2)            ENCODE az64,
    revenue_share_pct      DECIMAL(6,  2)            ENCODE az64,
    _computed_at           TIMESTAMP                 ENCODE az64
)
DISTSTYLE ALL
SORTKEY (rank);


-- ─────────────────────────────────────────────────────────────────────────────
-- Analytical Views
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW sales_dw.v_sales_summary AS
SELECT
    year,
    month,
    product_category,
    region,
    COUNT(transaction_id)       AS transaction_count,
    SUM(amount)                 AS total_revenue,
    ROUND(AVG(amount), 2)       AS avg_order_value,
    COUNT(DISTINCT customer_id) AS unique_customers
FROM  sales_dw.fact_sales
GROUP BY year, month, product_category, region;


CREATE OR REPLACE VIEW sales_dw.v_category_performance AS
SELECT
    product_category,
    total_revenue,
    transaction_count,
    avg_transaction_value,
    revenue_pct,
    unique_customers,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
FROM sales_dw.kpi_revenue_per_category;


CREATE OR REPLACE VIEW sales_dw.v_growth_trend AS
SELECT
    month_label,
    total_revenue,
    mom_growth_pct,
    CASE
        WHEN mom_growth_pct > 0  THEN 'GROWTH'
        WHEN mom_growth_pct < 0  THEN 'DECLINE'
        WHEN mom_growth_pct = 0  THEN 'FLAT'
        ELSE                          'BASELINE'
    END AS growth_status
FROM sales_dw.kpi_monthly_sales_growth
ORDER BY year, month;


-- ─────────────────────────────────────────────────────────────────────────────
-- Confirm table creation
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    schemaname,
    tablename,
    tableowner
FROM pg_tables
WHERE schemaname = 'sales_dw'
ORDER BY tablename;
