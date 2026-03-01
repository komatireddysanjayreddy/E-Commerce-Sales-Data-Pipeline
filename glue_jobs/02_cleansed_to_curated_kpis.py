"""
02_cleansed_to_curated_kpis.py
===============================
AWS Glue PySpark Job  -  Stage 2: Cleansed Zone -> Curated Zone

Responsibilities
----------------
1. Read Parquet from S3 Cleansed zone
2. Enrich with year / month / day partition columns
3. Write fact table partitioned by year/month/day  (Curated/sales/)
4. Compute KPI 1 - Total Revenue per Product Category
5. Compute KPI 2 - Monthly Sales Growth (MoM %)
6. Compute KPI 3 - Top 5 Regions by Transaction Volume
7. Write each KPI table to Curated/kpis/<kpi_name>/
8. Log a human-readable summary to CloudWatch

Job Parameters
--------------
--SOURCE_BUCKET   : Cleansed zone bucket name
--TARGET_BUCKET   : Curated zone bucket name
--GLUE_DATABASE   : Glue catalog database name
--JOB_NAME        : (injected automatically by Glue)
"""

import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F, Window

# -- Logging -------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# -- Job initialisation --------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET", "GLUE_DATABASE"],
)

sc      = SparkContext()
glueCtx = GlueContext(sc)
spark   = glueCtx.spark_session
job     = Job(glueCtx)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]
GLUE_DATABASE = args["GLUE_DATABASE"]

SOURCE_PATH        = f"s3://{SOURCE_BUCKET}/sales/"
CURATED_SALES_PATH = f"s3://{TARGET_BUCKET}/sales"
CURATED_KPI_BASE   = f"s3://{TARGET_BUCKET}/kpis"

# -- Spark tuning --------------------------------------------------------------

spark.conf.set("spark.sql.adaptive.enabled",                         "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",      "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
spark.conf.set("spark.sql.parquet.compression.codec",                "snappy")
spark.conf.set("spark.sql.shuffle.partitions",                       "200")
spark.conf.set("spark.sql.sources.partitionOverwriteMode",           "dynamic")


# -- Step 1: Read --------------------------------------------------------------

def read_cleansed(path: str):
    log.info("Reading cleansed Parquet from: %s", path)
    df = spark.read.parquet(path)
    count = df.count()
    log.info("Cleansed record count: %s", f"{count:,}")
    return df, count


# -- Step 2: Add time partitions -----------------------------------------------

def add_partitions(df):
    """Extract year / month / day from the timestamp column."""
    return (
        df
        .withColumn("year",  F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("day",   F.dayofmonth("timestamp"))
    )


# -- Step 3: Write partitioned fact table --------------------------------------

def write_fact_table(df, path: str):
    """
    Partition by year / month / day.
    Dynamic overwrite lets incremental runs replace only affected partitions.
    Repartition by year+month first to colocate data before writing.
    """
    log.info("Writing partitioned fact table to: %s", path)
    (
        df.repartition(F.col("year"), F.col("month"))
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .partitionBy("year", "month", "day")
          .parquet(path)
    )
    log.info("Fact table written.")


# -- KPI 1: Total Revenue per Category ----------------------------------------

def kpi_revenue_per_category(df):
    """
    Aggregates:
        total_revenue          - sum of amount
        transaction_count      - number of transactions
        avg_transaction_value  - average order value
        min_amount / max_amount
        unique_customers       - distinct customer count
        revenue_pct            - share of grand total revenue (%)
    """
    log.info("Computing KPI 1: Revenue per Category...")

    grand_total = df.agg(F.sum("amount").alias("gt")).collect()[0]["gt"]

    kpi = (
        df.groupBy("product_category")
          .agg(
              F.sum("amount")               .alias("total_revenue"),
              F.count("transaction_id")     .alias("transaction_count"),
              F.avg("amount")               .alias("avg_transaction_value"),
              F.min("amount")               .alias("min_amount"),
              F.max("amount")               .alias("max_amount"),
              F.countDistinct("customer_id").alias("unique_customers"),
          )
          .withColumn("revenue_pct",
                      F.round(F.col("total_revenue") / grand_total * 100, 2))
          .withColumn("total_revenue",
                      F.round("total_revenue", 2))
          .withColumn("avg_transaction_value",
                      F.round("avg_transaction_value", 2))
          .withColumn("_computed_at", F.current_timestamp())
          .orderBy(F.desc("total_revenue"))
    )
    return kpi


# -- KPI 2: Monthly Sales Growth -----------------------------------------------

def kpi_monthly_sales_growth(df):
    """
    Monthly aggregation with a LAG window to derive:
        prev_month_revenue - previous month's total revenue
        mom_growth_pct     - month-over-month % change
    """
    log.info("Computing KPI 2: Monthly Sales Growth...")

    monthly = (
        df.groupBy("year", "month")
          .agg(
              F.sum("amount")               .alias("total_revenue"),
              F.count("transaction_id")     .alias("transaction_count"),
              F.countDistinct("customer_id").alias("unique_customers"),
              F.avg("amount")               .alias("avg_transaction_value"),
          )
          .withColumn(
              "month_label",
              F.concat(F.col("year"), F.lit("-"), F.lpad(F.col("month"), 2, "0")),
          )
    )

    w = Window.orderBy("year", "month")

    kpi = (
        monthly
        .withColumn("prev_month_revenue",  F.lag("total_revenue", 1).over(w))
        .withColumn(
            "mom_growth_pct",
            F.when(
                F.col("prev_month_revenue").isNotNull()
                & (F.col("prev_month_revenue") > 0),
                F.round(
                    (F.col("total_revenue") - F.col("prev_month_revenue"))
                    / F.col("prev_month_revenue") * 100,
                    2,
                ),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("total_revenue",          F.round("total_revenue",          2))
        .withColumn("avg_transaction_value",   F.round("avg_transaction_value",  2))
        .withColumn("prev_month_revenue",      F.round("prev_month_revenue",     2))
        .withColumn("_computed_at", F.current_timestamp())
        .orderBy("year", "month")
    )
    return kpi


# -- KPI 3: Top 5 Regions by Volume -------------------------------------------

def kpi_top_regions(df):
    """
    Rank all regions by transaction count, return top 5.
    Includes volume_share_pct and revenue_share_pct for business context.
    """
    log.info("Computing KPI 3: Top 5 Regions by Volume...")

    total_txns = df.count()
    total_rev  = df.agg(F.sum("amount")).collect()[0][0]

    region_agg = (
        df.groupBy("region")
          .agg(
              F.count("transaction_id")     .alias("transaction_count"),
              F.sum("amount")               .alias("total_revenue"),
              F.avg("amount")               .alias("avg_revenue_per_txn"),
              F.countDistinct("customer_id").alias("unique_customers"),
          )
          .withColumn("volume_share_pct",
                      F.round(F.col("transaction_count") / total_txns * 100, 2))
          .withColumn("revenue_share_pct",
                      F.round(F.col("total_revenue") / total_rev * 100, 2))
          .withColumn("total_revenue",      F.round("total_revenue",      2))
          .withColumn("avg_revenue_per_txn",F.round("avg_revenue_per_txn",2))
    )

    rank_window = Window.orderBy(F.desc("transaction_count"))

    kpi = (
        region_agg
        .withColumn("rank", F.rank().over(rank_window))
        .filter(F.col("rank") <= 5)
        .withColumn("_computed_at", F.current_timestamp())
        .orderBy("rank")
    )
    return kpi


# -- Write KPI helper ----------------------------------------------------------

def write_kpi(df, name: str):
    """Write a KPI DataFrame as a single Parquet file to the Curated KPI path."""
    path = f"{CURATED_KPI_BASE}/{name}"
    log.info("Writing KPI '%s' -> %s  (%s rows)", name, path, df.count())
    (
        df.coalesce(1)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(path)
    )


# -- Summary logger ------------------------------------------------------------

def log_summary(kpi_cat, kpi_monthly, kpi_regions):
    log.info("\n%s\nKPI SUMMARY\n%s", "=" * 64, "=" * 64)

    log.info("\n[KPI 1] Revenue per Category")
    kpi_cat.select("product_category", "total_revenue", "revenue_pct").show(truncate=False)

    log.info("\n[KPI 2] Monthly Sales Growth (last 6 months)")
    (
        kpi_monthly
        .orderBy(F.desc("year"), F.desc("month"))
        .select("month_label", "total_revenue", "mom_growth_pct")
        .limit(6)
        .show(truncate=False)
    )

    log.info("\n[KPI 3] Top 5 Regions by Volume")
    kpi_regions.select("rank", "region", "transaction_count", "volume_share_pct").show(truncate=False)


# -- Main ----------------------------------------------------------------------

def main():
    log.info("=" * 64)
    log.info("JOB 2  -  Cleansed -> Curated + KPIs")
    log.info("  Source : %s", SOURCE_PATH)
    log.info("  Target : %s", CURATED_SALES_PATH)
    log.info("  KPIs   : %s", CURATED_KPI_BASE)
    log.info("=" * 64)

    # Read
    df, _ = read_cleansed(SOURCE_PATH)

    # Enrich with time partitions and cache for multiple downstream uses
    df_part = add_partitions(df)
    df_part.cache()
    df_part.count()   # materialise cache

    # Write fact table
    write_fact_table(df_part, CURATED_SALES_PATH)

    # Compute and write KPIs
    kpi_cat     = kpi_revenue_per_category(df_part)
    kpi_monthly = kpi_monthly_sales_growth(df_part)
    kpi_regions = kpi_top_regions(df_part)

    write_kpi(kpi_cat,     "revenue_per_category")
    write_kpi(kpi_monthly, "monthly_sales_growth")
    write_kpi(kpi_regions, "top_regions_by_volume")

    log_summary(kpi_cat, kpi_monthly, kpi_regions)

    df_part.unpersist()
    log.info("Job 2 finished successfully.")
    job.commit()


main()
