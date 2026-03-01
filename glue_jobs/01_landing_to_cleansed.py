"""
01_landing_to_cleansed.py
=========================
AWS Glue PySpark Job  -  Stage 1: Landing Zone -> Cleansed Zone

Responsibilities
----------------
1. Read raw CSV from S3 Landing zone
2. Enforce expected schema / cast types
3. Remove duplicates (on transaction_id)
4. Drop/impute nulls according to business rules
5. Validate domain values (category, region, amount range)
6. Attach audit columns for data lineage
7. Write output as Snappy-compressed Parquet to the Cleansed zone
8. Register (or update) the table in the Glue Data Catalog

Job Parameters (passed via Glue Job default_arguments)
-------------------------------------------------------
--SOURCE_BUCKET   : Landing zone bucket name
--TARGET_BUCKET   : Cleansed zone bucket name
--GLUE_DATABASE   : Glue catalog database name
--JOB_NAME        : (injected automatically by Glue)
"""

import sys
import logging

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType,
)

# -- Logging -------------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# -- Job initialisation --------------------------------------------------------

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "SOURCE_BUCKET", "TARGET_BUCKET", "GLUE_DATABASE"],
)

sc         = SparkContext()
glueCtx    = GlueContext(sc)
spark      = glueCtx.spark_session
job        = Job(glueCtx)
job.init(args["JOB_NAME"], args)

SOURCE_BUCKET = args["SOURCE_BUCKET"]
TARGET_BUCKET = args["TARGET_BUCKET"]
GLUE_DATABASE = args["GLUE_DATABASE"]

# -- Spark tuning --------------------------------------------------------------

spark.conf.set("spark.sql.adaptive.enabled",                         "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",      "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                "true")
spark.conf.set("spark.sql.parquet.compression.codec",                "snappy")
spark.conf.set("spark.sql.shuffle.partitions",                       "200")
spark.conf.set("spark.sql.sources.partitionOverwriteMode",           "dynamic")

# -- Constants -----------------------------------------------------------------

VALID_CATEGORIES = {
    "Electronics", "Clothing", "Books", "Home & Garden",
    "Sports", "Toys", "Automotive", "Food & Beverage",
    "Health & Beauty", "Office Supplies",
}

VALID_REGIONS = {
    "us-east-1", "us-west-2", "eu-west-1",
    "ap-southeast-1", "ap-northeast-1", "sa-east-1", "ca-central-1",
}

AMOUNT_MIN =     1.0
AMOUNT_MAX = 50_000.0

SOURCE_PATH = f"s3://{SOURCE_BUCKET}/raw/"
TARGET_PATH = f"s3://{TARGET_BUCKET}/sales/"

# -- Schema --------------------------------------------------------------------

EXPECTED_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),    nullable=False),
    StructField("customer_id",      StringType(),    nullable=True),
    StructField("product_category", StringType(),    nullable=True),
    StructField("amount",           DoubleType(),    nullable=True),
    StructField("timestamp",        TimestampType(), nullable=True),
    StructField("region",           StringType(),    nullable=True),
])


# -- Pipeline steps ------------------------------------------------------------

def read_raw_csv(path: str):
    """Read CSV from Landing zone with all-string schema for safe casting."""
    log.info("Reading raw CSV from: %s", path)
    df = (
        spark.read
        .option("header",        "true")
        .option("inferSchema",   "false")   # always false - we cast manually
        .option("multiLine",     "true")
        .option("escape",        '"')
        .option("mode",          "PERMISSIVE")  # bad rows -> _corrupt_record
        .csv(path)
    )
    raw_count = df.count()
    log.info("Raw records: %s", f"{raw_count:,}")
    return df, raw_count


def cast_schema(df):
    """
    Cast every column to its target type.
    Rows that cannot be parsed will produce nulls (caught in cleaning).
    """
    log.info("Casting schema...")
    return df.select(
        F.col("transaction_id").cast(StringType())                          .alias("transaction_id"),
        F.col("customer_id").cast(StringType())                             .alias("customer_id"),
        F.col("product_category").cast(StringType())                        .alias("product_category"),
        F.col("amount").cast(DoubleType())                                  .alias("amount"),
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss")          .alias("timestamp"),
        F.col("region").cast(StringType())                                  .alias("region"),
    )


def clean(df, raw_count: int):
    """
    Multi-step cleaning pipeline:

    Step 1  Drop rows with null primary key (transaction_id)
    Step 2  Deduplicate on transaction_id  (keep first occurrence)
    Step 3  Drop rows with null / out-of-range amount
    Step 4  Drop rows with null timestamp
    Step 5  Normalise product_category  (unknown -> 'UNKNOWN')
    Step 6  Normalise region            (unknown -> 'UNKNOWN')
    Step 7  Impute null customer_id     -> 'UNKNOWN'
    """
    stats = {"raw": raw_count}

    # Step 1 - PK null
    df = df.filter(F.col("transaction_id").isNotNull())
    stats["after_pk_null_drop"] = df.count()
    log.info("After PK null drop : %s (dropped %s)",
             f"{stats['after_pk_null_drop']:,}",
             f"{stats['raw'] - stats['after_pk_null_drop']:,}")

    # Step 2 - Deduplication
    df = df.dropDuplicates(["transaction_id"])
    stats["after_dedup"] = df.count()
    log.info("After deduplication: %s (dropped %s)",
             f"{stats['after_dedup']:,}",
             f"{stats['after_pk_null_drop'] - stats['after_dedup']:,}")

    # Step 3 - Amount validation
    df = df.filter(
        F.col("amount").isNotNull()
        & (F.col("amount") >= AMOUNT_MIN)
        & (F.col("amount") <= AMOUNT_MAX)
    )
    stats["after_amount"] = df.count()
    log.info("After amount filter: %s (dropped %s)",
             f"{stats['after_amount']:,}",
             f"{stats['after_dedup'] - stats['after_amount']:,}")

    # Step 4 - Timestamp null
    df = df.filter(F.col("timestamp").isNotNull())
    stats["after_ts"] = df.count()
    log.info("After ts filter    : %s (dropped %s)",
             f"{stats['after_ts']:,}",
             f"{stats['after_amount'] - stats['after_ts']:,}")

    # Step 5 - Category normalisation
    valid_cats = list(VALID_CATEGORIES)
    df = df.withColumn(
        "product_category",
        F.when(F.col("product_category").isin(valid_cats), F.col("product_category"))
         .otherwise(F.lit("UNKNOWN")),
    )

    # Step 6 - Region normalisation
    valid_regs = list(VALID_REGIONS)
    df = df.withColumn(
        "region",
        F.when(F.col("region").isin(valid_regs), F.col("region"))
         .otherwise(F.lit("UNKNOWN")),
    )

    # Step 7 - Impute null customer_id
    df = df.fillna({"customer_id": "UNKNOWN"})

    stats["final"] = df.count()
    log.info(
        "Cleaning complete: %s final records  (%.1f%% retained, %s dropped total)",
        f"{stats['final']:,}",
        stats["final"] / raw_count * 100,
        f"{raw_count - stats['final']:,}",
    )
    return df


def add_audit_columns(df):
    """Append metadata columns for lineage and debugging."""
    return (
        df
        .withColumn("_ingestion_ts",      F.current_timestamp())
        .withColumn("_source_system",      F.lit("sales_csv_landing"))
        .withColumn("_pipeline_version",   F.lit("1.0.0"))
    )


def write_cleansed(df, path: str):
    """
    Write to Cleansed zone as Snappy Parquet.
    50 output partitions ~ 20-40 MB each at 1 M rows.
    """
    log.info("Writing Parquet to: %s", path)
    (
        df.repartition(50)
          .write
          .mode("overwrite")
          .option("compression", "snappy")
          .parquet(path)
    )
    log.info("Write complete.")


def register_catalog(df, path: str):
    """Upsert the cleansed_sales table in the Glue Data Catalog."""
    log.info("Registering table in Glue catalog: %s.cleansed_sales", GLUE_DATABASE)
    dyf = DynamicFrame.fromDF(df, glueCtx, "cleansed_sales_dyf")
    glueCtx.write_dynamic_frame.from_catalog(
        frame=dyf,
        database=GLUE_DATABASE,
        table_name="cleansed_sales",
        transformation_ctx="write_cleansed_catalog",
        additional_options={
            "path":                path,
            "enableUpdateCatalog": True,
            "updateBehavior":      "UPDATE_IN_DATABASE",
        },
    )


# -- Main ----------------------------------------------------------------------

def main():
    log.info("=" * 64)
    log.info("JOB 1  -  Landing -> Cleansed")
    log.info("  Source : %s", SOURCE_PATH)
    log.info("  Target : %s", TARGET_PATH)
    log.info("=" * 64)

    df_raw, raw_count  = read_raw_csv(SOURCE_PATH)
    df_typed           = cast_schema(df_raw)
    df_clean           = clean(df_typed, raw_count)
    df_audited         = add_audit_columns(df_clean)

    write_cleansed(df_audited, TARGET_PATH)
    register_catalog(df_audited, TARGET_PATH)

    log.info("Job 1 finished successfully.")
    job.commit()


main()
