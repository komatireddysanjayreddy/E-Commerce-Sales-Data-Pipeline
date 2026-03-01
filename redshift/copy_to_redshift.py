"""
copy_to_redshift.py
===================
Loads Parquet files from the S3 Curated zone into Redshift
using the native COPY command (fastest bulk-load path).

Authentication
--------------
Redshift credentials are fetched from AWS Secrets Manager.
The cluster authenticates to S3 via an IAM role (no static keys needed).

Environment Variables Required
-------------------------------
REDSHIFT_SECRET_NAME   : Secrets Manager secret containing host/port/db/user/pass
REDSHIFT_IAM_ROLE_ARN  : IAM role ARN attached to the Redshift cluster
CURATED_BUCKET         : S3 bucket name of the Curated zone
AWS_REGION             : (optional) defaults to us-east-1

Secret JSON format
------------------
{
    "host":     "<cluster-endpoint>",
    "port":     5439,
    "database": "sales_dw",
    "username": "etl_admin",
    "password": "<secret>"
}

Usage
-----
    export REDSHIFT_SECRET_NAME=sales-etl-redshift-secret
    export REDSHIFT_IAM_ROLE_ARN=arn:aws:iam::123456789012:role/sales-etl-redshift-dev
    export CURATED_BUCKET=curated-sales-etl-dev-123456789012
    python redshift/copy_to_redshift.py
"""

import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import boto3
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger(__name__)


# -- Config --------------------------------------------------------------------

@dataclass
class LoadConfig:
    table:    str
    s3_path:  str
    truncate: bool = True


def build_load_configs(curated_bucket: str) -> List[LoadConfig]:
    base = f"s3://{curated_bucket}"
    return [
        LoadConfig(
            table   = "sales_dw.fact_sales",
            s3_path = f"{base}/sales/",
        ),
        LoadConfig(
            table   = "sales_dw.kpi_revenue_per_category",
            s3_path = f"{base}/kpis/revenue_per_category/",
        ),
        LoadConfig(
            table   = "sales_dw.kpi_monthly_sales_growth",
            s3_path = f"{base}/kpis/monthly_sales_growth/",
        ),
        LoadConfig(
            table   = "sales_dw.kpi_top_regions_by_volume",
            s3_path = f"{base}/kpis/top_regions_by_volume/",
        ),
    ]


# -- Secrets -------------------------------------------------------------------

def get_secret(secret_name: str, region: str) -> dict:
    """Retrieve and parse a JSON secret from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
    except client.exceptions.ResourceNotFoundException:
        log.error("Secret '%s' not found in region '%s'.", secret_name, region)
        raise
    return json.loads(response["SecretString"])


# -- Loader --------------------------------------------------------------------

class RedshiftLoader:
    """Encapsulates all Redshift bulk-load logic."""

    def __init__(
        self,
        host:           str,
        port:           int,
        database:       str,
        user:           str,
        password:       str,
        iam_role_arn:   str,
        aws_region:     str = "us-east-1",
    ):
        self.conn_params  = dict(
            host=host, port=port, database=database,
            user=user, password=password,
            connect_timeout=30, sslmode="require",
        )
        self.iam_role_arn = iam_role_arn
        self.aws_region   = aws_region
        self._conn: Optional[psycopg2.extensions.connection] = None

    # -- Connection management ----------------------------------------------

    def connect(self):
        log.info("Connecting to Redshift %s:%s/%s...",
                 self.conn_params["host"],
                 self.conn_params["port"],
                 self.conn_params["database"])
        self._conn = psycopg2.connect(**self.conn_params)
        self._conn.autocommit = False
        log.info("Connected.")

    def disconnect(self):
        if self._conn:
            self._conn.close()
            log.info("Connection closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *_):
        self.disconnect()

    # -- SQL helpers -------------------------------------------------------

    def _execute(self, sql: str, label: str = ""):
        cur = self._conn.cursor()
        try:
            log.info("SQL [%s]: %s", label, sql.strip()[:120])
            cur.execute(sql)
            self._conn.commit()
        except psycopg2.Error as exc:
            self._conn.rollback()
            log.error("SQL error [%s]: %s", label, exc)
            raise
        finally:
            cur.close()

    def _fetchone(self, sql: str):
        cur = self._conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        return row

    # -- Load operations ---------------------------------------------------

    def truncate(self, table: str):
        self._execute(f"TRUNCATE TABLE {table};", f"TRUNCATE {table}")

    def copy_parquet(self, s3_path: str, table: str):
        """
        COPY from S3 Parquet into Redshift using IAM role auth.

        FORMAT AS PARQUET       -> native Parquet support (no manifest needed)
        SERIALIZETOJSON         -> map complex types to JSON strings
        REGION                  -> required when cluster != bucket region
        """
        copy_sql = f"""
            COPY {table}
            FROM '{s3_path}'
            IAM_ROLE '{self.iam_role_arn}'
            FORMAT AS PARQUET
            SERIALIZETOJSON
            REGION '{self.aws_region}'
            COMPUPDATE OFF
            STATUPDATE OFF
        ;"""
        self._execute(copy_sql, f"COPY -> {table}")

    def analyze(self, table: str):
        """Refresh table statistics so the query planner stays accurate."""
        self._execute(f"ANALYZE {table};", f"ANALYZE {table}")

    def row_count(self, table: str) -> int:
        row = self._fetchone(f"SELECT COUNT(*) FROM {table};")
        return row[0] if row else -1

    def vacuum(self, table: str):
        """Reclaim space and re-sort after bulk load (run periodically)."""
        self._execute(f"VACUUM SORT ONLY {table};", f"VACUUM {table}")

    # -- Orchestration -----------------------------------------------------

    def load_all(self, configs: List[LoadConfig]) -> Dict[str, dict]:
        results: Dict[str, dict] = {}

        log.info("=" * 64)
        log.info("Starting Redshift load  -  %d tables", len(configs))
        log.info("=" * 64)

        for cfg in configs:
            table = cfg.table
            try:
                if cfg.truncate:
                    self.truncate(table)

                self.copy_parquet(cfg.s3_path, table)
                self.analyze(table)

                count = self.row_count(table)
                results[table] = {"status": "SUCCESS", "rows": count}
                log.info("[OK]     %-45s  %s rows", table, f"{count:,}")

            except Exception as exc:  # noqa: BLE001
                results[table] = {"status": "FAILED", "error": str(exc)}
                log.error("[FAILED] %-45s  %s", table, exc)

        # Summary
        log.info("\n%s\nLOAD SUMMARY\n%s", "=" * 64, "=" * 64)
        for tbl, res in results.items():
            status = res["status"]
            detail = f"{res.get('rows', 0):,} rows" if status == "SUCCESS" else res.get("error", "")
            log.info("  %-8s | %-45s | %s", status, tbl, detail)

        failed = [t for t, r in results.items() if r["status"] == "FAILED"]
        if failed:
            raise RuntimeError(f"Load failed for: {failed}")

        log.info("All tables loaded successfully.")
        return results


# -- Entry point ---------------------------------------------------------------

def main():
    AWS_REGION     = os.environ.get("AWS_REGION",            "us-east-1")
    SECRET_NAME    = os.environ.get("REDSHIFT_SECRET_NAME",  "")
    IAM_ROLE_ARN   = os.environ.get("REDSHIFT_IAM_ROLE_ARN", "")
    CURATED_BUCKET = os.environ.get("CURATED_BUCKET",        "")

    missing = [k for k, v in {
        "REDSHIFT_SECRET_NAME":  SECRET_NAME,
        "REDSHIFT_IAM_ROLE_ARN": IAM_ROLE_ARN,
        "CURATED_BUCKET":        CURATED_BUCKET,
    }.items() if not v]

    if missing:
        log.error("Missing required environment variables: %s", missing)
        sys.exit(1)

    secrets = get_secret(SECRET_NAME, AWS_REGION)
    configs = build_load_configs(CURATED_BUCKET)

    with RedshiftLoader(
        host         = secrets["host"],
        port         = int(secrets.get("port", 5439)),
        database     = secrets.get("database", "sales_dw"),
        user         = secrets["username"],
        password     = secrets["password"],
        iam_role_arn = IAM_ROLE_ARN,
        aws_region   = AWS_REGION,
    ) as loader:
        loader.load_all(configs)


if __name__ == "__main__":
    main()
