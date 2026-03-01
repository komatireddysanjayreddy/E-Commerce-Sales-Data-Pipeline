"""
generate_sales_data.py
----------------------
Generates a 1M+ row synthetic sales dataset (CSV) with intentional
data-quality issues (nulls ~2%, duplicates ~0.5%) to stress-test the
Glue cleaning job.

Output columns:
    transaction_id  - UUID v4
    customer_id     - CUST_XXXXXX
    product_category
    amount          - log-normal, clipped to [1, 50000]
    timestamp       - random within 2022-01-01 to 2024-12-31
    region          - AWS region codes

Usage:
    pip install pandas numpy
    python generate_sales_data.py [--records 1200000] [--output sales_data.csv]
"""

import argparse
import os
import sys
import uuid
import random
import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

# Force UTF-8 output on Windows terminals (avoids CP1252 UnicodeEncodeError)
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------

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


# -- Generation ---------------------------------------------------------------

def generate(num_records: int, seed: int = 42) -> pd.DataFrame:
    np.random.seed(seed)
    random.seed(seed)

    log.info("Generating %s transaction_ids (UUID v4)...", f"{num_records:,}")
    transaction_ids = [str(uuid.uuid4()) for _ in range(num_records)]

    log.info("Generating customer_ids...")
    customer_ids = [f"CUST_{random.randint(1, 100_000):06d}" for _ in range(num_records)]

    log.info("Sampling categories and regions...")
    categories = np.random.choice(PRODUCT_CATEGORIES, num_records, p=CATEGORY_WEIGHTS)
    regions    = np.random.choice(REGIONS,             num_records, p=REGION_WEIGHTS)

    log.info("Generating amounts (log-normal)...")
    amounts = np.round(np.random.lognormal(mean=4.0, sigma=1.2, size=num_records), 2)
    amounts = np.clip(amounts, 1.0, 50_000.0)

    log.info("Generating timestamps...")
    random_secs = np.random.randint(0, DELTA_SECS, size=num_records)
    timestamps  = [START_DATE + timedelta(seconds=int(s)) for s in random_secs]

    df = pd.DataFrame({
        "transaction_id":   transaction_ids,
        "customer_id":      customer_ids,
        "product_category": categories,
        "amount":           amounts,
        "timestamp":        timestamps,
        "region":           regions,
    })

    # -- Inject data-quality issues ------------------------------------------

    # ~1 % null amounts
    null_amt_idx = np.random.choice(num_records, size=int(num_records * 0.01), replace=False)
    df.loc[null_amt_idx, "amount"] = None

    # ~1 % null customer_ids
    null_cust_idx = np.random.choice(num_records, size=int(num_records * 0.01), replace=False)
    df.loc[null_cust_idx, "customer_id"] = None

    # ~0.5 % duplicate rows (same transaction_id -> simulates upstream re-sends)
    n_dups = int(num_records * 0.005)
    dup_rows = df.sample(n=n_dups, random_state=seed)
    df = pd.concat([df, dup_rows], ignore_index=True)

    log.info(
        "Dataset ready: %s rows  |  nulls(amount)=%s  nulls(customer_id)=%s  duplicates~%s",
        f"{len(df):,}", f"{df['amount'].isna().sum():,}",
        f"{df['customer_id'].isna().sum():,}", f"{n_dups:,}",
    )
    return df


# -- Entry point --------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate synthetic sales CSV")
    parser.add_argument("--records", type=int,  default=1_200_000, help="Base record count (pre-duplicates)")
    parser.add_argument("--output",  type=str,  default="sales_data.csv")
    parser.add_argument("--seed",    type=int,  default=42)
    args = parser.parse_args()

    df = generate(args.records, seed=args.seed)

    log.info("Writing to %s...", args.output)
    df.to_csv(args.output, index=False)

    size_mb = os.path.getsize(args.output) / 1024 / 1024
    log.info("Done. File size: %.1f MB", size_mb)

    print("\n-- Sample (5 rows) " + "-" * 42)
    print(df.head().to_string())
    print("\n-- Null counts " + "-" * 46)
    print(df.isnull().sum().to_string())
    print("\n-- Category distribution " + "-" * 36)
    print(df["product_category"].value_counts().to_string())


if __name__ == "__main__":
    main()
