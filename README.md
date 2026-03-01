# AWS Sales ETL Pipeline

End-to-end scalable data pipeline processing **1 M+ sales records** on AWS, producing business KPIs stored in both S3 (Parquet) and Amazon Redshift.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          AWS Account                                 │
│                                                                      │
│  ┌──────────┐    ┌──────────────────────────────────────────────┐   │
│  │ Local /  │    │             AWS Glue Workflow                 │   │
│  │ upstream │    │                                              │   │
│  │  source  │    │  ┌─────────────────┐   ┌──────────────────┐ │   │
│  └────┬─────┘    │  │  Job 1          │   │  Job 2           │ │   │
│       │ CSV      │  │  Landing →      │──▶│  Cleansed →      │ │   │
│       ▼          │  │  Cleansed       │   │  Curated + KPIs  │ │   │
│  ┌──────────┐    │  │  (PySpark)      │   │  (PySpark)       │ │   │
│  │S3 Landing│───▶│  └─────────────────┘   └──────────────────┘ │   │
│  │  Zone    │    └──────────────────────────────────────────────┘   │
│  └──────────┘             │                        │                 │
│                           ▼                        ▼                 │
│                  ┌──────────────┐        ┌──────────────────┐       │
│                  │  S3 Cleansed │        │   S3 Curated     │       │
│                  │    Zone      │        │     Zone         │       │
│                  │  (Parquet)   │        │ (Parquet,        │       │
│                  └──────────────┘        │  partitioned     │       │
│                                          │  year/month/day) │       │
│                                          └────────┬─────────┘       │
│                                                   │ COPY             │
│                                                   ▼                 │
│                                          ┌──────────────────┐       │
│                                          │ Amazon Redshift  │       │
│                                          │  sales_dw schema │       │
│                                          │  + KPI tables    │       │
│                                          └──────────────────┘       │
│                                                                      │
│  ┌──────────────┐   ┌───────────────┐   ┌──────────────────────┐   │
│  │  Glue Crawlers│   │  Glue Catalog │   │  IAM Roles (least    │   │
│  │  (schema     │──▶│  (Data Catalog│   │  privilege)          │   │
│  │   discovery) │   │   Database)   │   └──────────────────────┘   │
│  └──────────────┘   └───────────────┘                               │
└──────────────────────────────────────────────────────────────────────┘
```

### Data Zones

| Zone | S3 Prefix | Format | Purpose |
|------|-----------|--------|---------|
| **Landing** | `raw/` | CSV | Raw ingestion, immutable |
| **Cleansed** | `sales/` | Snappy Parquet | Schema-enforced, deduped |
| **Curated** | `sales/year=*/month=*/day=*/` | Snappy Parquet | Partitioned fact table |
| **Curated KPIs** | `kpis/<name>/` | Snappy Parquet | Pre-aggregated KPI tables |

### KPIs Produced

| KPI | Table | Description |
|-----|-------|-------------|
| Revenue per Category | `kpi_revenue_per_category` | Total/avg revenue, unique customers, revenue % share per category |
| Monthly Sales Growth | `kpi_monthly_sales_growth` | MoM revenue change (%), trend direction |
| Top 5 Regions | `kpi_top_regions_by_volume` | Ranked by transaction count with volume/revenue share % |

---

## Project Structure

```
aws-sales-etl-pipeline/
├── README.md
├── data_generation/
│   └── generate_sales_data.py     # Synthetic 1M+ row CSV generator
├── terraform/
│   ├── main.tf                    # Provider, backend
│   ├── variables.tf               # All input variables
│   ├── outputs.tf                 # Exported resource names/ARNs
│   ├── s3.tf                      # 4 S3 buckets + lifecycle policies
│   ├── iam.tf                     # Glue & Redshift IAM roles
│   ├── glue.tf                    # Jobs, Crawlers, Workflow, Triggers
│   └── redshift.tf                # Cluster, subnet group, param group
├── glue_jobs/
│   ├── 01_landing_to_cleansed.py  # PySpark: clean + validate
│   └── 02_cleansed_to_curated_kpis.py  # PySpark: partition + KPIs
├── redshift/
│   ├── create_tables.sql          # DDL: fact + KPI tables + views
│   └── copy_to_redshift.py        # COPY orchestrator (Secrets Manager)
└── scripts/
    └── trigger_pipeline.sh        # Full end-to-end pipeline runner
```

---

## Prerequisites

| Tool | Minimum version |
|------|-----------------|
| Python | 3.9 |
| Terraform | 1.5.0 |
| AWS CLI | 2.x |
| psql | 14 (optional, for DDL step) |

Python dependencies:

```bash
pip install pandas numpy boto3 psycopg2-binary
```

---

## Setup & Deployment

### 1. Generate Synthetic Data

```bash
cd data_generation
python generate_sales_data.py --records 1200000 --output sales_data.csv
```

Options:
- `--records` – base row count before duplicate injection (default: 1 200 000)
- `--output`  – output file path (default: `sales_data.csv`)
- `--seed`    – random seed for reproducibility (default: 42)

### 2. Bootstrap Terraform State Bucket (one-time)

```bash
aws s3 mb s3://terraform-state-$(aws sts get-caller-identity --query Account --output text) \
    --region us-east-1
```

Then uncomment the `backend "s3"` block in [terraform/main.tf](terraform/main.tf) and update the bucket name.

### 3. Deploy Infrastructure

```bash
cd terraform

# Initialise providers and backend
terraform init

# Review the plan
terraform plan \
  -var="redshift_master_password=YourStr0ngPass!" \
  -var="environment=dev"

# Apply
terraform apply \
  -var="redshift_master_password=YourStr0ngPass!" \
  -var="environment=dev" \
  -auto-approve
```

> **Tip:** Store the password in a `terraform.tfvars` file (add it to `.gitignore`):
> ```hcl
> redshift_master_password = "YourStr0ngPass!"
> ```

### 4. Create Redshift Secret

```bash
aws secretsmanager create-secret \
  --name sales-etl-redshift-secret \
  --secret-string '{
    "host":     "<terraform output redshift_endpoint | cut -d: -f1>",
    "port":     5439,
    "database": "sales_dw",
    "username": "etl_admin",
    "password": "YourStr0ngPass!"
  }'
```

### 5. Apply Redshift DDL

```bash
PGPASSWORD="YourStr0ngPass!" psql \
  --host=$(terraform -chdir=terraform output -raw redshift_endpoint | cut -d: -f1) \
  --port=5439 \
  --dbname=sales_dw \
  --username=etl_admin \
  --file=redshift/create_tables.sql
```

---

## Running the Pipeline

### Option A – Full automated run (recommended)

```bash
chmod +x scripts/trigger_pipeline.sh

# Generate data, upload, run Glue, load Redshift
./scripts/trigger_pipeline.sh -g -e dev
```

Flags:
| Flag | Description |
|------|-------------|
| `-e ENV` | Environment name (must match Terraform workspace) |
| `-g` | Regenerate CSV before upload |
| `-r N` | Number of records to generate |

### Option B – Step-by-step

```bash
# 1. Upload data
aws s3 cp data_generation/sales_data.csv \
    s3://$(terraform -chdir=terraform output -raw s3_landing_bucket)/raw/sales_data.csv

# 2. Trigger Glue workflow
aws glue start-workflow-run \
    --name $(terraform -chdir=terraform output -raw glue_workflow_name)

# 3. Monitor
aws glue get-workflow-run \
    --name <workflow-name> \
    --run-id <run-id>

# 4. Load Redshift
export REDSHIFT_SECRET_NAME=sales-etl-redshift-secret
export REDSHIFT_IAM_ROLE_ARN=$(terraform -chdir=terraform output -raw redshift_role_arn)
export CURATED_BUCKET=$(terraform -chdir=terraform output -raw s3_curated_bucket)
python redshift/copy_to_redshift.py
```

---

## Glue Job Configuration

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `glue_version` | `4.0` | Spark 3.3, Python 3.10 |
| `worker_type` | `G.2X` | 8 vCPU / 32 GB per worker – handles 1 M+ rows without spill |
| `num_workers` | `10` | ~320 GB total executor memory |
| `job_bookmark` | enabled | Incremental processing on re-runs |
| Spark AQE | enabled | Auto-coalesce partitions, skew join handling |
| Output compression | Snappy | Best balance of speed vs. size for Redshift COPY |

Change defaults in [terraform/variables.tf](terraform/variables.tf):

```hcl
variable "glue_worker_type"  { default = "G.2X" }
variable "glue_num_workers"  { default = 10 }
```

---

## Data Quality Rules

| Column | Rule | Action on failure |
|--------|------|-------------------|
| `transaction_id` | Not null | Drop row |
| `transaction_id` | Unique | Drop duplicate |
| `amount` | `[1.0, 50000.0]` and not null | Drop row |
| `timestamp` | Not null, parseable | Drop row |
| `customer_id` | - | Impute `UNKNOWN` |
| `product_category` | Must be in allowed list | Replace with `UNKNOWN` |
| `region` | Must be in allowed list | Replace with `UNKNOWN` |

---

## Redshift Schema

```
sales_dw
├── fact_sales                  DISTKEY(region) SORTKEY(ts, category)
├── kpi_revenue_per_category    DISTSTYLE ALL
├── kpi_monthly_sales_growth    DISTSTYLE ALL
├── kpi_top_regions_by_volume   DISTSTYLE ALL
├── v_sales_summary             (view)
├── v_category_performance      (view)
└── v_growth_trend              (view)
```

---

## Scheduling

The Glue Workflow includes a **daily trigger at 02:00 UTC** that is disabled by default in `dev`. Enable it in production:

```hcl
# terraform/glue.tf
resource "aws_glue_trigger" "daily_schedule" {
  enabled = var.environment == "prod"
}
```

Or enable manually via the console / CLI:

```bash
aws glue update-trigger \
  --name sales-etl-daily-prod \
  --trigger-update '{"Enabled": true}'
```

---

## Cost Estimate (dev, single run)

| Service | Unit | Approx cost |
|---------|------|-------------|
| Glue G.2X × 10 workers × ~15 min | DPU-hours | ~$0.88 |
| S3 storage (3 buckets, ~500 MB) | GB-month | ~$0.01 |
| Redshift dc2.large × 2 nodes × 8 h | node-hours | ~$1.10 |
| **Total** | | **~$2.00 / run** |

---

## Teardown

```bash
terraform destroy \
  -var="redshift_master_password=YourStr0ngPass!" \
  -var="environment=dev"
```

> Redshift creates a final snapshot in prod before destroy. S3 buckets with `force_destroy = true` are deleted in non-prod environments.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Glue job `FAILED` immediately | Script not found in S3 | Re-run `terraform apply` to re-upload scripts |
| `COPY` returns `S3ServiceException` | Redshift role missing S3 permission | Verify `aws_iam_role_policy.redshift_s3_read` is applied |
| Redshift connection refused | Security group / VPC | Ensure your client IP is inside the VPC CIDR or add a bastion |
| Parquet schema mismatch | Column added to Glue output | Re-run DDL script with `DROP TABLE … CASCADE` |
| Low Glue performance | Under-provisioned workers | Increase `glue_num_workers` or switch to `G.4X` |

---

## Security Notes

- All S3 buckets block public access and use AES-256 server-side encryption.
- Redshift enforces SSL (`require_ssl = true`) and logs all user activity.
- IAM roles follow least-privilege (no `*` resource on write actions).
- Credentials are stored in **AWS Secrets Manager**, never in code or Terraform state.
- Terraform state should be stored in an encrypted S3 backend with DynamoDB locking.
