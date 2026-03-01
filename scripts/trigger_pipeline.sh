#!/usr/bin/env bash
# =============================================================================
# trigger_pipeline.sh
# =============================================================================
# End-to-end pipeline launcher:
#   1. Generate synthetic dataset  (optional –g flag)
#   2. Upload CSV to S3 Landing zone
#   3. Start the Glue Workflow
#   4. Poll until the workflow finishes
#   5. Create Redshift tables (idempotent DDL)
#   6. Load Parquet from S3 into Redshift
#
# Prerequisites:
#   - AWS CLI v2 configured (aws configure or instance role)
#   - Terraform outputs already applied in ./terraform/
#   - Python 3.9+ with boto3, psycopg2-binary installed
#
# Usage:
#   ./scripts/trigger_pipeline.sh [OPTIONS]
#
# Options:
#   -e ENV        Terraform workspace / environment  (default: dev)
#   -g            Re-generate the CSV before upload
#   -r RECORDS    Number of records to generate     (default: 1200000)
#   -h            Show this help message
# =============================================================================

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
ENV="dev"
GENERATE=false
RECORDS=1200000
POLL_INTERVAL=30    # seconds between workflow status checks
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"
DATA_FILE="${PROJECT_ROOT}/data_generation/sales_data.csv"
TF_DIR="${PROJECT_ROOT}/terraform"

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Args ──────────────────────────────────────────────────────────────────────
usage() {
  grep '^#' "$0" | sed 's/^# \{0,2\}//' | head -30
  exit 0
}

while getopts ":e:gr:h" opt; do
  case $opt in
    e) ENV="$OPTARG" ;;
    g) GENERATE=true ;;
    r) RECORDS="$OPTARG" ;;
    h) usage ;;
    *) error "Unknown option: -$OPTARG" ;;
  esac
done

# ── Banner ────────────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         AWS Sales ETL Pipeline  –  Trigger Script        ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo "  Environment : ${ENV}"
echo "  Generate    : ${GENERATE}  (records: ${RECORDS})"
echo ""

# ── Helpers ───────────────────────────────────────────────────────────────────
require_cmd() { command -v "$1" &>/dev/null || error "Required command not found: $1"; }
require_cmd aws
require_cmd terraform
require_cmd python3

# ── Derive names from Terraform outputs ───────────────────────────────────────
tf_output() { terraform -chdir="${TF_DIR}" output -raw "$1" 2>/dev/null; }

info "Reading Terraform outputs…"
LANDING_BUCKET=$(tf_output s3_landing_bucket)     || error "Run 'terraform apply' first."
CURATED_BUCKET=$(tf_output s3_curated_bucket)
WORKFLOW_NAME=$(tf_output glue_workflow_name)
REDSHIFT_ROLE=$(tf_output redshift_role_arn)
AWS_REGION="${AWS_REGION:-us-east-1}"

info "  Landing  : ${LANDING_BUCKET}"
info "  Curated  : ${CURATED_BUCKET}"
info "  Workflow : ${WORKFLOW_NAME}"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1  –  (Optional) Generate dataset
# ─────────────────────────────────────────────────────────────────────────────
if [[ "${GENERATE}" == "true" ]]; then
  info "[1/6] Generating synthetic dataset (${RECORDS} records)…"
  python3 "${PROJECT_ROOT}/data_generation/generate_sales_data.py" \
    --records "${RECORDS}" \
    --output  "${DATA_FILE}"
  success "Dataset written to ${DATA_FILE}"
else
  info "[1/6] Skipping data generation (-g not set)"
  [[ -f "${DATA_FILE}" ]] || error "Data file not found: ${DATA_FILE}\nRun with -g to generate it."
fi
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2  –  Upload CSV to S3 Landing zone
# ─────────────────────────────────────────────────────────────────────────────
info "[2/6] Uploading ${DATA_FILE} → s3://${LANDING_BUCKET}/raw/sales_data.csv"

FILE_SIZE=$(stat -f%z "${DATA_FILE}" 2>/dev/null || stat -c%s "${DATA_FILE}")
FILE_SIZE_MB=$(echo "scale=1; ${FILE_SIZE}/1048576" | bc)

aws s3 cp "${DATA_FILE}" "s3://${LANDING_BUCKET}/raw/sales_data.csv" \
  --region "${AWS_REGION}" \
  --no-progress

success "Uploaded (${FILE_SIZE_MB} MB)"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3  –  Start Glue Workflow
# ─────────────────────────────────────────────────────────────────────────────
info "[3/6] Starting Glue Workflow: ${WORKFLOW_NAME}…"

RUN_ID=$(aws glue start-workflow-run \
  --name      "${WORKFLOW_NAME}" \
  --region    "${AWS_REGION}"    \
  --query     'RunId'            \
  --output    text)

success "Workflow started  (RunId: ${RUN_ID})"
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4  –  Poll workflow until terminal state
# ─────────────────────────────────────────────────────────────────────────────
info "[4/6] Monitoring workflow execution (poll every ${POLL_INTERVAL}s)…"

while true; do
  STATUS=$(aws glue get-workflow-run \
    --name    "${WORKFLOW_NAME}" \
    --run-id  "${RUN_ID}"       \
    --region  "${AWS_REGION}"   \
    --query   'Run.Status'      \
    --output  text)

  TS=$(date '+%H:%M:%S')
  echo "  [${TS}]  Status: ${STATUS}"

  case "${STATUS}" in
    COMPLETED)
      success "Workflow completed successfully."
      break ;;
    STOPPED|ERROR|FAILED)
      error "Workflow ended with status: ${STATUS}\nCheck CloudWatch Logs for details." ;;
  esac

  sleep "${POLL_INTERVAL}"
done
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5  –  Create/verify Redshift tables (idempotent DDL)
# ─────────────────────────────────────────────────────────────────────────────
info "[5/6] Applying Redshift DDL (idempotent)…"

REDSHIFT_ENDPOINT=$(tf_output redshift_endpoint)
REDSHIFT_HOST=$(echo "${REDSHIFT_ENDPOINT}" | cut -d: -f1)
REDSHIFT_SECRET="${REDSHIFT_SECRET_NAME:-sales-etl-redshift-secret}"

# Retrieve password via Secrets Manager
REDSHIFT_PASS=$(aws secretsmanager get-secret-value \
  --secret-id "${REDSHIFT_SECRET}" \
  --region    "${AWS_REGION}"      \
  --query     'SecretString'       \
  --output    text | python3 -c "import sys,json; s=json.load(sys.stdin); print(s['password'])")

PGPASSWORD="${REDSHIFT_PASS}" psql \
  --host="${REDSHIFT_HOST}" \
  --port=5439               \
  --dbname="sales_dw"       \
  --username="etl_admin"    \
  --file="${PROJECT_ROOT}/redshift/create_tables.sql" \
  --no-password             \
  --set ON_ERROR_STOP=1     \
  2>&1 | tail -20

success "Redshift DDL applied."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6  –  Load Parquet into Redshift
# ─────────────────────────────────────────────────────────────────────────────
info "[6/6] Loading Parquet files into Redshift…"

export REDSHIFT_SECRET_NAME="${REDSHIFT_SECRET}"
export REDSHIFT_IAM_ROLE_ARN="${REDSHIFT_ROLE}"
export CURATED_BUCKET="${CURATED_BUCKET}"
export AWS_REGION="${AWS_REGION}"

python3 "${PROJECT_ROOT}/redshift/copy_to_redshift.py"

success "Redshift load complete."
echo ""

# ─────────────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════╗"
echo "║              Pipeline finished successfully!              ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "  Curated S3  : s3://${CURATED_BUCKET}/"
echo "  Redshift DW : ${REDSHIFT_HOST}:5439/sales_dw"
echo ""
