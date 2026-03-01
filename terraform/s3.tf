##############################################################
# s3.tf  –  S3 buckets: Landing / Cleansed / Curated / Scripts
#
# Naming pattern: <project>-<zone>-<env>-<account_id>
# Account-ID suffix guarantees global uniqueness without a random suffix.
##############################################################

locals {
  account_id = data.aws_caller_identity.current.account_id
  bucket_suffix = "${var.project_name}-${var.environment}-${local.account_id}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Helper: common encryption / versioning / public-block modules
# (applied individually below to keep the plan readable)
# ─────────────────────────────────────────────────────────────────────────────

# ── Landing Zone ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "landing" {
  bucket = "landing-${local.bucket_suffix}"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "landing" {
  bucket = aws_s3_bucket.landing.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "landing" {
  bucket                  = aws_s3_bucket.landing.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "landing" {
  bucket = aws_s3_bucket.landing.id

  rule {
    id     = "archive-raw-data"
    status = "Enabled"

    filter { prefix = "raw/" }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER_IR"
    }

    expiration {
      days = 365
    }
  }
}

# ── Cleansed Zone ─────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "cleansed" {
  bucket        = "cleansed-${local.bucket_suffix}"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "cleansed" {
  bucket = aws_s3_bucket.cleansed.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "cleansed" {
  bucket = aws_s3_bucket.cleansed.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "cleansed" {
  bucket                  = aws_s3_bucket.cleansed.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "cleansed" {
  bucket = aws_s3_bucket.cleansed.id

  rule {
    id     = "transition-cleansed"
    status = "Enabled"
    filter { prefix = "" }

    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
  }
}

# ── Curated Zone ──────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "curated" {
  bucket        = "curated-${local.bucket_suffix}"
  force_destroy = var.environment != "prod"
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "curated" {
  bucket                  = aws_s3_bucket.curated.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Glue Scripts & Temp ───────────────────────────────────────────────────────
resource "aws_s3_bucket" "glue_scripts" {
  bucket        = "glue-assets-${local.bucket_suffix}"
  force_destroy = true   # always safe to recreate scripts
}

resource "aws_s3_bucket_server_side_encryption_configuration" "glue_scripts" {
  bucket = aws_s3_bucket.glue_scripts.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "glue_scripts" {
  bucket                  = aws_s3_bucket.glue_scripts.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── Upload Glue job scripts from local disk ───────────────────────────────────
resource "aws_s3_object" "job_landing_to_cleansed" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/01_landing_to_cleansed.py"
  source = "${path.module}/../glue_jobs/01_landing_to_cleansed.py"
  etag   = filemd5("${path.module}/../glue_jobs/01_landing_to_cleansed.py")
}

resource "aws_s3_object" "job_cleansed_to_curated" {
  bucket = aws_s3_bucket.glue_scripts.id
  key    = "scripts/02_cleansed_to_curated_kpis.py"
  source = "${path.module}/../glue_jobs/02_cleansed_to_curated_kpis.py"
  etag   = filemd5("${path.module}/../glue_jobs/02_cleansed_to_curated_kpis.py")
}
