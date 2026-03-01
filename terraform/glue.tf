##############################################################
# glue.tf  –  Glue Data Catalog, Jobs, Crawlers, Workflow
##############################################################

# ── Data Catalog Database ─────────────────────────────────────────────────────

resource "aws_glue_catalog_database" "etl_db" {
  name        = "${replace(var.project_name, "-", "_")}_${var.environment}"
  description = "Sales ETL pipeline – managed by Terraform"
}

# ── Common job arguments (shared between both jobs) ───────────────────────────

locals {
  common_glue_args = {
    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.glue_scripts.id}/spark-logs/"
    "--TempDir"                          = "s3://${aws_s3_bucket.glue_scripts.id}/tmp/"
    "--GLUE_DATABASE"                    = aws_glue_catalog_database.etl_db.name
    "--enable-glue-datacatalog"          = "true"
    # Adaptive Query Execution (Spark 3.x)
    "--conf" = "spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true"
  }
}

# ── Job 1: Landing → Cleansed ─────────────────────────────────────────────────

resource "aws_glue_job" "landing_to_cleansed" {
  name              = "${var.project_name}-01-landing-cleansed-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_job_timeout_mins
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.id}/scripts/01_landing_to_cleansed.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--SOURCE_BUCKET" = aws_s3_bucket.landing.id
    "--TARGET_BUCKET" = aws_s3_bucket.cleansed.id
  })

  execution_property {
    max_concurrent_runs = 1
  }

  # Notify on job failure via EventBridge (wired in glue_workflow below)
  tags = {
    Pipeline = "sales-etl"
    Stage    = "landing-to-cleansed"
  }

  depends_on = [
    aws_s3_object.job_landing_to_cleansed,
    aws_iam_role_policy_attachment.glue_service_policy,
  ]
}

# ── Job 2: Cleansed → Curated + KPIs ─────────────────────────────────────────

resource "aws_glue_job" "cleansed_to_curated" {
  name              = "${var.project_name}-02-cleansed-curated-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = var.glue_version
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_num_workers
  timeout           = var.glue_job_timeout_mins
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.glue_scripts.id}/scripts/02_cleansed_to_curated_kpis.py"
    python_version  = "3"
  }

  default_arguments = merge(local.common_glue_args, {
    "--SOURCE_BUCKET" = aws_s3_bucket.cleansed.id
    "--TARGET_BUCKET" = aws_s3_bucket.curated.id
  })

  execution_property {
    max_concurrent_runs = 1
  }

  tags = {
    Pipeline = "sales-etl"
    Stage    = "cleansed-to-curated"
  }

  depends_on = [
    aws_s3_object.job_cleansed_to_curated,
    aws_iam_role_policy_attachment.glue_service_policy,
  ]
}

# ── Crawlers ──────────────────────────────────────────────────────────────────

resource "aws_glue_crawler" "landing_crawler" {
  name          = "${var.project_name}-crawler-landing-${var.environment}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.etl_db.name
  description   = "Discovers CSV schema in the Landing zone"

  s3_target {
    path = "s3://${aws_s3_bucket.landing.id}/raw/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy     = "CombineCompatibleSchemas"
      TableLevelConfiguration = 3
    }
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })
}

resource "aws_glue_crawler" "curated_crawler" {
  name          = "${var.project_name}-crawler-curated-${var.environment}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.etl_db.name
  description   = "Discovers Parquet partitions in the Curated zone"

  s3_target {
    path = "s3://${aws_s3_bucket.curated.id}/sales/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.curated.id}/kpis/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }
  })
}

# ── Orchestration Workflow ────────────────────────────────────────────────────

resource "aws_glue_workflow" "etl_workflow" {
  name        = "${var.project_name}-workflow-${var.environment}"
  description = "End-to-end Sales ETL: Landing → Cleansed → Curated + KPIs"

  max_concurrent_runs = 1
}

# Trigger 1: On-demand kick-off (used by the trigger_pipeline.sh script)
resource "aws_glue_trigger" "start_on_demand" {
  name          = "${var.project_name}-start-${var.environment}"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.etl_workflow.name

  actions {
    job_name = aws_glue_job.landing_to_cleansed.name
  }
}

# Trigger 2: Conditional – when Job 1 SUCCEEDS, fire Job 2
resource "aws_glue_trigger" "after_cleanse" {
  name          = "${var.project_name}-after-cleanse-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.etl_workflow.name

  predicate {
    logical = "AND"

    conditions {
      job_name         = aws_glue_job.landing_to_cleansed.name
      state            = "SUCCEEDED"
      logical_operator = "EQUALS"
    }
  }

  actions {
    job_name = aws_glue_job.cleansed_to_curated.name
  }
}

# Trigger 3: Scheduled – runs daily at 02:00 UTC (disabled by default in dev)
resource "aws_glue_trigger" "daily_schedule" {
  name          = "${var.project_name}-daily-${var.environment}"
  type          = "SCHEDULED"
  schedule      = "cron(0 2 * * ? *)"
  workflow_name = aws_glue_workflow.etl_workflow.name
  enabled       = var.environment == "prod"

  actions {
    job_name = aws_glue_job.landing_to_cleansed.name
  }
}
