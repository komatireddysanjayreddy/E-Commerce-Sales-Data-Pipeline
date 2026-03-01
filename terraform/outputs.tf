##############################################################
# outputs.tf  –  Exported resource references
##############################################################

# ── S3 ────────────────────────────────────────────────────────────────────────

output "s3_landing_bucket" {
  description = "S3 Landing Zone bucket name"
  value       = aws_s3_bucket.landing.id
}

output "s3_cleansed_bucket" {
  description = "S3 Cleansed Zone bucket name"
  value       = aws_s3_bucket.cleansed.id
}

output "s3_curated_bucket" {
  description = "S3 Curated Zone bucket name"
  value       = aws_s3_bucket.curated.id
}

output "s3_glue_scripts_bucket" {
  description = "S3 bucket containing Glue scripts and temp files"
  value       = aws_s3_bucket.glue_scripts.id
}

# ── Glue ─────────────────────────────────────────────────────────────────────

output "glue_database_name" {
  description = "Glue Data Catalog database name"
  value       = aws_glue_catalog_database.etl_db.name
}

output "glue_job_landing_to_cleansed" {
  description = "Name of the Landing → Cleansed Glue job"
  value       = aws_glue_job.landing_to_cleansed.name
}

output "glue_job_cleansed_to_curated" {
  description = "Name of the Cleansed → Curated Glue job"
  value       = aws_glue_job.cleansed_to_curated.name
}

output "glue_workflow_name" {
  description = "Name of the orchestration workflow"
  value       = aws_glue_workflow.etl_workflow.name
}

# ── IAM ───────────────────────────────────────────────────────────────────────

output "glue_role_arn" {
  description = "IAM role ARN assumed by Glue jobs"
  value       = aws_iam_role.glue_role.arn
}

output "redshift_role_arn" {
  description = "IAM role ARN assumed by Redshift for S3 COPY"
  value       = aws_iam_role.redshift_role.arn
}

# ── Redshift ──────────────────────────────────────────────────────────────────

output "redshift_endpoint" {
  description = "Redshift cluster endpoint (host:port)"
  value       = aws_redshift_cluster.warehouse.endpoint
  sensitive   = true
}

output "redshift_cluster_identifier" {
  description = "Redshift cluster identifier"
  value       = aws_redshift_cluster.warehouse.cluster_identifier
}
