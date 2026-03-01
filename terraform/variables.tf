##############################################################
# variables.tf  –  Input variables
##############################################################

variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix applied to every resource name"
  type        = string
  default     = "sales-etl"
}

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

# ── Glue ─────────────────────────────────────────────────────────────────────

variable "glue_version" {
  description = "AWS Glue version (4.0 recommended for Spark 3.3)"
  type        = string
  default     = "4.0"
}

variable "glue_worker_type" {
  description = "Glue worker type.  G.1X = 4 vCPU / 16 GB  |  G.2X = 8 vCPU / 32 GB"
  type        = string
  default     = "G.2X"

  validation {
    condition     = contains(["G.025X", "G.1X", "G.2X", "G.4X", "G.8X"], var.glue_worker_type)
    error_message = "glue_worker_type must be one of: G.025X, G.1X, G.2X, G.4X, G.8X."
  }
}

variable "glue_num_workers" {
  description = "Number of Glue workers (min 2 for G.2X)"
  type        = number
  default     = 10
}

variable "glue_job_timeout_mins" {
  description = "Maximum job duration in minutes before auto-termination"
  type        = number
  default     = 120
}

# ── Redshift ──────────────────────────────────────────────────────────────────

variable "redshift_node_type" {
  description = "Redshift node type"
  type        = string
  default     = "dc2.large"
}

variable "redshift_num_nodes" {
  description = "Number of Redshift nodes (>= 2 for multi-node)"
  type        = number
  default     = 2
}

variable "redshift_db_name" {
  description = "Redshift database name"
  type        = string
  default     = "sales_dw"
}

variable "redshift_master_username" {
  description = "Redshift master username"
  type        = string
  default     = "etl_admin"
  sensitive   = true
}

variable "redshift_master_password" {
  description = "Redshift master password (min 8 chars, upper+lower+digit)"
  type        = string
  sensitive   = true
}

variable "redshift_snapshot_retention_days" {
  description = "Automated snapshot retention period (0 disables snapshots)"
  type        = number
  default     = 7
}
