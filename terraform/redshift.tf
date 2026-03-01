##############################################################
# redshift.tf  –  Redshift cluster, parameter group, security
##############################################################

# ── Security Group ────────────────────────────────────────────────────────────

resource "aws_security_group" "redshift" {
  name        = "${var.project_name}-redshift-sg-${var.environment}"
  description = "Allow Redshift access from within VPC"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "Redshift port – VPC only"
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = [data.aws_vpc.default.cidr_block]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# ── Subnet Group ──────────────────────────────────────────────────────────────

resource "aws_redshift_subnet_group" "main" {
  name        = "${var.project_name}-subnet-grp-${var.environment}"
  subnet_ids  = data.aws_subnets.default.ids
  description = "Redshift subnet group for the Sales ETL pipeline"
}

# ── Parameter Group ───────────────────────────────────────────────────────────

resource "aws_redshift_parameter_group" "main" {
  name   = "${var.project_name}-params-${var.environment}"
  family = "redshift-1.0"

  parameter {
    name  = "require_ssl"
    value = "true"
  }

  parameter {
    name  = "enable_user_activity_logging"
    value = "true"
  }

  # WLM: dedicated ETL queue (60 %) + default queue (40 %)
  parameter {
    name = "wlm_json_configuration"
    value = jsonencode([
      {
        name                  = "etl_queue"
        query_concurrency     = 5
        memory_percent_to_use = 60
        user_group            = ["etl_user"]
        queue_type            = "auto"
      },
      {
        name                  = "default_queue"
        query_concurrency     = 5
        memory_percent_to_use = 40
        queue_type            = "auto"
      }
    ])
  }
}

# ── Redshift Cluster ──────────────────────────────────────────────────────────

resource "aws_redshift_cluster" "warehouse" {
  cluster_identifier = "${var.project_name}-wh-${var.environment}"
  database_name      = var.redshift_db_name
  master_username    = var.redshift_master_username
  master_password    = var.redshift_master_password

  node_type       = var.redshift_node_type
  cluster_type    = var.redshift_num_nodes > 1 ? "multi-node" : "single-node"
  number_of_nodes = var.redshift_num_nodes > 1 ? var.redshift_num_nodes : null

  cluster_subnet_group_name    = aws_redshift_subnet_group.main.name
  cluster_parameter_group_name = aws_redshift_parameter_group.main.name
  vpc_security_group_ids       = [aws_security_group.redshift.id]

  # Attach the IAM role so the cluster can execute COPY from S3
  iam_roles = [aws_iam_role.redshift_role.arn]

  encrypted                           = true
  publicly_accessible                 = false
  automated_snapshot_retention_period = var.redshift_snapshot_retention_days
  preferred_maintenance_window        = "sun:05:00-sun:06:00"

  # Only create a final snapshot in prod
  skip_final_snapshot       = var.environment != "prod"
  final_snapshot_identifier = var.environment == "prod" ? "${var.project_name}-final-snap" : null

  logging {
    enable        = true
    bucket_name   = aws_s3_bucket.glue_scripts.id
    s3_key_prefix = "redshift-audit-logs/"
  }

  tags = {
    Name = "${var.project_name}-warehouse-${var.environment}"
  }

  depends_on = [aws_iam_role.redshift_role]
}
