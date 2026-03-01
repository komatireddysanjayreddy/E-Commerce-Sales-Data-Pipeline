##############################################################
# iam.tf  –  IAM roles & policies for Glue and Redshift
##############################################################

# ── Glue IAM Role ─────────────────────────────────────────────────────────────

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}-glue-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowGlueAssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS-managed base policy (CloudWatch Logs, Glue Data Catalog, EC2 networking)
resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Inline policy: S3 read/write on all pipeline buckets
resource "aws_iam_role_policy" "glue_s3" {
  name = "GlueS3Access"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowBucketList"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [
          aws_s3_bucket.landing.arn,
          aws_s3_bucket.cleansed.arn,
          aws_s3_bucket.curated.arn,
          aws_s3_bucket.glue_scripts.arn,
        ]
      },
      {
        Sid    = "AllowObjectAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject",
          "s3:DeleteObject", "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
        ]
        Resource = [
          "${aws_s3_bucket.landing.arn}/*",
          "${aws_s3_bucket.cleansed.arn}/*",
          "${aws_s3_bucket.curated.arn}/*",
          "${aws_s3_bucket.glue_scripts.arn}/*",
        ]
      },
    ]
  })
}

# Inline policy: Glue Data Catalog full access (for crawler + job)
resource "aws_iam_role_policy" "glue_catalog" {
  name = "GlueCatalogAccess"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowGlueCatalog"
      Effect = "Allow"
      Action = [
        "glue:GetDatabase", "glue:GetDatabases",
        "glue:CreateTable", "glue:GetTable", "glue:GetTables",
        "glue:UpdateTable", "glue:DeleteTable",
        "glue:GetPartition", "glue:GetPartitions",
        "glue:CreatePartition", "glue:UpdatePartition",
        "glue:BatchCreatePartition", "glue:BatchGetPartition",
        "glue:DeletePartition",
      ]
      Resource = "*"
    }]
  })
}

# Inline policy: CloudWatch metrics (Spark UI / job metrics)
resource "aws_iam_role_policy" "glue_cloudwatch" {
  name = "GlueCloudWatchAccess"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowCloudWatch"
      Effect = "Allow"
      Action = [
        "cloudwatch:PutMetricData",
        "logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents",
        "logs:AssociateKmsKey",
      ]
      Resource = "*"
    }]
  })
}

# ── Redshift IAM Role ─────────────────────────────────────────────────────────

resource "aws_iam_role" "redshift_role" {
  name = "${var.project_name}-redshift-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "AllowRedshiftAssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Redshift needs read access to curated S3 bucket for COPY command
resource "aws_iam_role_policy" "redshift_s3_read" {
  name = "RedshiftS3CopyAccess"
  role = aws_iam_role.redshift_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowCuratedBucketList"
        Effect = "Allow"
        Action = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = [aws_s3_bucket.curated.arn]
      },
      {
        Sid    = "AllowCuratedObjectGet"
        Effect = "Allow"
        Action = ["s3:GetObject"]
        Resource = ["${aws_s3_bucket.curated.arn}/*"]
      },
      {
        Sid    = "AllowGlueCatalogRead"
        Effect = "Allow"
        Action = ["glue:GetDatabase", "glue:GetTable", "glue:GetPartitions"]
        Resource = "*"
      },
    ]
  })
}
