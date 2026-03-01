##############################################################
# main.tf  –  Root Terraform configuration
# Project : aws-sales-etl-pipeline
##############################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Remote state – create this bucket manually once, then enable the block.
  # backend "s3" {
  #   bucket  = "terraform-state-<account_id>"
  #   key     = "sales-etl-pipeline/terraform.tfstate"
  #   region  = "us-east-1"
  #   encrypt = true
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      CostCenter  = "DataEngineering"
    }
  }
}

# Retrieve current AWS account identity (used in unique bucket names)
data "aws_caller_identity" "current" {}

# Retrieve the default VPC (used by Redshift)
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}
