terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.38"
    }

    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.8"
    }
  }

}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Purpose     = "kdu data engineer project"
      Project     = var.project_name
      Environment = var.environment
    }
  }
}
