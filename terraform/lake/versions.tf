terraform {
  required_version = ">= 1.10"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.68.0"
    }
  }

  # Bucket name comes from backend.hcl (see backend.hcl.example), so the account id
  # is not hardcoded in the repository. Native S3 locking, no DynamoDB table needed.
  backend "s3" {
    key          = "lake/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = { project = "cdc-lakehouse-aws", managed_by = "terraform", stack = "lake" }
  }
}
