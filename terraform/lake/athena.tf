# Serving layer: gold Delta tables registered in the Glue Data Catalog and queried with Athena.

resource "aws_glue_catalog_database" "gold" {
  name        = "shopflow_gold"
  description = "Business-ready ShopFlow tables (Delta Lake on S3). No PII."
}

# Tables are registered with Athena DDL by scripts/register-gold-tables.sh, not here: Athena copies
# the columns from the Delta transaction log at CREATE time, while a Terraform-managed column list
# would have to be kept in sync by hand every time a gold schema changes.

resource "aws_athena_workgroup" "lake" {
  name          = "cdc-lakehouse"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = 1073741824 # 1 GB: a runaway query cannot get expensive

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${aws_s3_bucket.lake.id}/athena-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
