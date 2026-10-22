output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "lake_uri" {
  value = "s3://${aws_s3_bucket.lake.id}"
}

output "pipeline_user" {
  value = aws_iam_user.pipeline.name
}

output "glue_database" {
  value = aws_glue_catalog_database.gold.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}
