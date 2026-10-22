output "lake_bucket" {
  value = aws_s3_bucket.lake.id
}

output "lake_uri" {
  value = "s3://${aws_s3_bucket.lake.id}"
}

output "pipeline_user" {
  value = aws_iam_user.pipeline.name
}
