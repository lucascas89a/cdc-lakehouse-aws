# Identity used by the Spark jobs running outside AWS (Docker on a VM).
# It can only touch the lake prefixes of one bucket. The access key is deliberately NOT
# managed here, so the secret never ends up in Terraform state; create and rotate it with:
#   aws iam create-access-key --user-name cdc-lakehouse-pipeline
resource "aws_iam_user" "pipeline" {
  name = "cdc-lakehouse-pipeline"
  path = "/service/"
}

data "aws_iam_policy_document" "pipeline" {
  statement {
    sid       = "ListLakePrefixes"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = flatten([for p in local.lake_prefixes : [p, "${p}/*"]])
    }
    # Optional source-IP pinning: a leaked access key is useless outside these CIDRs.
    # Empty list (default) = no condition, so a fresh clone still works anywhere.
    dynamic "condition" {
      for_each = length(var.pipeline_allowed_cidrs) > 0 ? [1] : []
      content {
        test     = "IpAddress"
        variable = "aws:SourceIp"
        values   = var.pipeline_allowed_cidrs
      }
    }
  }

  statement {
    sid       = "ReadWriteLakeObjects"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = [for p in local.lake_prefixes : "${aws_s3_bucket.lake.arn}/${p}/*"]
    # Optional source-IP pinning: a leaked access key is useless outside these CIDRs.
    # Empty list (default) = no condition, so a fresh clone still works anywhere.
    dynamic "condition" {
      for_each = length(var.pipeline_allowed_cidrs) > 0 ? [1] : []
      content {
        test     = "IpAddress"
        variable = "aws:SourceIp"
        values   = var.pipeline_allowed_cidrs
      }
    }
  }
}

resource "aws_iam_user_policy" "pipeline" {
  name   = "lake-read-write"
  user   = aws_iam_user.pipeline.name
  policy = data.aws_iam_policy_document.pipeline.json
}
