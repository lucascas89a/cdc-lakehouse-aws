# Read-only identity for the AI agent's tools. It can only run Athena queries in the lake
# workgroup; it gets S3 and Glue access exclusively *through* Athena (aws:CalledVia), so a
# leaked key cannot download lake files or browse the catalog directly. As with the pipeline,
# the access key is created with the CLI, never in Terraform state:
#   aws iam create-access-key --user-name cdc-lakehouse-agent
resource "aws_iam_user" "agent" {
  name = "cdc-lakehouse-agent"
  path = "/service/"
}

data "aws_iam_policy_document" "agent" {
  statement {
    sid = "RunQueriesInLakeWorkgroup"
    actions = [
      "athena:StartQueryExecution",
      "athena:GetQueryExecution",
      "athena:GetQueryResults",
      "athena:StopQueryExecution",
      "athena:GetWorkGroup",
    ]
    resources = [aws_athena_workgroup.lake.arn]
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
    sid     = "ReadGoldCatalogViaAthena"
    actions = ["glue:GetDatabase", "glue:GetTable", "glue:GetTables", "glue:GetPartitions"]
    resources = [
      "arn:aws:glue:${var.region}:${local.account_id}:catalog",
      aws_glue_catalog_database.gold.arn,
      "arn:aws:glue:${var.region}:${local.account_id}:table/${aws_glue_catalog_database.gold.name}/*",
    ]
    condition {
      test     = "ForAnyValue:StringEquals"
      variable = "aws:CalledVia"
      values   = ["athena.amazonaws.com"]
    }
  }

  statement {
    sid       = "ListGoldAndResultsViaAthena"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
    condition {
      test     = "ForAnyValue:StringEquals"
      variable = "aws:CalledVia"
      values   = ["athena.amazonaws.com"]
    }
  }

  statement {
    sid       = "ReadGoldViaAthena"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.lake.arn}/gold/*"]
    condition {
      test     = "ForAnyValue:StringEquals"
      variable = "aws:CalledVia"
      values   = ["athena.amazonaws.com"]
    }
  }

  statement {
    sid       = "WriteQueryResultsViaAthena"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"]
    resources = ["${aws_s3_bucket.lake.arn}/athena-results/*"]
    condition {
      test     = "ForAnyValue:StringEquals"
      variable = "aws:CalledVia"
      values   = ["athena.amazonaws.com"]
    }
  }

  # Belt and braces: never the unaudited staging area or the layers that still carry history/PII.
  statement {
    sid     = "DenyNonPublishedData"
    effect  = "Deny"
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.lake.arn}/gold/_staging/*",
      "${aws_s3_bucket.lake.arn}/silver/*",
      "${aws_s3_bucket.lake.arn}/bronze/*",
    ]
  }
}

resource "aws_iam_user_policy" "agent" {
  name   = "gold-read-only-via-athena"
  user   = aws_iam_user.agent.name
  policy = data.aws_iam_policy_document.agent.json
}

output "agent_user" {
  value = aws_iam_user.agent.name
}
