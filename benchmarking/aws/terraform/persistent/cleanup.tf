# Orphan-cleanup Lambda — runs every 15 minutes, destroys any
# Project=redpanda-connect-bench resource older than var.orphan_ttl_hours.
#
# Lives in persistent/ (NOT shared/) as of 2026-08-17: when it lived inside
# the session stack, an interrupted `terraform destroy` deleted the
# EventBridge rule FIRST (dependency order) and died before the expensive
# resources — disarming the safety net and then stranding a full stack for
# five days (~$240). The reaper must never share a lifecycle with what it
# guards. Requires cleanup-lambda/bootstrap.zip (`make zip`) at apply time.
#
# Tags are applied automatically via the provider's default_tags block in
# main.tf; no explicit tags block is needed on these resources.

resource "aws_sns_topic" "orphan_cleanup" {
  name = "redpanda-connect-bench-orphans"
}

resource "aws_iam_role" "orphan_cleanup" {
  name = "redpanda-connect-bench-orphan-cleanup"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "orphan_cleanup_basic" {
  role       = aws_iam_role.orphan_cleanup.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "orphan_cleanup" {
  name = "orphan-cleanup"
  role = aws_iam_role.orphan_cleanup.id
  # The destructive grants are fenced to Project=redpanda-connect-bench via
  # aws:ResourceTag wherever the action supports it, so IAM is a second line
  # of defence behind the Lambda's own tag filter. That code-side filter has
  # already failed once: the reaper deleted the persistent soak-archive
  # bucket when the persistent stack briefly carried the reapable Project
  # tag (fixed by retagging — exactly what this condition now enforces
  # structurally).
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Discovery + read-only lookups genuinely need account scope.
        Sid    = "Discover"
        Effect = "Allow"
        Action = [
          "tag:GetResources",
          "ec2:DescribeInstances", "ec2:DescribeVpcs", "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups", "ec2:DescribeRouteTables", "ec2:DescribeInternetGateways",
          "rds:DescribeDBInstances", "rds:DescribeDBSubnetGroups", "rds:DescribeDBParameterGroups",
          "s3:ListAllMyBuckets",
          "iam:GetRole", "iam:ListRolePolicies", "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole"
        ]
        Resource = "*"
      },
      {
        # Destructive EC2/RDS calls and role deletion only reach resources
        # carrying the bench Project tag — the persistent stack (distinct
        # Project tag) is structurally out of reach even if the Lambda's
        # own filter regresses.
        Sid    = "DestroyTagged"
        Effect = "Allow"
        Action = [
          "ec2:TerminateInstances",
          "ec2:DeleteVpc", "ec2:DeleteSubnet", "ec2:DeleteSecurityGroup",
          "ec2:DeleteRouteTable", "ec2:DisassociateRouteTable",
          "ec2:DetachInternetGateway", "ec2:DeleteInternetGateway",
          "rds:DeleteDBInstance", "rds:DeleteDBSubnetGroup", "rds:DeleteDBParameterGroup",
          "iam:DeleteRole", "iam:DeleteRolePolicy", "iam:DetachRolePolicy"
        ]
        Resource = "*"
        Condition = {
          StringEquals = { "aws:ResourceTag/Project" = "redpanda-connect-bench" }
        }
      },
      {
        # S3 bucket/object deletes and the instance-profile unwind don't
        # support aws:ResourceTag reliably, so they stay broad — but they
        # are the low-blast-radius remainder: the archive bucket's real
        # shield is its non-bench Project tag at the Lambda's discovery
        # layer, and instance profiles are only reachable through an
        # already-tag-fenced role deletion.
        Sid    = "DestroyUnconditionable"
        Effect = "Allow"
        Action = [
          "s3:ListBucket", "s3:ListBucketVersions",
          "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket",
          "iam:RemoveRoleFromInstanceProfile", "iam:DeleteInstanceProfile"
        ]
        Resource = "*"
      },
      {
        Sid      = "Alert"
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.orphan_cleanup.arn
      }
    ]
  })
}

resource "aws_lambda_function" "orphan_cleanup" {
  function_name    = "redpanda-connect-bench-orphan-cleanup"
  role             = aws_iam_role.orphan_cleanup.arn
  handler          = "bootstrap"
  runtime          = "provided.al2023"
  architectures    = ["arm64"]
  filename         = "${path.module}/../../cleanup-lambda/bootstrap.zip"
  source_code_hash = filebase64sha256("${path.module}/../../cleanup-lambda/bootstrap.zip")
  timeout          = 900 # 15 min — enough for slow RDS deletes

  environment {
    variables = {
      BENCH_ORPHAN_TTL_HOURS     = tostring(var.orphan_ttl_hours)
      BENCH_ORPHAN_SNS_TOPIC_ARN = aws_sns_topic.orphan_cleanup.arn
    }
  }
}

resource "aws_cloudwatch_event_rule" "orphan_cleanup" {
  name                = "redpanda-connect-bench-orphan-cleanup"
  description         = "Run the orphan-cleanup Lambda every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

resource "aws_cloudwatch_event_target" "orphan_cleanup" {
  rule = aws_cloudwatch_event_rule.orphan_cleanup.name
  arn  = aws_lambda_function.orphan_cleanup.arn
}

resource "aws_lambda_permission" "orphan_cleanup" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.orphan_cleanup.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.orphan_cleanup.arn
}
