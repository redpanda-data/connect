# Orphan-cleanup Lambda — runs every 15 minutes, destroys any
# Project=redpanda-connect-bench resource older than var.orphan_ttl_hours.
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
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "tag:GetResources",
          "ec2:DescribeInstances", "ec2:TerminateInstances",
          "ec2:DescribeVpcs", "ec2:DeleteVpc",
          "ec2:DescribeSubnets", "ec2:DeleteSubnet",
          "ec2:DescribeSecurityGroups", "ec2:DeleteSecurityGroup",
          "ec2:DescribeRouteTables", "ec2:DeleteRouteTable", "ec2:DisassociateRouteTable",
          "ec2:DescribeInternetGateways", "ec2:DetachInternetGateway", "ec2:DeleteInternetGateway",
          "rds:DescribeDBInstances", "rds:DeleteDBInstance",
          "rds:DescribeDBSubnetGroups", "rds:DeleteDBSubnetGroup",
          "rds:DescribeDBParameterGroups", "rds:DeleteDBParameterGroup",
          "s3:ListAllMyBuckets", "s3:ListBucket", "s3:ListBucketVersions",
          "s3:DeleteObject", "s3:DeleteObjectVersion", "s3:DeleteBucket",
          "iam:GetRole", "iam:DeleteRole",
          "iam:ListRolePolicies", "iam:DeleteRolePolicy",
          "iam:ListAttachedRolePolicies", "iam:DetachRolePolicy",
          "iam:ListInstanceProfilesForRole", "iam:RemoveRoleFromInstanceProfile", "iam:DeleteInstanceProfile",
          "sns:Publish"
        ]
        Resource = "*"
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
