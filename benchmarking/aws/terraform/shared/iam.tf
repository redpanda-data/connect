data "aws_iam_policy_document" "ec2_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "bench_host" {
  name               = "${local.name_prefix}-host"
  assume_role_policy = data.aws_iam_policy_document.ec2_assume.json
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.bench_host.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Read secrets (for any future stack that uses them) + write to results bucket.
resource "aws_iam_role_policy" "bench_host_extra" {
  role = aws_iam_role.bench_host.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          # s3ResetScript wipes each engine's prefix between sweep points
          # (aws s3 rm --recursive) so the sidecar's byte-growth measurement
          # never carries over the previous point's objects.
          "s3:DeleteObject",
          # Required by the Aiven S3 Sink connector's multipart upload path
          # (see its README's Authorization section): AbortMultipartUpload
          # cleans up failed/aborted uploads, and the ListMultipartUploadParts/
          # ListBucketMultipartUploads pair lets it resume/verify in-progress
          # ones.
          "s3:AbortMultipartUpload",
          "s3:ListMultipartUploadParts",
          "s3:ListBucketMultipartUploads",
          "secretsmanager:GetSecretValue",
          "glue:*",
          # DynamoDB: source-table read/write for the seeder, Streams read for
          # the Connect input, CreateTable for the connector's auto-managed
          # checkpoint table. dynamodb:* covers the stream actions too
          # (DescribeStream/GetRecords/GetShardIterator/ListStreams all live
          # under the dynamodb: namespace).
          "dynamodb:*",
        ]
        Resource = ["*"]
      },
    ]
  })
}

resource "aws_iam_instance_profile" "bench_host" {
  name = "${local.name_prefix}-host"
  role = aws_iam_role.bench_host.name
}
