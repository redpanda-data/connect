resource "aws_lambda_function" "benthos-lambda" {
  function_name = "benthos-lambda"
  role          = "${aws_iam_role.lambda-role.arn}"
  handler       = "not.used.for.provided.al2.runtime"
  runtime       = "provided.al2"
  architectures = ["arm64"]

  s3_bucket = "${var.bucket_name}"
  s3_key    = "benthos-lambda-${var.version}.zip"

  environment {
    variables = {
      LAMBDA_ENV = "${data.template_file.conf.rendered}"
    }
  }
}
