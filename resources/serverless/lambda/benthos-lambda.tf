resource "aws_lambda_function" "benthos-lambda" {
  function_name = "benthos-lambda"
  role          = "${aws_iam_role.lambda-role.arn}"
  handler       = "benthos-lambda"
  runtime       = "go1.x"

  s3_bucket = "${var.bucket_name}"
  s3_key    = "benthos-lambda-${var.version}.zip"

  environment {
    variables = {
      LAMBDA_ENV = "${data.template_file.conf.rendered}"
    }
  }
}