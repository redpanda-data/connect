Benthos Lambda
==============

The `benthos-lambda` distribution is a version of Benthos specifically tailored
for deployment as an AWS Lambda function.

It uses the same configuration format as a regular Benthos instance, except the
`input` and `buffer` sections are ignored as messages are inserted via function
invocations.

If the `output` section is omitted in your config then the result of the
processing pipeline is returned back to the caller, otherwise the resulting data
is sent to the output destination.

## Upload to AWS

Grab an archive from the [releases page][releases] page and then create your
function:

``` sh
LAMBDA_ENV=`cat yourconfig.yaml | jq -csR {Variables:{BENTHOS_CONFIG:.}}`
aws lambda create-function \
  --runtime go1.x \
  --handler benthos-lambda \
  --role benthos-example-role \
  --zip-file fileb://benthos-lambda.zip \
  --environment "$LAMBDA_ENV" \
  --function-name benthos-example
```

## Invoke

``` sh
aws lambda invoke \
  --function-name benthos-example \
  --payload '{"your":"document"}' \
  out.txt && cat out.txt && rm out.txt
```

## Build

``` sh
go build github.com/Jeffail/benthos/cmd/serverless/benthos-lambda
zip benthos-lambda.zip benthos-lambda
```

[releases]: https://github.com/Jeffail/benthos/releases