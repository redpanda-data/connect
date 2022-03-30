---
title: Lambda
description: Deploying Benthos as an AWS Lambda function
---

The `benthos-lambda` distribution is a version of Benthos specifically tailored
for deployment as an AWS Lambda function on the `go1.x` runtime,
which runs Amazon Linux on the `x86_64` architecture.
The `benthos-lambda-al2` distribution supports the `provided.al2` runtime,
which runs Amazon Linux 2 on either the `x86_64` or `arm64` architecture.

It uses the same configuration format as a regular Benthos instance, which can be
provided in 1 of 2 ways:

1. Inline via the `BENTHOS_CONFIG` environment variable (YAML format).
2. Via the filesystem using a layer, extension, or container image. By default,
   the `benthos-lambda` distribution will look for a valid configuration file in
   the locations listed below. Alternatively, the configuration file path can be
   set explicity by passing a `BENTHOS_CONFIG_PATH` environment variable.
  - `./benthos.yaml`
  - `./config.yaml`
  - `/benthos.yaml`
  - `/etc/benthos/config.yaml`
  - `/etc/benthos.yaml`

Also, the `http`, `input` and `buffer` sections are ignored as the service wide
HTTP server is not used, and messages are inserted via function invocations.

If the `output` section is omitted in your config then the result of the
processing pipeline is returned back to the caller, otherwise the resulting data
is sent to the output destination.

### Running with an output

The flow of a Benthos lambda function with an output configured looks like this:

```text
                    benthos-lambda
           +------------------------------+
           |                              |
       -------> Processors ----> Output -----> Somewhere
invoke     |                              |        |
       <-------------------------------------------/
           |         <Ack/Noack>          |
           |                              |
           +------------------------------+
```

Where the call will block until the output target has confirmed receipt of the
resulting payload. When the message is successfully propagated a JSON payload is
returned of the form `{"message":"request successful"}`, otherwise an error is
returned containing the reason for the failure.

### Running without an output

The flow when an output is not configured looks like this:

```text
               benthos-lambda
           +--------------------+
           |                    |
       -------> Processors --\  |
invoke     |                 |  |
       <---------------------/  |
           |     <Result>       |
           |                    |
           +--------------------+
```

Where the function returns the result of processing directly back to the caller.
The format of the result differs depending on the number of batches and messages
of a batch that resulted from the invocation:

- Single message of a single batch: `{}` (JSON object)
- Multiple messages of a single batch: `[{},{}]` (Array of JSON objects)
- Multiple batches: `[[{},{}],[{}]]` (Array of arrays of JSON objects, batches
  of size one are a single object array in this case)

#### Processing Errors

The default behaviour of a Benthos lambda is that the handler will not return an
error unless the output fails. This means that errors that occur within your
processors will not result in the handler failing, which will instead return the
final state of the message.

In the next major version release (V4) this will change and the handler will
fail if messages have encountered an uncaught error during execution. However,
in the meantime it is possible to configure your output to use the new
[`reject` output][output.reject] in order to trigger a handler error on
processor errors:

```yaml
output:
  switch:
    retry_until_success: false
    cases:
      - check: '!errored()'
        output:
          sync_response: {}
      - output:
          reject: "processing failed due to: ${! error() }"
```

### Running a combination

It's possible to configure pipelines that send messages to third party
destinations and also return a result back to the caller. This is done by
configuring an output block and including an output of the type
`sync_response`.

For example, if we wished for our lambda function to send a payload to Kafka
and also return the same payload back to the caller we could use a
[broker][output-broker]:

```yml
output:
  broker:
    pattern: fan_out
    outputs:
    - kafka:
        addresses:
        - todo:9092
        client_id: benthos_serverless
        topic: example_topic
    - sync_response: {}
```

## Upload to AWS

### go1.x on x86_64

Grab an archive labelled `benthos-lambda` from the [releases page][releases]
page and then create your function:

```sh
LAMBDA_ENV=`cat yourconfig.yaml | jq -csR {Variables:{BENTHOS_CONFIG:.}}`
aws lambda create-function \
  --runtime go1.x \
  --handler benthos-lambda \
  --role benthos-example-role \
  --zip-file fileb://benthos-lambda.zip \
  --environment "$LAMBDA_ENV" \
  --function-name benthos-example
```

There is also an example [SAM template][sam-template] and
[Terraform resource][tf-example] in the repo to copy from.

### provided.al2 on amd64

Grab an archive labelled `benthos-lambda-al2` for `arm64` from the [releases page][releases]
page and then create your function (AWS CLI v2 only):

```sh
LAMBDA_ENV=`cat yourconfig.yaml | jq -csR {Variables:{BENTHOS_CONFIG:.}}`
aws lambda create-function \
  --runtime provided.al2 \
  --architectures arm64 \
  --handler not.used.for.provided.al2.runtime \
  --role benthos-example-role \
  --zip-file fileb://benthos-lambda.zip \
  --environment "$LAMBDA_ENV" \
  --function-name benthos-example
```

There is also an example [SAM template][sam-template-al2] and
[Terraform resource][tf-example-al2] in the repo to copy from.

Note that you can also run `benthos-lambda-al2` on x86_64, just use the `amd64` zip instead.

## Invoke

```sh
aws lambda invoke \
  --function-name benthos-example \
  --payload '{"your":"document"}' \
  out.txt && cat out.txt && rm out.txt
```

## Build

You can build and archive the function yourself with:

```sh
go build github.com/benthosdev/benthos/v4/cmd/serverless/benthos-lambda
zip benthos-lambda.zip benthos-lambda
```

[releases]: https://github.com/benthosdev/benthos/releases
[sam-template]: https://github.com/benthosdev/benthos/tree/main/resources/serverless/lambda/benthos-lambda-sam.yaml
[tf-example]: https://github.com/benthosdev/benthos/tree/main/resources/serverless/lambda/benthos-lambda.tf
[sam-template-al2]: https://github.com/benthosdev/benthos/tree/main/resources/serverless/lambda/benthos-lambda-al2-sam.yaml
[tf-example-al2]: https://github.com/benthosdev/benthos/tree/main/resources/serverless/lambda/benthos-lambda-al2.tf
[output-broker]: /docs/components/outputs/broker
[output.reject]: /docs/components/outputs/reject
