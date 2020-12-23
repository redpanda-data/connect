---
title: Lambda
description: Deploying Benthos as an AWS Lambda function
---

The `benthos-lambda` distribution is a version of Benthos specifically tailored
for deployment as an AWS Lambda function.

It uses the same configuration format as a regular Benthos instance, except it
is read from the environment variable `BENTHOS_CONFIG` (YAML format). Also, the
`http`, `input` and `buffer` sections are ignored as the service wide HTTP
server is not used, and messages are inserted via function invocations.

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
    cases:
      - check: '!errored()'
        output:
          type: sync_response
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
    - type: sync_response
```

## Upload to AWS

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
go build github.com/Jeffail/benthos/v3/cmd/serverless/benthos-lambda
zip benthos-lambda.zip benthos-lambda
```

[releases]: https://github.com/Jeffail/benthos/releases
[sam-template]: https://github.com/Jeffail/benthos/tree/master/resources/serverless/lambda/benthos-lambda-sam.yaml
[tf-example]: https://github.com/Jeffail/benthos/tree/master/resources/serverless/lambda/benthos-lambda.tf
[output-broker]: /docs/components/outputs/broker
[output.reject]: /docs/components/outputs/reject
