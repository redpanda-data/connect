Dynamic Inputs and Outputs
==========================

It is possible to have sets of inputs and outputs in Benthos that can be added,
updated and removed during runtime with the [dynamic fan in][dynamic_inputs] and
[dynamic fan out][dynamic_outputs] types.

Dynamic inputs and outputs are each identified by unique string labels, which
are specified when adding them either in configuration or via the HTTP API. The
labels are useful when querying which types are active.

## API

The API for dynamic types (both inputs and outputs) is a collection of HTTP REST
endpoints:

### `/inputs`

Returns a JSON object of labels to uptime of the currently active inputs.

### `/input/{input_label}`

GET returns the configuration of the input idenfified by `input_label`.

POST sets the input `input_label` to the body of the request parsed as a JSON
configuration. If the input label already exists the previous input is first
stopped and removed.

DELETE stops and removes the input identified by `input_label`.

### `/outputs`

Returns a JSON object of labels to uptime of the currently active outputs.

### `/output/{output_label}`

GET returns the configuration of the output idenfified by `output_label`.

POST sets the output `output_label` to the body of the request parsed as a JSON
configuration. If the output label already exists the previous output is first
stopped and removed.

DELETE stops and removes the output identified by `output_label`.

A custom prefix can be set for these endpoints in configuration.

## Applications

Dynamic types are useful when a platforms data streams might need to change
regularly and automatically. It is also useful for triggering batches of
platform data, e.g. a cron job can be created to send hourly curl requests that
adds a dynamic input to read a file directory of sample data.

Some inputs have a finite lifetime, e.g. `amazon_s3` without an SQS queue
configured will close once the whole bucket has been read. When a dynamic types
lifetime ends the label will be no longer appear in queries. You can use this to
write tools that trigger new inputs (to move onto the next bucket, for example).

[dynamic_inputs]: ./inputs/README.md#dynamic
[dynamic_outputs]: ./outputs/README.md#dynamic
