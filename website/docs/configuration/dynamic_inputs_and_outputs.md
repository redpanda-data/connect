---
title: Dynamic Inputs and Outputs
---

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

Returns a JSON object that maps input labels to an object containing details
about the input, including uptime and configuration. If the input has terminated
naturally the uptime will be set to `stopped`.

``` json
{
  "<string, input_label>": {
    "uptime": "<string>",
    "config": <object>
  },
  ...
}
```

### `/inputs/{input_label}`

GET returns the configuration of the input idenfified by `input_label`.

POST sets the input `input_label` to the body of the request parsed as a JSON
configuration. If the input label already exists the previous input is first
stopped and removed.

DELETE stops and removes the input identified by `input_label`.

### `/outputs`

Returns a JSON object that maps output labels to an object containing details
about the output, including uptime and configuration. If the output has
terminated naturally the uptime will be set to `stopped`.

``` json
{
  "<string, output_label>": {
    "uptime": "<string>",
    "config": <object>
  },
  ...
}
```

### `/outputs/{output_label}`

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
adds a dynamic input to read a file of sample data:

``` sh
curl http://localhost:4195/inputs/read_sample -d @- << EOF
{
	"file": {
		"path": "/tmp/line_delim_sample_data.txt"
	}
}
EOF
```

Some inputs have a finite lifetime, e.g. `s3` without an SQS queue configured
will close once the whole bucket has been read. When a dynamic types lifetime
ends the `uptime` field of an input listing will be set to `stopped`. You can
use this to write tools that trigger new inputs (to move onto the next bucket,
for example).

[dynamic_inputs]: /docs/components/inputs/dynamic
[dynamic_outputs]: /docs/components/outputs/dynamic
