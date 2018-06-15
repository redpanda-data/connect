Streams API
===========

When Benthos is run in `--streams` mode it will open up an HTTP REST API for
creating and managing independent streams of data instead of creating a single
stream.

Each stream has its own input, buffer, pipeline and output sections which
contains an isolated stream of data with its own lifetime.

## API

### GET `/streams`

Returns a map of existing streams by their unique identifiers to an object
showing their status and uptime.

#### Response 200

``` json
{
	"<string, stream id>": {
		"active": "<bool, whether the stream is running>",
		"uptime": "<float, uptime in seconds>",
		"uptime_str": "<string, human readable string of uptime>"
	}
}
```

### POST `/streams`

Sets the entire collection of streams to the body of the request. Streams that
exist but aren't within the request body are *removed*, streams that exist
already and are in the request body are updated, other streams within the
request body are created.

``` json
{
	"<string, stream id>": "<object, a standard Benthos stream configuration>"
}
```

#### Response 200

The streams were updated successfully.

### POST `/streams/{id}`

Create a new stream identified by `id` by posting a body containing the stream
configuration in either JSON or YAML format. The configuration should be a
standard Benthos configuration containing the sections `input`, `buffer`,
`pipeline` and `output`.

#### Response 200

The stream was created successfully.

### GET `/streams/{id}`

Read the details of an existing stream identified by `id`.

#### Response 200

``` json
{
	"active": "<bool, whether the stream is running>",
	"uptime": "<float, uptime in seconds>",
	"uptime_str": "<string, human readable string of uptime>",
	"config": "<object, the configuration of the stream>"
}
```

### PUT `/streams/{id}`

Update an existing stream identified by `id` by posting a body containing the
new stream configuration in either JSON or YAML format. The configuration should
be a standard Benthos configuration containing the sections `input`, `buffer`,
`pipeline` and `output`.

The previous stream will be shut down before and a new stream will take its
place.

#### Response 200

The stream was updated successfully.

### DELETE `/streams/{id}`

Attempt to shut down and remove a stream identified by `id`.

#### Response 200

The stream was found, shut down and removed successfully.
