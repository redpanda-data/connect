---
title: Streams API
---

When Benthos is run in `streams` mode it will open up an HTTP REST API for creating and managing independent streams of data instead of creating a single stream.

Each stream has its own input, buffer, pipeline and output sections which contains an isolated stream of data with its own lifetime. A stream config cannot include [resources][resources], and instead these should be created and modified using the `/resources/{type}/{id}` endpoint.

A walkthrough on using this API [can be found here][streams-api-walkthrough].

## API

### GET `/ready`

Returns a 200 OK response if all active streams are connected to their respective inputs and outputs at the time of the request. Otherwise, a 503 response is returned along with a message naming the faulty stream.

If zero streams are active this endpoint still returns a 200 OK response.

### GET `/streams`

Returns a map of existing streams by their unique identifiers to an object showing their status and uptime.

#### Response 200

```json
{
	"<string, stream id>": {
		"active": "<bool, whether the stream is running>",
		"uptime": "<float, uptime in seconds>",
		"uptime_str": "<string, human readable string of uptime>"
	}
}
```

### POST `/streams`

Sets the entire collection of streams to the body of the request. Streams that exist but aren't within the request body are *removed*, streams that exist already and are in the request body are updated, other streams within the request body are created.

```json
{
	"<string, stream id>": "<object, a standard Benthos stream configuration>"
}
```

#### Response 200

The streams were updated successfully.

#### Response 400

A configuration was invalid, or has linting errors. If linting errors were detected then a JSON response is provided of the form:

```json
{
	"linting_errors": [
		"<a description of the error"
	]
}
```

If you wish for the streams API to proceed with configurations that contain linting errors then you can override this check by setting the URL param `chilled` to `true`, e.g. `/streams?chilled=true`.

### POST `/streams/{id}`

Create a new stream identified by `id` by posting a body containing the stream configuration in either JSON or YAML format. The configuration should be a standard Benthos configuration containing the sections `input`, `buffer`, `pipeline` and `output`.

#### Request Body Example

URL: `/streams/foo`

```yaml
input:
  file:
    paths: [ /tmp/input.ndjson ]
pipeline:
  processors:
    - bloblang: root = content().uppercase()
output:
  file:
    path: /tmp/output.ndjson
```

#### Response 200

The stream was created successfully.

#### Response 400

The configuration was invalid, or has linting errors. If linting errors were detected then a JSON response is provided of the form:

```json
{
	"linting_errors": [
		"<a description of the error"
	]
}
```

If you wish for the streams API to proceed with configurations that contain linting errors then you can override this check by setting the URL param `chilled` to `true`, e.g. `/streams/foo?chilled=true`.

### GET `/streams/{id}`

Read the details of an existing stream identified by `id`.

#### Response 200

```json
{
	"active": "<bool, whether the stream is running>",
	"uptime": "<float, uptime in seconds>",
	"uptime_str": "<string, human readable string of uptime>",
	"config": "<object, the configuration of the stream>"
}
```

### PUT `/streams/{id}`

Update an existing stream identified by `id` by posting a body containing the new stream configuration in either JSON or YAML format. The configuration should be a standard Benthos configuration containing the sections `input`, `buffer`, `pipeline` and `output`.

The previous stream will be shut down before and a new stream will take its place.

#### Response 200

The stream was updated successfully.

#### Response 400

The configuration was invalid, or has linting errors. If linting errors were detected then a JSON response is provided of the form:

```json
{
	"linting_errors": [
		"<a description of the error"
	]
}
```

If you wish for the streams API to proceed with configurations that contain linting errors then you can override this check by setting the URL param `chilled` to `true`, e.g. `/streams/foo?chilled=true`.

### PATCH `/streams/{id}`

Update an existing stream identified by `id` by posting a body containing only changes to be made to the existing configuration. The existing configuration will be patched with the new fields and the stream restarted with the result.

#### Response 200

The stream was patched successfully.

### DELETE `/streams/{id}`

Attempt to shut down and remove a stream identified by `id`.

#### Response 200

The stream was found, shut down and removed successfully.

### GET `/streams/{id}/stats`

Read the metrics of an existing stream as a hierarchical JSON object.

#### Response 200

The stream was found.

### POST `/resources/{type}/{id}`

Add or modify a resource component configuration of a given `type` identified by a unique `id`. The configuration must be in JSON or YAML format and must only contain configuration fields for the component.

Valid component types are `cache`, `input`, `output`, `processor` and `rate_limit`.

#### Request Body Example

URL: `/resources/cache/foo`

```yml
redis:
  url: http://localhost:6379
  expiration: 1h
```

#### Response 200

The resource was created successfully.

#### Response 400

The configuration was invalid, or has linting errors. If linting errors were detected then a JSON response is provided of the form:

```json
{
	"linting_errors": [
		"<a description of the error"
	]
}
```

If you wish for the streams API to proceed with configurations that contain linting errors then you can override this check by setting the URL param `chilled` to `true`, e.g. `/resources/cache/foo?chilled=true`.

[streams-api-walkthrough]: /docs/guides/streams_mode/using_rest_api
[resources]: /docs/configuration/resources
