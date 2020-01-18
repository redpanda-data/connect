---
title: http_client
type: input
---

Connects to a server and continuously performs requests for a single message.


import Tabs from '@theme/Tabs';

<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Advanced', value: 'advanced', },
]}>

import TabItem from '@theme/TabItem';

<TabItem value="common">

```yaml
input:
  http_client:
    url: http://localhost:4195/get
    verb: GET
    headers:
      Content-Type: application/octet-stream
    rate_limit: ""
    timeout: 5s
    payload: ""
    stream:
      enabled: false
      reconnect: true
```

</TabItem>
<TabItem value="advanced">

```yaml
input:
  http_client:
    url: http://localhost:4195/get
    verb: GET
    headers:
      Content-Type: application/octet-stream
    oauth:
      access_token: ""
      access_token_secret: ""
      consumer_key: ""
      consumer_secret: ""
      enabled: false
      request_url: ""
    basic_auth:
      enabled: false
      password: ""
      username: ""
    tls:
      client_certs: []
      enabled: false
      root_cas_file: ""
      skip_cert_verify: false
    copy_response_headers: false
    rate_limit: ""
    timeout: 5s
    retry_period: 1s
    max_retry_backoff: 300s
    retries: 3
    backoff_on:
    - 429
    drop_on: []
    successful_on: []
    payload: ""
    stream:
      enabled: false
      reconnect: true
      multipart: false
      max_buffer: 1e+06
      delimiter: ""
```

</TabItem>
</Tabs>

The URL and header values of this type can be dynamically set using function
interpolations described [here](/docs/configuration/interpolation#functions).

### Streaming

If you enable streaming then Benthos will consume the body of the response as a
line delimited feed of message parts. Each part is read as an individual message
unless multipart is set to true, in which case an empty line indicates the end
of a message.

## Fields

### `url`

`string` The URL to connect to.

### `verb`

`string` A verb to connect with

```yaml
# Examples

verb: POST

verb: GET

verb: DELETE
```

### `headers`

`object` A map of headers to add to the request.

```yaml
# Examples

headers:
  Content-Type: application/octet-stream
```

### `oauth`

`object` Allows you to specify open authentication.

```yaml
# Examples

oauth:
  access_token: baz
  access_token_secret: bev
  consumer_key: foo
  consumer_secret: bar
  enabled: true
  request_url: http://thisisjustanexample.com/dontactuallyusethis
```

### `basic_auth`

`object` Allows you to specify basic authentication.

```yaml
# Examples

basic_auth:
  enabled: true
  password: bar
  username: foo
```

### `tls`

`object` Custom TLS settings can be used to override system defaults. This includes
providing a collection of root certificate authorities, providing a list of
client certificates to use for client verification and skipping certificate
verification.

Client certificates can either be added by file or by raw contents.

```yaml
# Examples

tls:
  client_certs:
  - cert_file: ./example.pem
    key_file: ./example.key
  enabled: true

tls:
  client_certs:
  - cert: foo
    key: bar
  enabled: true
  skip_cert_verify: true
```

### `copy_response_headers`

`bool` Sets whether to copy the headers from the response to the resulting payload.

### `rate_limit`

`string` An optional [rate limit](/docs/components/rate_limits/about) to throttle requests by.

### `timeout`

`string` A static timeout to apply to requests.

### `retry_period`

`string` The base period to wait between failed requests.

### `max_retry_backoff`

`string` The maximum period to wait between failed requests.

### `retries`

`number` The maximum number of retry attempts to make.

### `backoff_on`

`array` A list of status codes whereby retries should be attempted but the period between them should be increased gradually.

### `drop_on`

`array` A list of status codes whereby the attempt should be considered failed but retries should not be attempted.

### `successful_on`

`array` A list of status codes whereby the attempt should be considered successful (allows you to configure non-2XX codes).

### `payload`

`string` An optional payload to deliver for each request.

### `stream`

`object` Allows you to set streaming mode, where requests are kept open and messages are processed line-by-line.

### `stream.enabled`

`bool` Enables streaming mode.

### `stream.reconnect`

`bool` Sets whether to re-establish the connection once it is lost.

### `stream.multipart`

`bool` When running in stream mode sets whether an empty line indicates the end of a message batch, and only then is the batch flushed downstream.

### `stream.max_buffer`

`number` Must be larger than the largest line of the stream.

### `stream.delimiter`

`string` A string that indicates the end of a message within the stream. If left empty
then line feed (\n) is used.


