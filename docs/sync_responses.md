Synchronous Responses
=====================

EXPERIMENTAL: The features outlined in this document are considered experimental
and are therefore subject to change outside of major version releases.

In a regular Benthos stream pipeline messages flow in one direction and
acknowledgements flow in the other:

``` text
    ----------- Message ------------->

Input (AMQP) -> Processors -> Output (AMQP)

    <------- Acknowledgement ---------
```

For most Benthos input and output targets this is the only workflow available.
However, Benthos has support for a number of protocols where this limitation is
not the case.

For example, HTTP is a request/response protocol, and so our `http_server` input
is capable of returning a response payload after consuming a message from a
request.

When using these protocols it's possible to configure Benthos stream pipelines
that allow messages to pass in the opposite direction, resulting in response
messages at the input level:

``` text
           --------- Request Body -------->

Input (HTTP Server) -> Processors -> Output (Sync Response)

           <------- Response Body ---------
```

Such responses can be triggered by configuring a [`sync_response`][sync-res]
output, which allows you to mutate payloads using Benthos processors and then
send them back to the input source:

``` yaml
input:
  http_server:
    path: /post
pipeline:
  processors:
  - text:
      operator: to_upper
output:
  type: sync_response
```

Using the above example, sending a request 'foo bar' to the path `/post` returns
the response 'FOO BAR'.

It's also possible to combine a `sync_response` output with other outputs using
a [`broker`][output-broker]:

``` yaml
input:
  http_server:
    path: /post
output:
  broker:
    pattern: fan_out
    outputs:
    - kafka:
        addresses: [ TODO:9092 ]
        topic: foo_topic
    - type: sync_response
      processors:
      - text:
          operator: to_upper
```

Using the above example, sending a request 'foo bar' to the path `/post` passes
the message unchanged to the Kafka topic `foo_topic` and also returns the
response 'FOO BAR'.

[sync-res]: ./outputs/README.md#sync_response
[output-broker]: ./outputs/README.md#broker