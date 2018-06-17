Webhook With Fallback
=====================

It's possible to create fallback options for output types by using a
[`try` broker][try-broker]. In this guide we'll explore this pattern by creating
stream bridges that accept messages via an HTTP server endpoint and forwards the
messages to another HTTP endpoint, and if it fails to do so will print the
message to `stdout` as a warning.

In practice our input might be something like a RabbitMQ exchange, and our
fallback output might be a queue used for scheduling action by a third party.

We will also be using Benthos in `--streams` mode in this example so that we can
create these bridges dynamically via [REST endpoints][streams-api].

The first thing to do is to run Benthos:

``` sh
benthos --streams
```

And we can set up and start our stream under the id `foo` by sending it to the
`/streams/foo` endpoint. For this example we will use curl, but this would
normally be done via another service.

``` sh
curl http://localhost:4195/streams/foo --data-binary @- <<EOF
input:
  type: http_server
output:
  type: broker
  broker:
    pattern: try
    outputs:
      - type: http_client
        http_client:
          url: http://localhost/post
          verb: POST
          backoff_on: [ 429 ]
          drop_on: [ 409 ]
          retries: 3
      - type: stdout
        processors:
        - type: insert_part
          insert_part:
            content: "MESSAGE FAILED:"
            index: 0
EOF
```

It's worth noting that we have added a processor on the fallback output which
adds prefix content to the logged message. We could apply any processors here to
make arbitrary changes to the payload.

We can check whether the stream is ready and active with:

``` sh
curl http://localhost:4195/streams/foo | jq '.active'
```

And send some content with:

``` sh
curl http://localhost:4195/foo/post -d "hello world"
```

Benthos will now attempt to send the payload to the configured `http_client`
endpoint. If the endpoint isn't reachable then after a few seconds you should
see something like this on `stdout`:

``` sh
MESSAGE FAILED:
hello world
```

Try playing around with inputs, fallback outputs and fallback processors.

[try-broker]: ../outputs/README.md#try
[streams-api]: ../api/streams.md
