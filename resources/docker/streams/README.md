Streams
=======

This docker-compose example shows how to use Benthos in [`--streams`][streams]
mode with a static directory of stream configurations. If you wish to avoid
using configuration files entirely you can just remove the volume and
exclusively add streams using the [REST HTTP interface][http-streams].

The example creates two streams, where `es.yaml` sends `http_server` data to an
Elasticsearch index, and `s3.yaml` sends `http_server` data to an Amazon S3
bucket.

Start: `docker-compose up`

Send data to Elasticsearch:

``` bash
curl http://localhost:4195/es/send -d '{"foo":"hello world"}'
```

Send data to Amazon S3:

``` bash
curl http://localhost:4195/s3/send -d '{"foo":"hello world"}'
```

[streams]: ../../../docs/streams/README.md
[http-streams]: ../../../docs/api/streams.md
