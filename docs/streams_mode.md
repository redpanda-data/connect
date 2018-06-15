Streams Mode
============

A Benthos stream consists of four components; an input, an optional buffer,
processor pipelines and an output. Under normal use a Benthos instance is a
single stream, and the components listed are configured within the service
config file.

Alternatively, Benthos can be run in `--streams` mode, where a single running
Benthos instance is able to run multiple entirely isolated streams. Streams can
be configured by setting the `--streams-dir` flag to a directory containing a
config file for each stream (`/benthos/streams` by default).

During runtime streams can also be added, updated, removed and monitored using
[a REST HTTP interface][http-interface].

Note that stream configs loaded from `--streams-dir` can benefit from
[interpolation][interpolation] but configs loaded through the HTTP REST
endpoints cannot.

## Using directory of stream configs

Make a directory of stream configs:

``` bash
$ mkdir ./streams

$ cat > ./streams/foo.yaml <<EOF
input:
  type: http_server
buffer:
  type: memory
pipeline:
  threads: 4
  processors:
  - type: jmespath
    jmespath:
      query: "{id: user.id, content: body.content}"
output:
  type: http_server
EOF

$ cat > ./streams/bar.yaml <<EOF
input:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    topic: my_topic
buffer:
  type: none
pipeline:
  threads: 1
  processors:
  - type: sample
    sample:
      retain: 10
output:
  type: elasticsearch
  elasticsearch:
    urls:
    - http://localhost:9200
EOF
```

Run Benthos in streams mode, pointing to our directory of streams:

``` bash
$ benthos --streams --streams-dir ./streams
```

On a separate terminal you can query the set of streams loaded:

``` bash
$ curl http://localhost:4195/streams | jq '.'
{
  "bar": {
    "active": true,
    "uptime": 19.381001424,
    "uptime_str": "19.381001552s"
  },
  "foo": {
    "active": true,
    "uptime": 19.380582951,
    "uptime_str": "19.380583306s"
  }
}
```

And you can query the loaded configuration of a stream:

``` bash
$ curl http://localhost:4195/streams/foo | jq '.'
{
  "active": true,
  "uptime": 69.334717193,
  "uptime_str": "1m9.334717193s",
  "config": {
    "input": {
      "type": "http_server",
      "http_server": {
        "address": "",
        "cert_file": "",
        "key_file": "",
        "path": "/post",
        "timeout_ms": 5000
      },
      "processors": [
        {
          "type": "bounds_check",
          "bounds_check": {
            "max_part_size": 1073741824,
            "max_parts": 100,
            "min_part_size": 1,
            "min_parts": 1
          }
        }
      ]
    },
    "buffer": {
      "type": "memory",
      "memory": {
        "limit": 0
      }
    },
    "pipeline": {
      "processors": [
        {
          "type": "jmespath",
          "jmespath": {
            "parts": [],
            "query": "{id: user.id, content: body.content}"
          }
        }
      ],
      "threads": 4
    },
    "output": {
      "type": "http_server",
      "http_server": {
        "address": "",
        "cert_file": "",
        "key_file": "",
        "path": "/get",
        "stream_path": "/get/stream",
        "timeout_ms": 5000
      }
    }
  }
}
```

There are other endpoints [in the REST API][http-interface] for creating,
updating and deleting streams.

[http-interface]: api/streams.md
[interpolation]: config_interpolation.md
