---
title: Streams Via Config Files
---

When running Benthos in `--streams` mode it's possible to create streams with
their own static configurations by setting the `--streams-dir` flag to a
directory containing a config file for each stream (`/benthos/streams` by
default).

Note that stream configs loaded in this way can benefit from
[interpolation][interpolation].

## Walkthrough

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

You can also query a specific stream to see the loaded configuration:

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
        "timeout": "5s"
      }
    },
    "buffer": {
      "type": "memory",
      "memory": {
        "limit": 10000000
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
        "timeout": "5s"
      }
    }
  }
}
```

There are other endpoints [in the REST API][rest-api] for creating, updating and
deleting streams.

[rest-api]: using_rest_api
[interpolation]: /docs/configuration/interpolation
