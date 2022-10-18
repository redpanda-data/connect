---
title: Streams Via Config Files
---

When running Benthos in `streams` mode it's possible to create streams with their own static configurations, simply list one or more files after the `streams` subcommand:

```sh
benthos streams ./foo.yaml ./configs/*.yaml
```

## Resources

A stream configuration should only include the base stream component fields (`input`, `buffer`, `pipeline`, `output`), and therefore should NOT include any [resources][resources]. Instead, define resources separately and import them using the `-r`/`--resources` flag:

```sh
benthos -r "./resources/prod/*.yaml" streams ./stream_configs/*.yaml
```

## Walkthrough

Make a directory of stream configs:

``` bash
$ mkdir ./streams

$ cat > ./streams/foo.yaml <<EOF
input:
  http_server: {}
pipeline:
  threads: 4
  processors:
    - mapping: 'root = {"id": this.user.id, "content": this.body.content}'
output:
  http_server: {}
EOF

$ cat > ./streams/bar.yaml <<EOF
input:
  kafka:
    addresses:
      - localhost:9092
    topics:
      - my_topic
pipeline:
  threads: 1
  processors:
    - mapping: 'root = this.uppercase()'
output:
  elasticsearch:
    urls:
    - http://localhost:9200
EOF
```

Run Benthos in streams mode, pointing to our directory of streams:

``` bash
$ benthos streams ./streams/*.yaml
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
      "http_server": {
        "address": "",
        "cert_file": "",
        "key_file": "",
        "path": "/post",
        "timeout": "5s"
      }
    },
    "buffer": {
      "memory": {
        "limit": 10000000
      }
    },
    "pipeline": {
      "processors": [
        {
          "mapping": "root = {\"id\": this.user.id, \"content\": this.body.content}",
        }
      ],
      "threads": 4
    },
    "output": {
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

You can then send data to the stream via it's namespaced URL:

```
$ curl http://localhost:4195/foo/post -d '{"user":{"id":"foo"},"body":{"content":"bar"}}'
```

There are other endpoints [in the REST API][rest-api] for creating, updating and deleting streams.

[rest-api]: /docs/guides/streams_mode/using_rest_api
[interpolation]: /docs/configuration/interpolation
[resources]: /docs/configuration/resources
