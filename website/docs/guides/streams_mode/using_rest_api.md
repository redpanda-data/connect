---
title: Streams Via REST API
---

By using the Benthos `streams` mode REST API you can dynamically control which streams are active at runtime. The full spec for the Benthos streams mode REST API can be [found here][http-interface].

Note that stream configs created and updated using this API do *not* benefit from [environment variable interpolation][interpolation] (function interpolation will still work).

## Walkthrough

Start by running Benthos in streams mode:

```bash
$ benthos streams
```

On a separate terminal we can add our first stream `foo` by `POST`ing a JSON or YAML config to the `/streams/foo` endpoint:

```bash
$ curl http://localhost:4195/streams/foo -X POST --data-binary @- <<EOF
input:
  http_server: {}
buffer:
  memory: {}
pipeline:
  threads: 4
  processors:
    - bloblang: |
        root = {
          "id": this.user.id,
          "content": this.body.content
        }
output:
  http_server: {}
EOF
```

Now we can check the full set of streams loaded by `GET`ing the `/streams` endpoint:

```bash
$ curl http://localhost:4195/streams | jq '.'
{
  "foo": {
    "active": true,
    "uptime": 7.223545951,
    "uptime_str": "7.223545951s"
  }
}
```

And we can send data to our new stream via it's namespaced URL:

```
$ curl http://localhost:4195/foo/post -d '{"user":{"id":"foo"},"body":{"content":"bar"}}'
```

Good, now let's add another stream `bar` the same way:

```bash
$ curl http://localhost:4195/streams/bar -X POST --data-binary @- <<EOF
input:
  kafka:
    addresses:
      - localhost:9092
    topics:
      - my_topic
pipeline:
  threads: 1
  processors:
    - bloblang: 'root = this.uppercase()'
output:
  elasticsearch:
    urls:
      - http://localhost:9200
EOF
```

And check the set again:

```bash
$ curl http://localhost:4195/streams | jq '.'
{
  "bar": {
    "active": true,
    "uptime": 10.121344484,
    "uptime_str": "10.121344484s"
  },
  "foo": {
    "active": true,
    "uptime": 19.380582951,
    "uptime_str": "19.380583306s"
  }
}
```

It's also possible to get the configuration of a loaded stream by `GET`ing the path `/streams/{id}`:

```bash
$ curl http://localhost:4195/streams/foo | jq '.'
{
  "active": true,
  "uptime": 30.123488951,
  "uptime_str": "30.123488951s"
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
    ... etc ...
  }
}
```

Next, we might want to update stream `foo` by `PUT`ing a new config to the path `/streams/foo`:

```bash
$ curl http://localhost:4195/streams/foo -X PUT --data-binary @- <<EOF
input:
  http_server: {}
pipeline:
  threads: 4
  processors:
  - bloblang: |
      root = {
        "id": this.user.id,
        "content": this.body.content
      }
output:
  http_server: {}
EOF
```

We have removed the memory buffer with this change, let's check that the config has actually been updated:

```bash
$ curl http://localhost:4195/streams/foo | jq '.'
{
  "active": true,
  "uptime": 12.328482951,
  "uptime_str": "12.328482951s"
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
      "type": "none"
    },
    ... etc ...
  }
}
```

Good, we are done with stream `bar` now, so let's delete it by `DELETE`ing the `/streams/bar` endpoint:

```bash
$ curl http://localhost:4195/streams/bar -X DELETE
```

And let's `GET` the `/streams` endpoint to see the new set:

```bash
$ curl http://localhost:4195/streams | jq '.'
{
  "foo": {
    "active": true,
    "uptime": 31.872448851,
    "uptime_str": "31.872448851s"
  }
}
```

Great. Another useful feature is `POST`ing to `/streams`, this allows us to set the entire set of streams with a single request.

The payload is a map of stream ids to configurations and this will become the exclusive set of active streams. If there are existing streams that are not on the list they will be removed.

```bash
$ curl http://localhost:4195/streams -X POST --data-binary @- <<EOF
bar:
  input:
    http_client:
      url: http://localhost:4195/baz/get
  output:
    stdout: {}
baz:
  input:
    http_server: {}
  output:
    http_server: {}
EOF
```

Let's check our new set of streams:

```bash
$ curl http://localhost:4195/streams | jq '.'
{
  "bar": {
    "active": true,
    "uptime": 3.183883444,
    "uptime_str": "3.183883444s"
  },
  "baz": {
    "active": true,
    "uptime": 3.183883449,
    "uptime_str": "3.183883449s"
  }
}
```

Done.

[http-interface]: /docs/guides/streams_mode/streams_api
[interpolation]: /docs/configuration/interpolation
