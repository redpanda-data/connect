---
title: HTTP
---

When Benthos runs it kicks off an HTTP server that provides a few generally useful endpoints and is also where configured components such as the [`http_server` input][inputs.http_server] [and output][outputs.http_server] can register their own endpoints if they don't require their own host/port.

The configuration for this server lives under the `http` namespace, with the following default values:

```yaml
http:
  address: 0.0.0.0:4195
  enabled: true
  root_path: /benthos
  debug_endpoints: false
  cert_file: ""
  key_file: ""
  cors:
    enabled: false
    allowed_origins: []
```

The field `enabled` can be set to `false` in order to disable the server.

The field `root_path` specifies a general prefix for all endpoints, this can help isolate the service endpoints when using a reverse proxy with other shared services. All endpoints will still be registered at the root as well as behind the prefix, e.g. with a `root_path` set to `/foo` the endpoint `/version` will be accessible from both `/version` and `/foo/version`.

## Enabling HTTPS

By default Benthos will serve traffic over HTTP. In order to enforce TLS and serve traffic exclusively over HTTPS you must provide a `cert_file` and `key_file` path in your config, which point to a file containing a certificate and a matching private key for the server respectively.

If the certificate is signed by a certificate authority, the `cert_file` should be the concatenation of the server's certificate, any intermediates, and the CA's certificate.

## Endpoints

The following endpoints will be generally available when the HTTP server is enabled:

- `/version` provides version info.
- `/ping` can be used as a liveness probe as it always returns a 200.
- `/ready` can be used as a readiness probe as it serves a 200 only when both the input and output are connected, otherwise a 503 is returned.
- `/metrics`, `/stats` both provide metrics when the metrics type is either [`json_api`][metrics.json_api] or [`prometheus`][metrics.prometheus].
- `/endpoints` provides a JSON object containing a list of available endpoints, including those registered by configured components.

## CORS

In order to serve Cross-Origin Resource Sharing headers, which instruct browsers to allow CORS requests, set the subfield `cors.enabled` to `true`.

### allowed_origins

A list of allowed origins to connect from. The literal value `*` can be specified as a wildcard. Note `cors.enabled` must be set to `true` for this list to take effect.

## Debug Endpoints

The field `debug_endpoints` when set to `true` prompts Benthos to register a few extra endpoints that can be useful for debugging performance or behavioral problems:

- `/debug/config/json` returns the loaded config as JSON.
- `/debug/config/yaml` returns the loaded config as YAML.
- `/debug/pprof/block` responds with a pprof-formatted block profile.
- `/debug/pprof/heap` responds with a pprof-formatted heap profile.
- `/debug/pprof/mutex` responds with a pprof-formatted mutex profile.
- `/debug/pprof/profile` responds with a pprof-formatted cpu profile.
- `/debug/pprof/symbol` looks up the program counters listed in the request, responding with a table mapping program counters to function names.
- `/debug/pprof/trace` responds with the execution trace in binary form. Tracing lasts for duration specified in seconds GET parameter, or for 1 second if not specified.
- `/debug/stack` returns a snapshot of the current service stack trace.

[inputs.http_server]: /docs/components/inputs/http_server
[outputs.http_server]: /docs/components/outputs/http_server
[metrics.json_api]: /docs/components/metrics/json_api
[metrics.prometheus]: /docs/components/metrics/prometheus
