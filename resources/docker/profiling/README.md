Profiling
=========

This docker compose sets a Benthos instance up with a custom config, [Prometheus][prometheus], [Grafana][grafana] and [Jaeger][jaeger] in order to observe its performance under different configurations.

# Set up

- Run with `docker-compose up -d`.
- Open up Grafana at [http://localhost:3000/d/PHrVlmniz/benthos-dash](http://localhost:3000/d/PHrVlmniz/benthos-dash)
    - default user:pw is admin:admin
- Go to [http://localhost:16686](http://localhost:16686) in order to observe opentracing events with Jaeger.
- Endpoints is available at [http://localhost:4195/endpoints](http://localhost:4195/endpoints) documented [here](https://www.benthos.dev/docs/components/http/about/#endpoints)
    - Use `go tool pprof http://localhost:4195/debug/pprof/profile` and similar endpoints to get profiling data.
- Stop with `docker-compose stop`

[prometheus]: https://prometheus.io/
[grafana]: https://grafana.com/
[jaeger]: https://www.jaegertracing.io/
