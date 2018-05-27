Profiling
=========

This docker compose sets a Benthos instance up with a custom config,
[Prometheus][prometheus] and [Grafana][grafana] in order to observe its
performance under different configurations.

# Set up

Edit the data within `sample_data.txt` to the type of input data you wish to
profile with.

Next, edit `benthos.yaml` in order to add processors and features that you wish
to profile against.

Run with `docker-compose up`.

Go to [http://localhost:3000](http://localhost:3000) in order to set up your own
dashboards.

Use `go tool pprof http://localhost:4195/debug/pprof/profile` and similar
endpoints to get profiling data.

[prometheus]: https://prometheus.io/
[grafana]: https://grafana.com/
