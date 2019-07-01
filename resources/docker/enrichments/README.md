Enrichments
===========

This docker compose sets a Benthos instance up with a config demonstrating how Benthos can be used for orchestrating HTTP based enrichment flows. An instance of [httpbin](https://hub.docker.com/r/kennethreitz/httpbin/) is used for mocking the enrichment endpoints, [Prometheus](https://prometheus.io/) and [Grafana](https://grafana.com/) are used in order to monitor the performance and behaviour of the configuration.

Set up
======

Edit the data within `sample_data.txt` to the type of input data you wish to profile with.

Next, edit `benthos.yaml` in order to add processors and features that you wish to profile against.

Run with `docker-compose up`.

Go to [http://localhost:3000](http://localhost:3000) in order to set up your own dashboards.

Use `go tool pprof http://localhost:4195/debug/pprof/profile` and similar endpoints to get profiling data.
