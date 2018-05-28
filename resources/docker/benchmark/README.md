Benchmark
=========

This docker-compose example is designed to set up similar Benthos and Logstash
pipelines and compare the throughput and resource usage of each.

Creates:

- Prometheus
- Grafana
- Kafka
- Exporter for Kafka metrics to Prometheus
- Benthos
- Logstash

Both Benthos and Logstash are configured to read data from the same topic
`data_source` and write to the topics `benthos_sink` and `logstash_sink`
respectively.

# Set up

Edit the `benthos.yaml` and `pipeline/kafka.conf` configs in order to create
equivalent test pipelines for both services.

Next, edit the data in `sample_data.txt` to data types of your choosing, and run
`docker-compose up`. You might need to restart `kafka_exporter` if it crashes.

Finally, use `benthos -c ./inject_data.yaml` to start sending data into the
`data_source` Kafka topic. This config will continue writing the sample data in
a loop until stopped.

You can compare the resulting data streams of each queue with:

```
TARGET=logstash benthos -c ./extract_data.yaml
TARGET=benthos benthos -c ./extract_data.yaml
```

# Monitoring

Go to [http://localhost:3000](http://localhost:3000) (admin/admin) in order to
view dashboards. There is a dashboard `kafka_offsets` that will show the current
offset of both services sink destinations, this can be used to observe the raw
data throughput of both.
