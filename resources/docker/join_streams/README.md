Join Streams
============

This docker-compose example demonstrates a pipeline where a stream of multiple
document types (articles and likes of articles) can be joined using caches.

The sample data contains articles that have an id at the path `article.id` and
likes that have a target article id at `like.article_id`. The goal is for the
pipeline to attach the target article to all likes passing through the pipeline.

This is achieved by caching all articles as they pass through, and querying that
cache for the target article id of all likes. In the case where a target article
is not found for a like it is placed in a retry topic for reconsumption (in this
case by the same pipeline.)

When a document is reconsumed via the retry topic its processing is delayed for
30 seconds from the time where it was previously attempted. If a like is
reattempted more than three times it is dropped entirely and a log message is
printed. The sample data contains likes that will never resolve in order to
demonstrate this behaviour.

# Set up

Run the pipeline and monitoring services with `docker-compose up`.

Use `benthos -c ./inject_data.yaml` to start sending data into the
`data_source` Kafka topic. This config will continue writing the sample data in
a loop until stopped.

In order to see the joined data being output from the pipeline you can consume
it and write it to stdout with `benthos -c ./extract_data.yaml`.

# Monitoring

Go to [http://localhost:3000](http://localhost:3000) (admin/admin) in order to
view dashboards.