Examples
========

Here is a collection of Benthos configuration examples showing a range of common
or interesting use cases.

### [Kafka JSON Mutating](./kafka-json-mutating.md)

Create a pipeline that mutates JSON documents from a Kafka stream.

### [Kafka Delayed Retry](./kafka-delayed-retry.md)

Create a processing pipeline where if the processor fails the data is redirected
to a second pipeline for retries. The second pipeline delays the next attempt
based on a timestamp within the document.

### [Deduplicated Twitter Firehose](./twitter-firehose.md)

Create a pipeline that consumes a continuous HTTP stream (in this case the
Twitter firehose) and writes the data to a Kafka topic.

### [Streaming AWS S3 Archives](./streaming-aws-s3-archives.md)

Create a pipeline that continuously consumes `.tar.gz` archives from an S3
bucket, decompresses and unarchives the data, and then writes the data to a
Kafka topic.
