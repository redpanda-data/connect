Streaming AWS S3 Archives
=========================

This example demonstrates how Benthos can be used to stream an S3 bucket of
`.tar.gz` archives containing JSON documents into any output target. This
example is able to listen for newly added archives and then downloads,
decompresses, unarchives and streams the JSON documents found within to a Kafka
topic. The Kafka output in this example can be replaced with any Benthos
[output target][outputs].

The method used to stream archives is via an [SQS queue][s3-tracking], which is
a common pattern. Benthos can work either with S3 events sent via SQS directly,
or by S3 events broadcast via SNS to SQS, there is a small adjustment to the
config which is explained in the input section.

The full config for this [example can be found here][example].

## Input

``` yaml
input:
  s3:
    region: eu-west-1 # TODO
    bucket: TODO
    delete_objects: false
    sqs_url: TODO
    sqs_body_path: Records.*.s3.object.key
    sqs_envelope_path: ""
    sqs_max_messages: 10
    credentials:
      id: "TODO"
      secret: "TODO"
      token: "TODO"
      role: "TODO"
```

This input section contains lots of fields to be completed which are self
explanatory, such as `bucket`, `sqs_url` and the `credentials` section.

The `sqs_body_path` field is the JSON path within an SQS message that contains
the name of new S3 files, which should be left as `Records.*.s3.object.key` unless
you have built a custom solution.

If SNS is being used to broadcast S3 events instead of connecting SQS directly
you will need to fill in the `sqs_envelope_path`, which is the JSON path inside
an SNS message that contains the enveloped S3 event. The value of
`sqs_envelope_path` should be `Message` when using the standard AWS set up.

This example uses a single consumer, but if the throughput isn't high enough to
keep up with the bucket it is possible to use a `broker` type to have multiple
parallel consumers:

``` yaml
input:
  broker:
    copies: 8 # Increase this to gain more parallel consumers
    inputs:
    - s3:
      ... etc
```

You can have any number of consumers of a bucket and messages (archives) will
automatically be distributed amongst them via the SQS queue.

## Pipeline

``` yaml
pipeline:
  threads: 4 # Try to match the number of available logical CPU cores
  processors:
  - decompress:
      algorithm: gzip
  - unarchive:
      format: tar
  - split:
      size: 10 # The size of message batches to send to Kafka
```

The processors in this example start off with a simple decompress and unarchive
of the payload. This results in a single payload of multiple documents. The
split processor turns this payload into smaller batches.

These processors are heavy on CPU, which is why they are configured inside the
pipeline section. This allows you to explicitly set the number of parallel
threads to exactly match the number of logical CPU cores available.

## Output

The output config is a standard Kafka output.

[s3-tracking]: https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html
[example]: ./streaming-aws-s3-archives.yaml
[outputs]: ../outputs/README.md
