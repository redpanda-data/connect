Kafka Delayed Retry
===================

## The Enrichment Stream

We are consuming a Kafka stream of documents and wish to apply an enrichment to
each item of the stream via an HTTP service. If the enrichment fails for a
document then we wish to send the data to a different destination than
successful documents such that the enrichment can be retried on a delay by a
component down stream.

In order to do this we are going to use the [`catch`][catch-proc] processor as
our [error recovery handling][error-handling-recover] mechanism. In order to
dynamically route documents we will use [function interpolation][func-interp] to
base the output topic on metadata, which we can set dynamically via our recovery
mechanism.

``` yaml
input:
  kafka_balanced:
    addresses:
    - TODO
    topics:
    - source-queue
    consumer_group: enrichment-consumer
    batching:
      count: 20
      period: 1s

pipeline:
  processors:
  - metadata:
      operator: set
      set: output_topic
      value: enriched-queue

  - http:
      parallel: true
      request:
        url: TODO
        verb: POST
        retries: 3

  - catch:
    - metadata:
        operator: set
        set: output_topic
        value: retry-queue

output:
  kafka:
    addresses:
    - TODO
    topic: "${!metadata:output_topic}"
```

We start our processing steps by setting all documents to have a metadata key
`output_topic` set to `enriched-queue`, which is where successfully enriched
documents should go.

We then do an HTTP request with the [`http`][http-proc] processor which performs
our enrichment. In reality it would likely be more useful to wrap this step in a
[`process_map`][proc-map] processor but the error handling mechanism would be
the same.

After our enrichment the documents will either be enriched or will be flagged as
having [failed a processing step][error-handling-failed], which means we can
perform processors specifically only on failed documents with the
[`catch`][catch-proc] processor. We use this to set the metadata field
`output_topic` to `retry-queue` only for failed documents.

Finally, our output topic is a [function interpolation][func-interp] string
`${!metadata:output_topic}` which resolves dynamically to the contents of the
metadata key `output_topic` for each document. Most output types have a similar
way of dynamically routing documents, otherwise you could use the
[`switch`][switch] or [`broker`][broker] outputs to multiplex the documents.

## The Retry Stream

We now wish to reconsume and reprocess the failed documents from the above
pipeline, but only after 3600 seconds since the data was first consumed. This
time period can be calculated by referring to a timestamp within the JSON
document at the path `meta.created_at`.

We can do this by combining the [`awk`][awk-proc] processor with the
[`sleep`][sleep-proc] processor, using `awk` to calculate our target sleep
period:

``` yaml
input:
  kafka_balanced:
    addresses:
    - TODO
    topics:
    - retry-queue
    consumer_group: retry-consumer
    batching:
      count: 20
      period: 1s

pipeline:
  processors:
  - awk:
      codec: json
      program: |
        {
          delay_for = 3600 - (timestamp_unix() - timestamp_unix(meta_created_at))
          if ( delay_for < 0 )
            delay_for = 0

          metadata_set("delay_for_s", delay_for)
        }

  - type: sleep
    sleep:
      duration: "${!metadata:delay_for_s}s"

  # TODO: Reprocess

output:
  type: TODO
```

This works because the `awk` processor codec is set to `json`, meaning the
document is parsed as a JSON object, walked, and all fields found are set as
variables, allowing them to be referred to within the AWK program.

The `awk` processor also [has functions][awk-functions] for setting metadata,
which is used for writing our calculated sleep period. We do not print anything
with our AWK program as we do not wish to modify the contents of the document.

The `sleep` processor then simply halts the pipeline for a duration determined
through function interpolation, allowing us to specify it via the metadata key
we set.

After reprocessing we can multiplex the documents that still failed the retry
stage to a dead-letter queue similar to the first pipeline.

[catch-proc]: ../processors/README.md#catch
[awk-proc]: ../processors/README.md#awk
[awk-functions]: ../processors/awk_functions.md#metadata-functions
[http-proc]: ../processors/README.md#http
[proc-map]: ../processors/README.md#process_map
[sleep-proc]: ../processors/README.md#sleep
[error-handling-recover]: ../error_handling.md#recover-failed-messages
[error-handling-failed]: ../error_handling.md#processor-errors
[func-interp]: ../config_interpolation.md#functions
[switch]: ../outputs/README.md#switch
[broker]: ../outputs/README.md#broker
