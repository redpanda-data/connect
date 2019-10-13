Kafka JSON Mutating
===================

In this example we start off with a stream of JSON data being written to a Kafka
topic. Our task is to consume the stream, mutate the data into a new format,
filter certain items, and write the stream to a new topic.

For this example the data will be social media interactions with NLP enrichments
such as sentiment and language detection, it looks something like this:

``` json
{
  "content": {
    "text": "This ist a public service announcement: I hate Jerry, Kate is okay.",
    "created_at": 1524599100
  },
  "language":[
    {
      "confidence": 0.07,
      "code": "de"
    },
    {
      "confidence": 0.93,
      "code": "en"
    }
  ],
  "sentiment": [
    {
       "entity": "jerry",
       "level": "negative",
       "confidence": 0.45
    },
    {
       "entity": "kate",
       "level": "neutral",
       "confidence": 0.08
    }
  ]
}
```

And we wish to mutate the data such that irrelevant data is removed.
Specifically, we want to reduce the language detection candidates down to a
single language code, and we want to remove sentiment entities with a confidence
below a certain level (0.3). The desired output from the above sample would be:

``` json
{
  "content": {
    "created_at": 1524599100,
    "text": "This ist a public service announcement: I hate Jerry, Kate is okay."
  },
  "entities": [
    {
      "name": "jerry",
      "sentiment": "negative"
    }
  ],
  "language": "en"
}
```

Which we can accomplish using the [`JMESPath` processor][jmespath-processor].

The main factor of concern in this example is throughput, it needs to be as high
as possible, but we also need to preserve at-least-once delivery guarantees.

The full config for this [example can be found here][example].

Since the pipeline is mostly going to be throttled by CPU the mutations execute
on processing threads matching the number of logical CPUs. In order to keep the
processing threads busy with traffic there are parameters in the config for
tuning the number of concurrent readers to reduce input IO stalling. There is
also a parameter for the message batch size, which can tuned in order to
increase the throughput of the output in case it becomes the bottleneck.

## Input

``` yaml
input:
  broker:
    count: 8 # Try cranking this value up if your CPUs aren't maxed out
    inputs:
    - kafka_balanced:
        addresses:
        - localhost:9092 # TODO
        client_id: benthos_mutator_1
        consumer_group: benthos_mutator_group
        topics:
        - data_stream
        batching:
          count: 8 # Batch size: Tune this to increase output throughput
          period: 1s
```

The `kafka_balanced` input is used here, which automatically distributes the
partitions of a topic across all consumers of the same consumer group. This
allows us to have as many consumers both inside the process and as separate
services as we need. It should be noted that the number of Kafka partitions for
this topic should be significantly higher than the total number of consumers in
order to get a good distribution of messages.

Using a broker allows us to tune the number of parallel consumers inside the
process in order to reach our maximum CPU utilisation.

We also specify a `batching.count`, which is the maximum number of messages to
batch together when we have a backlog of prefetched messages. The advantage of
this is that a batch will be sent as a single request on the Kafka producer
output. By tuning the batch size we should be able to increase our output
throughput to prevent it from bottlenecking the pipeline.

## Pipeline

``` yaml
pipeline:
  threads: 4 # This should match the number of logical CPUs
  processors:
  - jmespath:
      query: |
        {
          content: content,
          entities: sentiment[?confidence > `0.3`].{
            name: entity,
            sentiment: level
          },
          language: max_by(language, &confidence).code
        }
```

The pipeline is where we construct our processing pipelines. We set the number
of threads to match our available logical CPUs in order to avoid contention. You
might find slightly increasing or decreasing this number can improve throughput
due to competing services on the machine.

The number of input consumers needs to be significantly higher than the number
of threads here in order to prevent IO stalling.

The only processor defined here is our JMESPath query, which is where our JSON
mutation comes from. You can [read more about JMESPath here][jmespath].

## Output

Our output is a standard Kafka producer output.

[jmespath]: http://jmespath.org/
[jmespath-processor]: ../processors/README.md#jmespath
[example]: ./kafka-json-mutating.yaml
