---
title: Windowed Processing
---

There are many ways of performing windowed or aggregated message processing with the wide range of [connectors and processors][processors] Benthos offers, but this usually relies on aggregating messages in transit with a cache or database.

Instead, this document outlines the simplest way of performing windowed processing in Benthos, which is to use input level [batching][batching].

## Creating Batches

Firstly, we will need to create batches of messages _before_ our processing stages. This batch mechanism is what creates our window of messages, which we can later process as a group.

Some [inputs][inputs] natively support batching, otherwise we can wrap them within a [broker][input-broker]:

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs defaultValue="kafka" values={[
  { label: 'Kafka', value: 'kafka', },
  { label: 'NATS (Broker)', value: 'nats', },
]}>
<TabItem value="kafka">

```yaml
input:
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup
    batching:
      count: 50
      period: 30s
```

</TabItem>
<TabItem value="nats">

```yaml
input:
  broker:
    inputs:
    - nats:
        urls:
        - nats://TODO
        queue: benthos_queue
        subject: foosubject
    batching:
      count: 50
      period: 30s
```

</TabItem>
</Tabs>

> NOTE: Batching here doesn't mean we _have_ to output messages as a batch. After processing we can break this batch out and even re-batch with different settings if we want.

When a [batching policy][batching-policy] is defined at the input level it means inputs will consume messages and aggregate them until the batch is complete, at which point it is flushed downstream to your processors and subsequently your outputs.

Tune the batch parameters to suit the size (or time interval, etc) of window you require.

## Grouping

Once our messages are batched we have one large but general window of messages. Depending on our use case we may wish to divide them into groups based on their contents. For that purpose we have two processor options: [`group_by`][group-by-proc] and [`group_by_value`][group-by-value-proc].

For example, we can break our window out into groups based on the messages Kafka key:

```yaml
pipeline:
  processors:
  - group_by_value:
      value: ${!metadata:kafka_key}
```

## Aggregating

The main purpose of windowing messages is so they can be aggregated into a single message that summarises the window. For this purpose we have lots of options within Benthos, but for this guide we'll cover a select few.

### Flat Counter

The easiest aggregation to perform is simply counting how many messages were within the window. In Benthos you can extract that value with the [interpolation function][interpolation] `batch_size`, which allows us to insert the value into the first message of our group and then remove all other messages.

Here's an example using the [`json`][json-proc] and [`select_parts`][select-parts-proc] processors, where we inject it as a new field in an existing JSON document:

```yaml
pipeline:
  processors:
  # TODO: Paste group processor here if you want it.

  # Set the value of doc.count to the batch size.
  - json:
      parts: [ 0 ] # Only bother running this for the first message.
      operator: set
      path: doc.count
      value: ${!batch_size}

  # Drop all messages except the first.
  - select_parts:
      parts: [ 0 ]
```

But you can just as easily inject a `batch_size` value using processors such as [`text`][text-proc] when working with unstructured data.

### Real Counter

If you have a group of structured documents containing numeric values that you wish to count then it gets slightly trickier. For brevity we're going to assume our messages are JSON documents of the format:

```json
{"doc":{"count":5,"contents":"foobar"}}
```

We can simply merge all of the JSON documents and fold the values of `doc.count`:

```yaml
pipeline:
  processors:
  # TODO: Paste group processor here if you want it.

  # Create a new document by merging all documents of the batch. This results in
  # all leaves of the document being converted into arrays.
  - merge_json:
      retain_parts: false

  # Fold the counts array of the merged doc.
  - json:
      operator: fold_number_array
      path: doc.count
```

This results in a document containing all values of the group in arrays, except the count which we explicitly folded:

```json
{
  "doc": {
    "count": 243,
    "contents": ["foobar","another value","and another"]
  }
}
```

We can easily structure the resulting document with a [`jmespath` processor][jmespath-proc] in order to reduce its size. If, however, we needed to preserve the exact structure of the first message, without contamination from other documents of the group, then we can do that with a slightly longer configuration:

```yaml
pipeline:
  processors:
  # TODO: Paste group processor here if you want it.

  # Create a new document by merging all documents of the batch, and append the
  # result to the end of the batch.
  - merge_json:
      retain_parts: true

  # Fold the counts array of the merged doc.
  - json:
      parts: [ -1 ] # Only calculate for last message (the merged one)
      operator: fold_number_array
      path: doc.count

  # Inject the folded value from the last message into the first. This is
  # possible because interpolation functions are resolved batch wide, so we can
  # specify an optional message index after our path.
  - json:
      parts: [ 0 ] # Only inject into the first message.
      operator: set
      path: doc.count
      value: ${!json_field:doc.count,-1}

  # Using interpolation above results in a string, so we parse that back into a
  # number.
  - process_field:
      parts: [ 0 ] 
      path: doc.count
      result_type: float

  # Drop all messages except the first.
  - select_parts:
      parts: [ 0 ]
```

It ain't pretty, but it gets the job done.

[interpolation]: /docs/configuration/interpolation
[batching]: /docs/configuration/batching
[batching-policy]: /docs/configuration/batching#batch-policy
[processors]: /docs/components/processors/about
[jmespath-proc]: /docs/components/processors/jmespath
[json-proc]: /docs/components/processors/json
[text-proc]: /docs/components/processors/text
[select-parts-proc]: /docs/components/processors/select_parts
[group-by-proc]: /docs/components/processors/group_by
[group-by-value-proc]: /docs/components/processors/group_by_value
[inputs]: /docs/components/inputs/about
[input-broker]: /docs/components/inputs/broker