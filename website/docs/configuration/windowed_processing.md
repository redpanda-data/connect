---
title: Windowed Processing
---

There are many ways of performing windowed or aggregated message processing with the wide range of [connectors and processors][processors] Benthos offers, but this usually relies on aggregating messages in transit with a cache or database.

Instead, this document outlines the simplest way of performing tumbling window processing in Benthos, which is to use input level [batching][batching]. There are plans to eventually offer other windowing mechanisms such as hopping or sliding and these will behave similarly.

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
  kafka:
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
      value: ${! meta("kafka_key") }
```

## Aggregating

The main purpose of windowing messages is so they can be aggregated into a single message that summarises the window. For this purpose we have lots of options within Benthos, but for this guide we'll cover a select few, where each example uses [Bloblang][bloblang].

### Flat Counter

The easiest aggregation to perform is simply counting how many messages were within the window. This is easy to do with the [`bloblang` processor][processors.bloblang] using the [`batch_size` function][bloblang.functions.batch_size]:

```yaml
pipeline:
  processors:
  # TODO: Paste group processor here if you want it.

  # Set the value of doc.count to the batch size.
  - bloblang: |
      root = this
      doc.count = batch_size()

      # Drop all documents except the first.
      root = match {
        batch_index() > 0 => deleted()
      }
```

### Real Counter

If you have a group of structured documents containing numeric values that you wish to count then that's also pretty easy with Bloblang. For brevity we're going to assume our messages are JSON documents of the format:

```json
{"doc":{"count":5,"contents":"foobar"}}
```

And that we only wish to preserve the first message of the batch. We can do this by extracting the `doc.count` value of each document into an array with the method [`from_all`][bloblang.methods.from_all] and adding them with the method [`sum`][bloblang.methods.sum]:

```yaml
pipeline:
  processors:
  # TODO: Paste group processor here if you want it.

  - bloblang: |
      root = this
      doc.count = json("doc.count").from_all().sum()

      # Drop all documents except the first.
      root = match {
        batch_index() > 0 => deleted()
      }
```

This results in a document containing our aggregated count, along with the rest of the first document of the batch:

```json
{
  "doc": {
    "count": 243,
    "contents": "foobar"
  }
}
```

### Custom Folding

Bloblang also has a method [`fold`][bloblang.methods.fold] which allows you to write custom folding logic for your values. Here's an example where we implement a max function for our counts:

```yaml
pipeline:
  processors:
  # TODO: Paste group processor here if you want it.

  - bloblang: |
      root = this
      doc.max = json("doc.count").from_all().fold(0, match {
        tally < value => value
        _ => tally
      })

      # Drop all documents except the first.
      root = match {
        batch_index() > 0 => deleted()
      }
```

[Bloblang][bloblang] is very powerful, and by using [`from`][bloblang.methods.from] and [`from_all`][bloblang.methods.from_all] it's possible to perform a wide range of batch-wide processing.

[batching]: /docs/configuration/batching
[batching-policy]: /docs/configuration/batching#batch-policy
[processors]: /docs/components/processors/about
[bloblang]: /docs/guides/bloblang/about
[bloblang.functions.batch_size]: /docs/guides/bloblang/functions#batch_size
[bloblang.methods.from_all]: /docs/guides/bloblang/methods#from_all
[bloblang.methods.from]: /docs/guides/bloblang/methods#from
[bloblang.methods.sum]: /docs/guides/bloblang/methods#sum
[bloblang.methods.fold]: /docs/guides/bloblang/methods#fold
[processors.bloblang]: /docs/components/processors/bloblang
[group-by-proc]: /docs/components/processors/group_by
[group-by-value-proc]: /docs/components/processors/group_by_value
[inputs]: /docs/components/inputs/about
[input-broker]: /docs/components/inputs/broker