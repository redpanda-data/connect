Processing Pipelines
====================

Within a Benthos configuration, in between `input` and `output`, is a `pipeline` section. This section describes an array of [processors][processors] that are to be applied to *all* messages, and are not bound to any particular input or output.

If you have processors that are heavy on CPU and aren't specific to a certain input or output they are best suited for the pipeline section. It is advantageous to use the pipeline section as it allows you to set an explicit number of parallel threads of execution which should ideally match the number of available logical CPU cores.

For example, imagine we are consuming from a source `foo`. Our goal is to consume the stream as fast as possible, perform mutations on the JSON payload using the [`jmespath` processor][jmespath-processor], and write the resulting stream to a sink `bar`. The following patterns allow you to achieve a distribution of work across these processing threads for maximum horizontal scaling.

### Multiple Consumers

Sometimes our source of data can have many multiple connected clients and will distribute a stream of messages amongst them. In these circumstances it is possible to fully utilise a set of parallel processing threads by configuring the number of consumers to be greater than or equal to the number of threads. Ideally the number of consumers would be slightly higher than the number of threads in order to compensate for occasional IO stalls.

We can configure this arrangement with a [`broker` input][broker-input]:

```yaml
input:
  broker:
    copies: 8
    inputs:
      - type: foo
buffer:
  type: none
pipeline:
  threads: 4
  processors:
    - jmespath:
        query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"
output:
  type: bar
```

With this config the pipeline within our Benthos instance would look something like the following:

```text
baz -\
baz -\
baz ---> processors ---> bar
baz ---> processors -/
baz ---> processors -/
baz ---> processors -/
baz -/
baz -/
```

The disadvantage of this set up is that increasing the number of consuming clients potentially puts unnecessary stress on your data source.

### Single Consumer Without Buffer

Sometimes a source of data can only have a single consuming client. In these circumstances it is still possible to have the single stream of data processed on parallel processing threads and preserve our delivery guarantees. For this purpose we can do a batch and split:

```yaml
input:
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup
    max_batch_count: 100
  processors:
    - split:
        size: 25
pipeline:
  threads: 4
  processors:
    - jmespath:
        query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"
output:
  type: bar
```

When a batch at the input level is split into N smaller batches Benthos will automatically dispatch those batches in parallel across your processing threads. This action preserves transaction based resiliency. Most inputs have a native way of batching documents, if that is not the case you can precede your [`split` processor][split-proc] with a [`batch` processor][batch-proc].

With this config the pipeline within our Benthos instance would look something like the following:

```text
                  Batch       Split
kafka_balanced -> ######## -> ## ---> processors ---> bar
                              ## \--> processors -/
                              ## \--> processors -/
                              ## \--> processors -/
```

However, this pattern caps the processing time of a whole batch with the slowest processing time of the split batches, as they are locked within a transaction. If there is wide variance in the expected processing time of message batches then it's possible to mitigate this effect by combining this pattern with the previous, allowing you to utilise all threads even when the number of brokered inputs is less than the thread count.

### Single Consumer With Buffer

Sometimes batching documents up isn't a viable option, but we can still only have a single consuming client. In these circumstances it is possible to have the single stream of data processed on parallel processing threads by using a [buffer][buffers]:

```yaml
input:
  type: foo
buffer:
  memory:
    limit: 5000000
pipeline:
  threads: 4
  processors:
    - jmespath:
        query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"
output:
  type: bar
```

With this config the pipeline within our Benthos instance would look something like the following:

```text
foo -> memory buffer ---> processors ---> bar
          ( 5MB )    \--> processors -/
                     \--> processors -/
                     \--> processors -/
```

However, using a buffer weakens the delivery guarantees that Benthos provides, making data loss possible during system crashes or disk corruption. This is less of a concern when your source of data is [at-most-once][search-amo].

[processors]: ./processors/README.md
[jmespath-processor]: ./processors/README.md#jmespath
[split-proc]: ./processors/README.md#split
[batch-proc]: ./processors/README.md#batch
[broker-input]: ./inputs/README.md#broker
[broker-output]: ./outputs/README.md#broker
[buffers]: ./buffers/README.md
[search-amo]: https://duckduckgo.com/?q=at+most+once
[search-alo]: https://duckduckgo.com/?q=at+least+once