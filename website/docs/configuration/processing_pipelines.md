---
title: Processing Pipelines
---

Within a Benthos configuration, in between `input` and `output`, is a `pipeline` section. This section describes an array of [processors][processors] that are to be applied to *all* messages, and are not bound to any particular input or output.

If you have processors that are heavy on CPU and aren't specific to a certain input or output they are best suited for the pipeline section. It is advantageous to use the pipeline section as it allows you to set an explicit number of parallel threads of execution:

```yaml
input:
  type: foo

pipeline:
  threads: 4
  processors:
    - jmespath:
        query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"

output:
  type: bar
```

If the field `threads` is set to `0` (or omitted) it will automatically match the number of logical CPUs available.

By default almost all Benthos sources will utilise as many processing threads as have been configured, which makes horizontal scaling easy. However, this configuration would not be optimal if our input isn't able to utilise >1 processing threads, which will be mentioned in its documentation ([`kafka`][kafka-input], for example).

It's also possible that the input source isn't able to provide enough traffic to fully saturate our processing threads. The following patterns can help you to achieve a distribution of work across these processing threads even under those circumstances.

### Multiple Consumers

Sometimes our source of data can have many multiple connected clients and will distribute a stream of messages amongst them. In which case it is possible to increase utilisation of parallel processing threads by adding more consumers. This can be done with a [`broker` input][broker-input]:

```yaml
input:
  broker:
    copies: 8
    inputs:
      - type: baz

pipeline:
  threads: 4
  processors:
    - jmespath:
        query: "reservations[].instances[].[tags[?Key=='Name'].Values[] | [0], type, state.name]"

output:
  type: bar
```

The disadvantage of this set up is that increasing the number of consuming clients potentially puts unnecessary stress on your data source.

### Add a Buffer

[Buffers][buffers] should be used with caution as they weaken the delivery guarantees of your pipeline. However, they can be very useful for horizontally scaling processing in cases where an input feed is sporadic, as they can level out throughput spikes and provide a backlog of messages during gaps. 

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

[processors]: /docs/components/processors/about
[jmespath-processor]: /docs/components/processors/jmespath
[split-proc]: /docs/components/processors/split
[broker-input]: /docs/components/inputs/broker
[kafka-input]: /docs/components/inputs/kafka
[buffers]: /docs/components/buffers/about