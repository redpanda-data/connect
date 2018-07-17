Benthos Concepts
================

## Contents

1. [Configuration](#configuration)
2. [Mutating And Filtering Content](#mutating-and-filtering-content)
3. [Content Based Multiplexing](#content-based-multiplexing)
4. [Sharing Resources Across Processors](#sharing-resources-across-processors)
5. [Maximising IO Throughput](#maximising-io-throughput)
6. [Maximising CPU Utilisation](#maximising-cpu-utilisation)

## Configuration

A Benthos configuration consists of a number of root sections, the key parts
being:

- [`input`](./inputs)
- [`buffer`](./buffers)
- [`pipeline`](./pipeline.md)
- [`output`](./outputs)

There are also sections for `metrics`, `logging` and `http` server options.
Config examples for every input, output and processor type can be found
[here](../config).

Benthos provides lots of tooling to try and make writing configuration easier,
you can read about them [here](./configuration.md).

## Mutating And Filtering Content

Benthos bridges different transport and storage mediums of data, but these often
require the data to be represented in different ways. For example, we might be
reading `.tgz` archives of messages from Amazon S3, but we need to decompress
and unarchive the messages before sending them to Kafka. For this purpose we
can use processors, which you [can read about in more detail here][processors].

Processors can be attributed to both inputs and outputs, meaning you can be
specific about which processors apply to data from specific sources or to
specific sinks.

## Content Based Multiplexing

It is possible to perform content based multiplexing of messages to specific
outputs using [an output broker with the `fan_out` pattern][broker-output] and a
[filter processor][filter-processor] on each output, which is a processor
that drops messages if the [condition][conditions] does not pass.
[Conditions][conditions] are content aware logical operators that can be
combined using boolean logic.

For example, say we have an output `foo` that we only want to receive messages
that contain the word `foo`, and an output `bar` that we wish to send everything
that `foo` doesn't receive, we can achieve that with this config:

``` yaml
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        foo_field_1: value1
      processors:
      - type: filter
        filter:
          type: text
          text:
            operator: contains
            part: 0
            arg: foo
    - type: bar
      bar:
        bar_field_1: value2
        bar_field_2: value3
      processors:
      - type: filter
        filter:
          type: not
          not:
            type: text
            text:
              operator: contains
              part: 0
              arg: foo
```

For more information regarding filter conditions, including the full list of
conditions available, please [read the docs here][conditions].

## Sharing Resources Across Processors

Sometimes it is advantageous to share configurations for resources such as
caches or complex conditions between processors when they would otherwise be
duplicated. For this purpose there is a `resource` section in a Benthos config
where [caches][caches] and [conditions][conditions] can be configured to a label
that is referred to by any processors/conditions that wish to use them.

For example, let's imagine we have three inputs, two of which we wish to
deduplicate using a shared cache. We also have two outputs, one of which only
receives messages that satisfy a condition and the other receives the logical
NOT of that same condition. In this example we can save ourselves the trouble of
configuring the same cache and condition twice by referring to them as resources
like this:

``` yaml
input:
  type: broker
  broker:
    inputs:
    - type: foo
      processors:
      - type: dedupe
        dedupe:
          cache: foobarcache
          hash: none
          parts: [0]
    - type: bar
      processors:
      - type: dedupe
        dedupe:
          cache: foobarcache
          hash: none
          parts: [0]
    - type: baz
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: quz
      quz:
        processors:
        - type: filter
          filter:
            type: resource
            resource: foobarcondition
    - type: qux
      qux:
        processors:
        - type: filter
          filter:
            type: not
            not:
              type: resource
              resource: foobarcondition
resources:
  caches:
    foobarcache:
      type: memcached
      memcached:
        addresses:
        - localhost:11211
        ttl: 60
  conditions:
    foobarcondition:
      type: text
      text:
        operator: equals_cs
        part: 1
        arg: filter me please
```

It is also worth noting that when conditions are used as resources in this way
they will only be executed once per message, regardless of how many times they
are referenced (unless the content is modified). Therefore, resource conditions
can act as a runtime optimisation as well as a config optimisation.

## Maximising IO Throughput

This section assumes your Benthos instance is doing minimal or zero processing,
and therefore has minimal reliance on your CPU resource. Even if this is not the
case the following still applies to an extent, but you should also refer to
[the next section regarding CPU utilisation](#maximising-cpu-utilisation)

Building a high throughput platform is an endless topic, instead this section
outlines a few common throughput issues and ways in which they can be solved
within Benthos.

Before venturing into Benthos configurations you should first take an in-depth
look at your sources and sinks. Benthos is generally much simpler
architecturally than the inputs and outputs it supports. Spend some time
understanding how to squeeze the most out of these services and it will make it
easier (or unnecessary) to tune your bridge within Benthos.

### Benthos Reads Too Slowly

If Benthos isn't reading fast enough from your source it might not necessarily
be due to a slow consumer. If the sink is slow this can cause back pressure that
throttles the amount Benthos can read. Try replacing the output with `stdout`
and pipe it to `/dev/null` (or use `file` with the path set to `/dev/null`). If
you notice that the input suddenly speeds up then the issue is likely with the
output, in which case [try the next section](#benthos-writes-too-slowly).

If the `/dev/null` output pipe didn't help then take a quick look at the basic
configuration fields for the input source type. Sometimes there are fields for
setting a number of background prefetches or similar concepts that can increase
your throughput. For example, increasing the value of `prefetch_count` for an
AMQP consumer can greatly increase the rate at which it is consumed.

Next, if your source supports multiple parallel consumers then you can try doing
that within Benthos by using a [broker][broker-input]. For example, if you
started with:

``` yaml
input:
  type: foo
  foo:
    field1: etc
```

You could change to:

``` yaml
input:
  type: broker
  broker:
    copies: 4
    inputs:
    - type: foo
      foo:
        field1: etc
```

Which would create the exact same consumer as before with four copies in total.
Try increasing the number of copies to see how that affects the throughput. If
your multiple consumers would require different configurations then set copies
to `1` and write each consumer as a separate object in the `inputs` array.

Read the [broker documentation][broker-input] for more tips on simplifying
broker configs.

If your source doesn't support multiple parallel consumers then unfortunately
your options are limited. A logical next step might be to look at your
network/disk configuration to see if that's a potential cause of contention.

### Benthos Writes Too Slowly

If you have an output sink that regularly places back pressure on your source
there are a few solutions depending on the details of the issue.

Firstly, you should check the config parameters of your output sink. There are
often fields specifically for controlling the level of acknowledgement to expect
before moving onto the next message, if these levels of guarantee are overkill
you can disable them for greater throughput. For example, setting the
`ack_replicas` field to `false` in the Kafka sink can have a high impact on
throughput.

If the config parameters for an output sink aren't enough then you can try the
following:

#### Send messages in batches

Some output sinks do not support multipart messages and when receiving one will
send each part as an individual message as a batch (the Kafka output will do
this). You can use this to your advantage by using the `combine` processor to
create batches of messages to send.

For example, given the following input and output combination:

``` yaml
input:
  type: foo
output:
  type: kafka
```

This bridge will send messages one at a time, wait for acknowledgement from the
output and propagate that acknowledgement to the input. Instead, using this
config:

``` yaml
input:
  type: foo
  processors:
  - type: combine
    combine:
      parts: 8
output:
  type: kafka
```

The bridge will read 8 messages from the input, send those 8 messages to the
output as a batch, receive the acknowledgement from the output for all messages
together, then propagate the acknowledgement for all those messages to the input
together.

Therefore, provided the input is able to send messages and acknowledge them
outside of lock-step (or doesn't support acknowledgement at all), you can
improve throughput without losing delivery guarantees.

#### Level out input spikes with a buffer

There are many reasons why an input source might have spikes or inconsistent
throughput rates. It is possible that your output is capable of keeping up with
the long term average flow of data, but fails to keep up when an intermittent
spike occurs.

In situations like these it is sometimes a better use of your hardware and
resources to level out the flow of data rather than try and match the peak
throughput. This would depend on the frequency and duration of the spikes as
well as your latency requirements, and is therefore a matter of judgement.

Leveling out the flow of data can be done within Benthos using a
[buffer][buffers]. Buffers allow an input source to store a bounded amount of
data temporarily, which a consumer can work through at its own pace. Buffers
always have a fixed capacity, which when full will proceed to block the input
just like a busy output would.

Therefore, it's still important to have an output that can keep up with the flow
of data, the difference that a buffer makes is that the output only needs to
keep up with the _average_ flow of data versus the instantaneous flow of data.

If your input usually produces 10 msgs/s, but occasionally spikes to 100 msgs/s,
and your output can handle up to 50 msgs/s, it might be possible to configure a
buffer large enough to store spikes in their entirety. As long as the average
flow of messages from the input remains below 50 msgs/s then your bridge should
be able to continue indefinitely without ever blocking the input source.

Benthos offers [a range of buffer strategies][buffers] and it is worth studying
them all in order to find the correct combination of resilience, throughput and
capacity that you need.

#### Increase the number of parallel output sinks

If your output sink supports multiple parallel writers then it can greatly
increase your throughput to have multiple outputs configured. However, one thing
to keep in mind is that due to the lock-step of reading/sending/acknowledging of
a Benthos bridge, if the number of output writers exceeds the number of input
consumers you will need a [buffer][buffers] between them in order to keep all
outputs busy, the buffer doesn't need to be large.

Increasing the number of parallel output sinks is similar to doing the same for
input sources and is done using a [broker][broker-output]. The output broker
type supports a few different routing patterns depending on your intention. In
this case we want to maximize throughput so our best choice is a `greedy`
pattern. For example, if you started with:

``` yaml
output:
  type: foo
  foo:
    field1: etc
```

You could change to:

``` yaml
output:
  type: broker
  broker:
    pattern: greedy
    copies: 4
    outputs:
    - type: foo
      foo:
        field1: etc
```

Which would create the exact same output writer as before with four copies in
total. Try increasing the number of copies to see how that affects the
throughput. If your multiple output writers would require different
configurations (client ids, for example) then set copies to `1` and write each
consumer as a separate object in the `outputs` array.

Read the [broker documentation][broker-output] for more tips on simplifying
broker configs.

## Maximising CPU Utilisation

Some [processors][processors] within Benthos are relatively heavy on your CPU,
and can potentially become the bottleneck of a bridge. In these circumstances
it is worth configuring your bridge so that your processors are running on each
available core of your machine without contention.

An array of processors in any section of a Benthos config becomes a single
logical pipeline of steps running on a single logical thread.

When the target of the processors (an input or output) is a broker type the
pipeline will be duplicated once for each discrete input/output. This is one way
to create parallel processing threads but they will be tightly coupled to the
input or output they are bound to. Using processing pipelines in this way
results in uneven and varying loads which is unideal for distributing processing
work across logical CPUs.

The other way to create parallel processor threads is to configure them inside
the [pipeline][pipeline] configuration block, where we can explicitly set any
number of parallel processor threads independent of how many inputs or outputs
we want to use. If the number of inputs is less than or close to the number of
processing threads then it is also important to use a [buffer][buffers] in order
to decouple those inputs.

Please refer [to the documentation regarding pipelines][pipeline] for some
examples.

[default-conf]: ../../config/everything.yaml
[pipeline]: ./pipeline.md
[processors]: ./processors
[buffers]: ./buffers
[broker-input]: ./inputs/README.md#broker
[broker-output]: ./outputs/README.md#broker
[filter-processor]: ./processors/README.md#filter
[conditions]: ./conditions
[caches]: ./caches
