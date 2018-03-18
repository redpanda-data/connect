Benthos
=======

## Contents

- [1. Configuration](#configuration)
- [2. Mutating And Filtering Content](#mutating-and-filtering-content)
- [3. Content Based Multiplexing](#content-based-multiplexing)
- [4. Maximising IO Throughput](#maximising-io-throughput)
- [5. Maximising CPU Utilisation](#maximising-cpu-utilisation)

## Configuration

Sections of a Benthos configuration take the form of an object with a `type` and
`type` specific fields under the same field namespace as the content of the
`type` field.

In order to illustrate let's imagine a hypothetical config section `foo`. We
want our `foo` to have the `type` of `bar`. Our `bar` type has its own specific
fields `size` and `should_block` which will also be set:

``` yaml
foo:
  type: bar
  bar:
    size: 5
    should_block: true
```

This format means that when we print a default Benthos configuration the
potential types of our `foo` are listed as fields of its object along with their
own config fields, making it easier to discover types. For example, if we were
to print a default config for `foo` it might look like this:

``` yaml
foo:
  type: bar
  bar:
    size: 5
    should_block: true
  baz:
    sample: 0.1
  qux:
    delete_objects: false
```

Allowing us to infer that `foo` can have one of the following types: `bar`,
`baz` and `qux`. We can also see the specific config fields relevant to those
types. The full default config for Benthos can [be found here][default-conf]
and, although long, shows you every type and field available to you.

A Benthos configuration consists of a number of root sections, the key parts
being:

- [`input`](./inputs)
- [`buffer`](./buffers)
- [`output`](./inputs)

Please refer to those links for more information.

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
[condition processor][condition-processor] on each output, which is a processor
that drops messages if the condition does not pass. Conditions are content aware
logical operators that can be combined using boolean logic.

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
      - type: condition
        condition:
          type: content
          content:
            operator: contains
            part: 0
            arg: foo
    - type: bar
      bar:
        bar_field_1: value2
        bar_field_2: value3
      processors:
      - type: condition
        condition:
          type: not
          not:
            type: content
            content:
              operator: contains
              part: 0
              arg: foo
```

For more information regarding conditions, including the list of conditions
available, please [read the docs here][conditions].

## Maximising IO Throughput

TODO

## Maximising CPU Utilisation

TODO

[default-conf]: ../../config/everything.yaml
[processors]: ./processors
[broker-output]: ./outputs/README.md#broker
[condition-processor]: ./processors/README.md#condition
[conditions]: ./conditions
