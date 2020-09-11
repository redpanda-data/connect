---
title: Configuration
sidebar_label: About
description: Learn about Benthos configuration
---

Benthos pipelines are configured in a YAML file that consists of a number of
root sections, arranged like so:

import Tabs from '@theme/Tabs';

<Tabs defaultValue="common" values={[
  { label: 'Common', value: 'common', },
  { label: 'Full', value: 'full', },
]}>

import TabItem from '@theme/TabItem';

<TabItem value="common">

```yaml
input:
  type: kafka_balanced
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  processors:
  - type: bloblang
    bloblang: |
      root.message = this
      root.meta.link_count = this.links.length()

output:
  type: s3
  s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'
```

</TabItem>
<TabItem value="full">

```yaml
http:
  address: 0.0.0.0:4195
  debug_endpoints: false

input:
  type: kafka_balanced
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

buffer:
  type: none

pipeline:
  processors:
  - type: bloblang
    bloblang: |
      root.message = this
      root.meta.link_count = this.links.length()

output:
  type: s3
  s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'

resources:
  caches: {}
  conditions: {}
  processors: {}
  rate_limits: {}

logger:
  level: INFO
  static_fields:
    '@service': benthos

metrics:
  type: prometheus

tracer:
  type: none

shutdown_timeout: 20s
```

</TabItem>

</Tabs>

Most sections represent a component type, which you can read about in more
detail in [this document][components].

These types are hierarchical. For example, an `input` can have a list of child
`processor` types attached to it, which in turn can have their own `condition`
or even more `processor` children.

This is powerful but can potentially lead to large and cumbersome configuration
files. This document outlines tooling provided by Benthos to help with writing
and managing these more complex configuration files.

### Testing

For guidance on how to write and run unit tests for your configuration files
read [this guide][config.testing].

## Concise Configuration

It's often possible to make your configuration files more concise but less
explicit by omitting the `type` field in components as well as any fields that
are default. For example, the above configuration could be written as:

```yaml
input:
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  processors:
  - bloblang: |
      root.document = this.without("links")
      root.link_count = this.links.length()

output:
  s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'
```

## Customising Your Configuration

Sometimes it's useful to write a configuration where certain fields can be
defined during deployment. For this purpose Benthos supports
[environment variable interpolation][config-interp], allowing you to set fields
in your config with environment variables like so:

```yaml
input:
  type: kafka_balanced
  kafka_balanced:
    addresses:
    - ${KAFKA_BROKER:localhost:9092}
    topics:
    - ${KAFKA_TOPIC:default-topic}
```

This is very useful for sharing configuration files across different deployment
environments.

## Reusing Configuration Snippets

It's possible to break a large configuration file into smaller parts with
[JSON references][json-references]. Benthos doesn't yet support the full
specification as it only resolves local or file path URIs, but this still allows
you to break your configs down significantly.

To reference a config snippet use the `$ref` keyword:

```yml
local_reference:
  $ref: '#/path/to/field'

file_reference:
  $ref: './foo.yaml'

file_field_reference:
  $ref: './foo.yaml#/path/to/field'
```

For example, suppose we have a configuration snippet saved under
`./config/foo.yaml`:

```yaml
pipeline:
  processors:
  - cache:
      operator: get
      key: ${! json("id") }
      resource: objects
```

And we wished to use this snippet within a larger configuration file
`./config/bar.yaml`. We can do so by adding an object with a key `$ref` and a
string value which is the path to our snippet:

```yml
pipeline:
  processors:
  - decompress:
      algorithm: gzip

  - "$ref": "./foo.yaml#/pipeline/processors/0"
```

When Benthos loads this config, it will resolve the reference, resulting in this
configuration:

```yaml
pipeline:
  processors:
  - decompress:
      algorithm: gzip

  - cache:
      operator: get
      key: ${! json("id") }
      resource: objects
```

Note that the path of a reference is relative to the configuration file
containing the reference, therefore the path used above is `./foo.yaml` and not
`./config/foo.yaml`.

If you like, these references can even be nested. 

It is further possible to use environment variables to specify which snippet 
to load. This works because environment variable interpolations within 
configurations are resolved _before_ references are resolved.

```yml
pipeline:
  processors:
  - decompress:
      algorithm: gzip

  - "$ref": "./${TARGET_SNIPPET}#/pipeline/processors/0"
```

Running the above with `TARGET_SNIPPET=foo.yaml benthos -c ./config/bar.yaml`
would be equivalent to the previous example.

## Enabling Discovery

The discoverability of configuration fields is a common headache with any
configuration driven application. The classic solution is to provide curated
documentation that is often hosted on a dedicated site.

However, a user often only needs to get their hands on a short, runnable example
config file for their use case. They just need to see the format and field names
as the fields themselves are usually self explanatory. Forcing such a user to
navigate a website, scrolling through paragraphs of text, seems inefficient when
all they actually needed to see was something like:

```yaml
input:
  type: amqp_0_9
  amqp_0_9:
    url: amqp://guest:guest@localhost:5672/
    consumer_tag: benthos-consumer
    queue: benthos-queue
    prefetch_count: 10
    prefetch_size: 0
output:
  type: stdout
```

In order to make this process easier Benthos is able to generate usable
configuration examples for any types, and you can do this from the binary using
the `create` subcommand.

If, for example, we wanted to generate a config with a websocket input, a Kafka
output and a Bloblang processor in the middle, we could do it with the following
command:

```text
benthos create websocket/bloblang/kafka
```

> If you need a gentle reminder as to which components Benthos offers you can see those as well with `benthos list`.

All of these generated configuration examples also include other useful config
sections such as `metrics`, `logging`, etc with sensible defaults.

For more information read the output from `benthos create --help`.

## Help With Debugging

Once you have a config written you now move onto the next headache of proving
that it works, and understanding why it doesn't. Benthos, like most good config
driven services, performs validation on configs and tries to provide sensible
error messages.

However, with validation it can be hard to capture all problems, and the user
usually understands their intentions better than the service. In order to help
expose and diagnose config errors Benthos provides two mechanisms, linting and
echoing.

### Linting

If you attempt to run a config that has linting errors Benthos will print the
errors and halt execution. If, however, you want to test your configs before
deployment you can do so with the `lint` subcommand:

For example, imagine we have a config `foo.yaml`, where we intend to read from
AMQP, but there is a typo in our config struct:

```yaml
input:
  type: amqp_0_9
  amqp_0_9:
    url: amqp://guest:guest@rabbitmqserver:5672/
```

We can catch this error before attempting to run the config:

```sh
$ benthos lint ./foo.yaml
./foo.yaml: input: Key 'amqq_0_9' found but is ignored
```

For more information read the output from `benthos lint --help`.

### Echoing

Echoing is where Benthos can print back your configuration _after_ it has been
parsed. It is done with the `echo` subcommand, which is able to show you a
normalised version of your config, allowing you to see how it was interpreted:

```sh
benthos -c ./your-config.yaml echo
```

You can check the output of the above command to see if certain sections are
missing or fields are incorrect, which allows you to pinpoint typos in the
config.

[processors]: /docs/components/processors/about
[conditions]: /docs/components/conditions/about
[config-interp]: /docs/configuration/interpolation
[config.testing]: /docs/configuration/unit_testing
[json-references]: https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03
[components]: /docs/components/about