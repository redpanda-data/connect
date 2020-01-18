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
  - type: jmespath
    jmespath:
      query: '{ message: @, meta: { link_count: length(links) } }'

output:
  type: s3
  s3:
    bucket: TODO
    path: "${!metadata:kafka_topic}/${!json_field:message.id}.json"
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
  - type: jmespath
    jmespath:
      query: '{ message: @, meta: { link_count: length(links) } }'

output:
  type: s3
  s3:
    bucket: TODO
    path: "${!metadata:kafka_topic}/${!json_field:message.id}.json"

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


Config examples for every input, output and processor type can be found
[here](https://github.com/Jeffail/benthos/tree/master/config).

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
  - jmespath:
      query: '{ message: @, meta: { link_count: length(links) } }'

output:
  s3:
    bucket: TODO
    path: "${!metadata:kafka_topic}/${!json_field:message.id}.json"
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

```yaml
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
      key: ${!json_field:id}
      cache: objects
```

And we wished to use this snippet within a larger configuration file
`./config/bar.yaml`. We can do so by adding an object with a key `$ref` and a
string value which is the path to our snippet:

```yaml
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
      key: ${!json_field:id}
      cache: objects
```

Note that the path of a reference is relative to the configuration file
containing the reference, therefore the path used above is `./foo.yaml` and not
`./config/foo.yaml`.

If you like, these references can even be nested. 

It is further possible to use environment variables to specify which snippet 
to load. This works because environment variable interpolations within 
configurations are resolved _before_ references are resolved.

```yaml
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
  type: amqp
  amqp:
    url: amqp://guest:guest@localhost:5672/
    consumer_tag: benthos-consumer
    exchange: benthos-exchange
    exchange_type: direct
    key: benthos-key
    prefetch_count: 10
    prefetch_size: 0
    queue: benthos-queue
output:
  type: stdout
```

In order to make this process easier Benthos is able to generate usable
configuration examples for any types, and you can do this from the binary using
the `--example` flag in combination with `--print-yaml` or `--print-json`. If,
for example, we wanted to generate a config with a websocket input, a Kafka
output and a JMESPath processor in the middle, we could do it with the following
command:

```yaml
benthos --print-yaml --example websocket,kafka,jmespath
```

All of these generated configuration examples also include other useful config
sections such as `metrics`, `logging`, etc with sensible defaults.

### Printing Every Field

The format of a Benthos config file naturally exposes all of the options for a
section when it's printed with all default values. For example, in a fictional
section `foo`, which has type options `bar`, `baz` and `qux`, if you were to
print the entire default `foo` section of a config it would look something like
this:

```yaml
foo:
  type: bar
  bar:
    field1: default_value
    field2: 2
  baz:
    field3: another_default_value
  qux:
    field4: false
```

Which tells you that section `foo` supports the three object types `bar`, `baz`
and `qux`, and defaults to type `bar`. It also shows you the fields that each
section has, and their default values.

The Benthos binary is able to print a JSON or YAML config file containing every
section in this format with the commands `benthos --print-yaml --all` and
`benthos --print-json --all`. This can be extremely useful for quick and dirty
config discovery when the full repo isn't at hand.

As a user you could create a new config file with:

```sh
benthos --print-yaml --all > conf.yaml
```

And simply delete all lines for sections you aren't interested in, then you are
left with the full set of fields you want.

Alternatively, using tools such as [jq][jq] you can extract specific type
fields:

```sh
# Get a list of all input types:
benthos --print-json --all | jq '.input | keys'

# Get all Kafka input fields:
benthos --print-json --all | jq '.input.kafka'

# Get all AMQP output fields:
benthos --print-json --all | jq '.output.amqp'

# Get a list of all processor types:
benthos --print-json --all | jq '.pipeline.processors[0] | keys'

# Get all JSON processor fields:
benthos --print-json --all | jq '.pipeline.processors[0].json'
```

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

Benthos has a lint command (`--lint`) that, after parsing a config file, will
print any errors it detects.

The main goal of the linter is to expose instances where fields within a
provided config are valid JSON or YAML but don't actually affect the behaviour
of Benthos. These are useful for pointing out typos in object keys or the use of
deprecated fields.

For example, imagine we have a config `foo.yaml`, where we intend to read from
AMQP, but there is a typo in our config struct:

```yaml
input:
  type: amqp
  amqq:
    url: amqp://guest:guest@rabbitmqserver:5672/
```

This config is parse successfully, and Benthos will simply ignore the `amqq` key
and run using default values for the `amqp` input. This is therefore an easy
error to miss, but if we use the linter it will immediately report the problem:

```sh
$ benthos -c ./foo.yaml --lint
input: Key 'amqq' found but is ignored
```

Which points us to exactly where the problem is.

### Echoing

Echoing is where Benthos can print back your configuration _after_ it has been
parsed. It is done with the `--print-yaml` and `--print-json` commands, which
print the Benthos configuration in YAML and JSON format respectively. Since this
is done _after_ parsing and applying your config it is able to show you exactly
how your config was interpretted:

```sh
benthos -c ./your-config.yaml --print-yaml
```

You can check the output of the above command to see if certain sections are
missing or fields are incorrect, which allows you to pinpoint typos in the
config.

If your configuration is complex, and the behaviour that you notice implies a
certain section is at fault, then you can drill down into that section by using
tools such as [jq][jq]:

```sh
# Check the second processor config
benthos -c ./your-config.yaml --print-json | jq '.pipeline.processors[1]'

# Check the condition of a filter processor
benthos -c ./your-config.yaml --print-json | jq '.pipeline.processors[0].filter'
```

[processors]: /docs/components/processors/about
[conditions]: /docs/components/conditions/about
[config-interp]: interpolation
[config.testing]: unit_testing
[json-references]: https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03
[jq]: https://stedolan.github.io/jq/