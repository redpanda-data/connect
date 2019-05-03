Configuration
=============

A Benthos stream is configured either in a YAML or JSON file using a
hierarchical format. For a basic stream pipeline that means the section for,
say, an input is very simple:

``` yaml
input:
  type: kafka
  kafka:
    topic: foo
    partition: 0
    addresses:
      - localhost:9092
```

However, as configurations become more complex this format can sometimes be
difficult to read and manage:

``` yaml
input:
  type: kafka
  kafka:
    ...
  processors:
    - type: conditional
      conditional:
        condition:
          type: jmespath
          jmespath:
            query: contains(foo.bar, "compress me")
        processors:
          - type: compress
            compress:
              algorithm: gzip
```

The above example reads messages from Kafka and, if the JSON path `foo.bar`
contains the phrase "compress me" the entire message will be compressed with
`gzip`, otherwise it passes unchanged.

Allowing arbitrary hierarchies of [processors][processors] and
[conditions][conditions] like this is powerful, but increases the likelihood of
issues being introduced by typos.

This document outlines tooling provided by Benthos to help with writing and
managing these more complex configuration files.

## Contents

- [Enabling Discovery](#enabling-discovery)
- [Help With Debugging](#help-with-debugging)
- [Fragmenting Your Configuration](#fragmenting-your-configuration)

## Enabling Discovery

The discoverability of configuration fields is a common headache with any
configuration driven application. The classic solution is to provide curated
documentation that is often hosted on a dedicated site. Benthos does this by
generating a markdown document
[per configuration section](./README.md#core-components).

However, a user often only needs to get their hands on a short, runnable example
config file for their use case. They just need to see the format and field names
as the fields themselves are usually self explanatory. Forcing such a user to
navigate a website, scrolling through paragraphs of text, seems inefficient when
all they actually needed to see was something like:

``` yaml
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

``` yaml
benthos --print-yaml --example websocket,kafka,jmespath
```

There are also examples within the [config directory](../config), where there is
a config file for each input and output type, and inside the
[processors](../config/processors) subdirectory there is a file showing each
processor type, and so on.

All of these generated configuration examples also include other useful config
sections such as `metrics`, `logging`, etc with sensible defaults.

### Printing Every Field

The format of a Benthos config file naturally exposes all of the options for a
section when it's printed with all default values. For example, in a fictional
section `foo`, which has type options `bar`, `baz` and `qux`, if you were to
print the entire default `foo` section of a config it would look something like
this:

``` yaml
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

``` sh
benthos --print-yaml --all > conf.yaml
```

And simply delete all lines for sections you aren't interested in, then you are
left with the full set of fields you want.

Alternatively, using tools such as `jq` you can extract specific type fields:

``` sh
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
expose and diagnose config errors Benthos provides two mechanisms, linting and echoing.

### Linting

Benthos has a lint command (`--lint`) that, after parsing a config file, will
print any errors it detects.

The main goal of the linter is to expose instances where fields within a
provided config are valid JSON or YAML but don't actually affect the behaviour
of Benthos. These are useful for pointing out typos in object keys or the use of
deprecated fields.

For example, imagine we have a config `foo.yaml`, where we intend to read from
AMQP, but there is a typo in our config struct:

``` yaml
input:
  type: amqp
  amqq:
    url: amqp://guest:guest@rabbitmqserver:5672/
```

This config is parse successfully, and Benthos will simply ignore the `amqq` key
and run using default values for the `amqp` input. This is therefore an easy
error to miss, but if we use the linter it will immediately report the problem:

``` sh
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

``` sh
benthos -c ./your-config.yaml --print-yaml
```

You can check the output of the above command to see if certain sections are
missing or fields are incorrect, which allows you to pinpoint typos in the
config.

If your configuration is complex, and the behaviour that you notice implies a
certain section is at fault, then you can drill down into that section by using
tools such as `jq`:

``` sh
# Check the second processor config
benthos -c ./your-config.yaml --print-json | jq '.pipeline.processors[1]'

# Check the condition of a filter processor
benthos -c ./your-config.yaml --print-json | jq '.pipeline.processors[0].filter'
```

## Fragmenting Your Configuration

WARNING: THIS FEATURE IS EXPERIMENTAL AND SUBJECT TO CHANGE

It's possible to break a large configuration file into smaller parts with
[JSON references][json-references]. Benthos doesn't yet support the full
specification as it only resolves local or file path URIs, but this still allows
you to break your configs down significantly.

For example, suppose we have a configuration snippet saved under
`./config/foo.yaml`:

``` yaml
type: process_map
process_map:
  premap:
    id: foo.id
  processors:
  - type: parallel
    parallel:
      processors:
      - type: cache
        cache:
          operator: get
          key: ${!json_field:id}
          cache: objects
  postmap:
    result: .
```

And we wished to use this fragment within a larger configuration file. We can do
so by adding an object with a key `$ref` and a string value which is a path to
our snippet:

``` yaml
pipeline:
  processors:
  - type: decompress
    decompress:
      algorithm: gzip

  - "$ref": "./foo.yaml"

resources:
  caches:
    objects:
      type: baz
      baz: {}
```

The path of a reference is relative to the configuration file containing the
reference, and so if this configuration file were saved as `./config/bar.yaml`
then the path would resolve and we would end up with:

``` yaml
pipeline:
  processors:
  - type: decompress
    decompress:
      algorithm: gzip

  - type: process_map
    process_map:
      premap:
        id: foo.id
      processors:
      - type: parallel
        parallel:
          processors:
          - type: cache
            cache:
              operator: get
              key: ${!json_field:id}
              cache: objects
      postmap:
        result: .

resources:
  caches:
    objects:
      type: baz
      baz: {}
```

These references can be nested. Also, environment variable interpolations within
a configuration snippet are resolved _before_ references are resolved, therefore
it's possible to use them to specify which snippet to load:

``` yaml
pipeline:
  processors:
  - type: decompress
    decompress:
      algorithm: gzip

  - "$ref": "./${TARGET_SNIPPET}"
```

Finally, we can extract a specific field from our reference using a fragment
within the reference URI following the [JSON Pointer][json-pointer] format. For
example, the following:

``` yaml
pipeline:
  processors:
  - type: decompress
    decompress:
      algorithm: gzip

  - "$ref": "./foo.yaml#/process_map/processors/0"
```

Would result in the following expansion:

``` yaml
pipeline:
  processors:
  - type: decompress
    decompress:
      algorithm: gzip

  - type: parallel
    parallel:
      processors:
      - type: cache
        cache:
          operator: get
          key: ${!json_field:id}
          cache: objects
```

[processors]: ./processors/README.md
[conditions]: ./conditions/README.md
[json-references]: https://tools.ietf.org/html/draft-pbryan-zyp-json-ref-03
[json-pointer]: https://tools.ietf.org/html/rfc6901