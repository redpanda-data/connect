---
title: Unit Testing
---

The Benthos service offers a command `benthos test` for running unit tests on sections of a configuration file. This makes it easy to protect your config files from regressions over time.

## Contents

1. [Writing a Test](#writing-a-test)
2. [Output Conditions](#output-conditions)
3. [Running Tests](#running-tests)
4. [Mocking Processors](#mocking-processors)

## Writing a Test

Let's imagine we have a configuration file `foo.yaml` containing some processors:

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  processors:
  - bloblang: '"%vend".format(content().uppercase().string())'

output:
  aws_s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'
```

One way to write our unit tests for this config is to accompany it with a file of the same name and extension but suffixed with `_benthos_test`, which in this case would be `foo_benthos_test.yaml`. We can generate an example definition for this config with `benthos test --generate ./foo.yaml` which gives:

```yml
parallel: true
tests:
  - name: example test
    target_processors: '/pipeline/processors'
    environment: {}
    input_batch:
      - content: 'example content'
        metadata:
          example_key: example metadata value
    output_batches:
      -
        - content_equals: example content
          metadata_equals:
            example_key: example metadata value
```

The field `parallel` instructs as to whether the tests listed in this definition should be executed in parallel. Under `tests` we then have a list of any number of unit tests to execute for the config file.

Each test is run in complete isolation, including any resources defined by the config file. Tests should be allocated a unique `name` that identifies the feature being tested.

The field `target_processors` is a [JSON Pointer][json-pointer] that identifies the specific processors within the file which should be executed by the test. This allows you to target a specific processor (`/pipeline/processors/0`), or processors within a different section on your config (`/input/broker/inputs/0/processors`) if required.

The field `environment` allows you to define an object of key/value pairs that set environment variables to be evaluated during the parsing of the target config file. These are unique to each test, allowing you to test different environment variable interpolation combinations.

> When tests are run in parallel they will NOT retain their environment variables during execution. In order to retain custom environment variables ensure that `parallel` is set to `false`.

The field `input_batch` lists one or more messages to be fed into the targeted processors as a batch. Each message of the batch may have its raw content defined as well as metadata key/value pairs.

For the common case where the messages are in JSON format, you can use `json_content` instead of `content` to specify the message structurally rather than verbatim.

The field `output_batches` lists any number of batches of messages which are expected to result from the target processors. Each batch lists any number of messages, each one defining [`conditions`](#output-conditions) to describe the expected contents of the message.

If the number of batches defined does not match the resulting number of batches the test will fail. If the number of messages defined in each batch does not match the number in the resulting batches the test will fail. If any condition of a message fails then the test fails.

### Inline Tests

Sometimes it's more convenient to define your tests within the config being tested. This is fine, simply add the `tests` field to the end of the config being tested. When defining inline tests the field `parallel` is not supported.

### Bloblang Tests

Sometimes when working with large [Bloblang mappings][bloblang] it's preferred to have the full mapping in a separate file to your Benthos configuration. In this case it's possible to write unit tests that target and execute the mapping directly with the field `target_mapping`, which when specified is interpreted as either an absolute path or a path relative to the test definition file that points to a file containing only a Bloblang mapping.

For example, if we were to have a file `cities.blobl` containing a mapping:

```coffee
root.Cities = this.locations.
                filter(loc -> loc.state == "WA").
                map_each(loc -> loc.name).
                sort().join(", ")
```

We can accompany it with a test file `cities_test.yaml` containing a regular test definition:

```yml
tests:
  - name: test cities mapping
    target_mapping: './cities.blobl'
    environment: {}
    input_batch:
      - content: |
          {
            "locations": [
              {"name": "Seattle", "state": "WA"},
              {"name": "New York", "state": "NY"},
              {"name": "Bellevue", "state": "WA"},
              {"name": "Olympia", "state": "WA"}
            ]
          }
    output_batches:
      -
        - json_equals: {"Cities": "Bellevue, Olympia, Seattle"}
```

And execute this test the same way we execute other Benthos tests (`benthos test ./dir/cities_test.yaml`, `benthos test ./dir/...`, etc).

### Fragmented Tests

Sometimes the number of tests you need to define in order to cover a config file is so vast that it's necessary to split them across multiple test definition files. This is possible but Benthos still requires a way to detect the configuration file being targeted by these fragmented test definition files. In order to do this we must prefix our `target_processors` field with the path of the target relative to the definition file.

The syntax of `target_processors` in this case is a full [JSON Pointer][json-pointer] that should look something like `target.yaml#/pipeline/processors`. For example, if we saved our test definition above in an arbitrary location like `./tests/first.yaml` and wanted to target our original `foo.yaml` config file, we could do that with the following:

```yml
tests:
  - name: example test
    target_processors: '../foo.yaml#/pipeline/processors'
    environment: {}
    input_batch:
      - content: 'example content'
        metadata:
          example_key: example metadata value
    output_batches:
      -
        - content_equals: example content
          metadata_equals:
            example_key: example metadata value
```

## Input Definitions

### `content`

Sets the raw content of the message.

### `json_content`

```yml
json_content:
  foo: foo value
  bar: [ element1, 10 ]
```

Sets the raw content of the message to a JSON document matching the structure of the value.

### `file_content`

```yml
file_content: ./foo/bar.txt
```

Sets the raw content of the message by reading a file. The path of the file should be relative to the path of the test file.

### `metadata`

A map of key/value pairs that sets the metadata values of the message.

## Output Conditions

### `bloblang`

```yml
bloblang: 'this.age > 10 && meta("foo").length() > 0'
```

Executes a [Bloblang expression][bloblang] on a message, if the result is anything other than a boolean equalling `true` the test fails.

### `content_equals`

```yml
content_equals: example content
```

Checks the full raw contents of a message against a value.

### `content_matches`

```yml
content_matches: "^foo [a-z]+ bar$"
```

Checks whether the full raw contents of a message matches a regular expression (re2).

### `metadata_equals`

```yml
metadata_equals:
  example_key: example metadata value
```

Checks a map of metadata keys to values against the metadata stored in the message. If there is a value mismatch between a key of the condition versus the message metadata this condition will fail.

### `file_equals`

```yml
file_equals: ./foo/bar.txt
```

Checks that the contents of a message matches the contents of a file. The path of the file should be relative to the path of the test file.

### `json_equals`

```yml
json_equals: { "key": "value" }
```

Checks that both the message and the condition are valid JSON documents, and that they are structurally equivalent. Will ignore formatting and ordering differences.

You can also structure the condition content as YAML and it will be converted to the equivalent JSON document for testing:

```yml
json_equals:
  key: value
```

### `json_contains`

```yml
json_contains: { "key": "value" }
```

Checks that both the message and the condition are valid JSON documents, and that the message is a superset of the condition.

## Running Tests

Executing tests for a specific config can be done by pointing the subcommand `test` at either the config to be tested or its test definition, e.g. `benthos test ./config.yaml` and `benthos test ./config_benthos_test.yaml` are equivalent.

In order to execute all tests of a directory simply point `test` to that directory, e.g. `benthos test ./foo` will execute all tests found in the directory `foo`. In order to walk a directory tree and execute all tests found you can use the shortcut `./...`, e.g. `benthos test ./...` will execute all tests found in the current directory, any child directories, and so on.

## Mocking Processors

BETA: This feature is currently in a BETA phase, which means breaking changes could be made if a fundamental issue with the feature is found.

Sometimes you'll want to write tests for a series of processors, where one or more of them are networked (or otherwise stateful). Rather than creating and managing mocked services you can define mock versions of those processors in the test definition. For example, if we have a config with the following processors:

```yaml
pipeline:
  processors:
    - bloblang: 'root = "simon says: " + content()'
    - label: get_foobar_api
      http:
        url: http://example.com/foobar
        verb: GET
    - bloblang: 'root = content().uppercase()'
```

Rather than create a fake service for the `http` processor to interact with we can define a mock in our test definition that replaces it with a `bloblang` processor. Mocks are configured as a map of labels that identify a processor to replace and the config to replace it with:

```yaml
tests:
  - name: mocks the http proc
    target_processors: '/pipeline/processors'
    mocks:
      get_foobar_api:
        bloblang: 'root = content().string() + " this is some mock content"'
    input_batch:
      - content: "hello world"
    output_batches:
      - - content_equals: "SIMON SAYS: HELLO WORLD THIS IS SOME MOCK CONTENT"
```

With the above test definition the `http` processor will be swapped out for `bloblang: 'root = content().string() + " this is some mock content"'`. For the purposes of mocking it is recommended that you use a `bloblang` processor that simply mutates the message in a way that you would expect the mocked processor to.

> Note: It's not currently possible to mock components that are imported as separate resource files (using `--resource`/`-r`). It is recommended that you mock these by maintaining separate definitions for test purposes (`-r "./test/*.yaml"`).

### More granular mocking

It is also possible to target specific fields within the test config by [JSON pointers][json-pointer] as an alternative to labels. The following test definition would create the same mock as the previous:

```yaml
tests:
  - name: mocks the http proc
    target_processors: '/pipeline/processors'
    mocks:
      /pipeline/processors/1:
        bloblang: 'root = content().string() + " this is some mock content"'
    input_batch:
      - content: "hello world"
    output_batches:
      - - content_equals: "SIMON SAYS: HELLO WORLD THIS IS SOME MOCK CONTENT"
```

[json-pointer]: https://tools.ietf.org/html/rfc6901
[bloblang]: /docs/guides/bloblang/about
