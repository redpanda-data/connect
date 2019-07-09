Configuration Testing
=====================

EXPERIMENTAL: The tooling outlined on this page are experimental and therefore subject to change outside of major version releases.

The Benthos service offers a command `benthos --test ./...` for running unit tests on sections on a configuration file. This makes it easy to protect your config files from regressions over time.

## Contents

1. [Writing a Test](#writing_a_test)
2. [Output Conditions](#output_conditions)
3. [Running Tests](#running_tests)

## Writing a Test

For this example let's imagine we have a configuration file `foo.yaml` containing some processors:

```yaml
input:
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  processors:
  - text:
      operator: to_upper
  - text:
      operator: append
      value: end

output:
  s3:
    bucket: TODO
    path: "${!metadata:kafka_topic}/${!json_field:message.id}.json"
```

In order to write unit tests for this config we must accompany it with a file of the same name and extension but suffixed with `_benthos_test`, which in this case would be `foo_benthos_test.yaml`. To make this easier it's possible to generate a default test definition file with `benthos --generate-test ./foo.yaml`, which creates:

```yaml
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
            key: example_key
            value: example metadata value
```

The field `parallel` instructs as to whether the tests listed in this definition should be executed in parallel. Under `tests` we then have a list of any number of unit tests to execute for the config file. 

Each test is run in complete isolation, including any resources defined by the config file. Tests should be allocated a unique `name` that identifies the feature being tested.

The field `target_processors` is a JSON Pointer that identifies the specific processors within the file which should be executed by the test. This allows you to target a specific processor (`/pipeline/processors/0`), or processors within a different section on your config (`/input/broker/inputs/0/processors`).

The field `environment` allows you to define an object of key/value pairs that set environment variables to be evaluated during the parsing of the config file. These are unique to each test, allowing you to test different environment variable interpolations of the config file.

The field `input_batch` lists one or more messages to be fed into the targeted processors as a batch. Each message of the batch may define its raw content as well as metadata key/value pairs.

The field `output_batches` lists one or more batches of messages which are expected to result from the target processors. Each batch lists one or more messages, each one defining [`conditions`](#output_conditions) to describe the expected contents of the message.

If the number of batches defined does not match the resulting number of batches the test will fail. If the number of messages defined in each batch does not match the number in the resulting batches the test will fail. If any condition of a message fails then the test fails.

## Output Conditions

### `content_equals`

```yaml
content_equals: example content
```

Checks the full raw contents of a message against a value.

### `metadata_equals`

```yaml
metadata_equals:
  key: example_key
  value: example metadata value
```

Checks a metadata field against a value.

## Running Tests