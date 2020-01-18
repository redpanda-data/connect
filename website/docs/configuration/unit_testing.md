---
title: Unit Testing
---

EXPERIMENTAL: The tooling outlined on this document are experimental and therefore subject to change outside of major version releases.

The Benthos service offers a command `benthos --test ./...` for running unit tests on sections of a configuration file. This makes it easy to protect your config files from regressions over time.

## Contents

1. [Writing a Test](#writing-a-test)
2. [Output Conditions](#output-conditions)
3. [Running Tests](#running-tests)

## Writing a Test

Let's imagine we have a configuration file `foo.yaml` containing some processors:

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

In order to write unit tests for this config we must accompany it with a file of the same name and extension, but suffixed with `_benthos_test`, which in this case would be `foo_benthos_test.yaml`. We can generate an example definition for this config with `benthos --gen-test ./foo.yaml`:

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

The field `target_processors` is a [JSON Pointer][json-pointer] that identifies the specific processors within the file which should be executed by the test. This allows you to target a specific processor (`/pipeline/processors/0`), or processors within a different section on your config (`/input/broker/inputs/0/processors`) if required.

The field `environment` allows you to define an object of key/value pairs that set environment variables to be evaluated during the parsing of the target config file. These are unique to each test, allowing you to test different environment variable interpolation combinations. Note that these environment variables are not used during the execution of the tests, only during parse time.

The field `input_batch` lists one or more messages to be fed into the targeted processors as a batch. Each message of the batch may have its raw content defined as well as metadata key/value pairs.

The field `output_batches` lists any number of batches of messages which are expected to result from the target processors. Each batch lists any number of messages, each one defining [`conditions`](#output-conditions) to describe the expected contents of the message.

If the number of batches defined does not match the resulting number of batches the test will fail. If the number of messages defined in each batch does not match the number in the resulting batches the test will fail. If any condition of a message fails then the test fails.

## Output Conditions

### `content_equals`

```yaml
content_equals: example content
```

Checks the full raw contents of a message against a value.

### `content_matches`

```yaml
content_matches: "^foo [a-z]+ bar$"
```

Checks whether the full raw contents of a message matches a regular expression (re2).

### `metadata_equals`

```yaml
metadata_equals:
  example_key: example metadata value
```

Checks a map of metadata keys to values against the metadata stored in the message. If there is a value mismatch between a key of the condition versus the message metadata this condition will fail.

## Running Tests

Executing tests for a specific config can be done by pointing the command flag `--test` at either the config to be tested or its test definition, e.g. `benthos --test ./config.yaml` and `benthos --test ./config_benthos_test.yaml` are equivalent.

In order to execute all tests of a directory simply point `--test` to that directory, e.g. `benthos --test ./foo` will execute all tests found in the directory `foo`. In order to walk a directory tree and execute all tests found you can use the shortcut `./...`, e.g. `benthos --test ./...` will execute all tests found in the current directory, any child directories, and so on.

### Linting

Benthos has a linter that can be executed on a config file with `--lint`, it's possible to run this linter as well as your tests on config files by including the flag, e.g. `benthos --lint --test ./config.yaml` will both lint and test the config file `./config.yaml`. If the linting stage fails then the process exits with status 1 similar to if a test had failed.

Note that when combining the flags `--lint` and `--test` the linting will _only_ be executed on config files accompanied with a test definition. This is in order to avoid linting files unrelated to Benthos execution during directory walking.

[json-pointer]: https://tools.ietf.org/html/rfc6901