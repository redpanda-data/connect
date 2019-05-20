Changelog
=========

All notable changes to this project will be documented in this file.

## Unreleased

## 2.2.0 - 2019-05-20

### Added

- Core service logic has been moved into new package `service`, making it easier
  to maintain plugin builds that match upstream Benthos.

## 2.1.1 - 2019-05-17

### Added

- Experimental support for WASM builds.

## 2.1.0 - 2019-05-16

### Added

- Config linting now reports line numbers.
- Config interpolations now support escaping.

## 2.0.0 - 2019-05-14

### Added

- API for creating `cache` implementations.
- API for creating `rate_limit` implementations.

### Changed

This is a major version released due to a series of minor breaking changes, you
can read the [full migration guide here](https://docs.benthos.dev/migration/v2/).

#### Configuration

- Benthos now attempts to infer the `type` of config sections whenever the field
  is omitted, for more information please read this overview:
  [Concise Configuration](https://docs.benthos.dev/configuration/#concise-configuration).
- Field `unsubscribe_on_close` of the `nats_stream` input is now `false` by
  default.

#### Service

- The following commandline flags have been removed: `swap-envs`, `plugins-dir`,
  `list-input-plugins`, `list-output-plugins`, `list-processor-plugins`,
  `list-condition-plugins`.

#### Go API

- Package `github.com/Jeffail/benthos/lib/processor/condition` changed to
  `github.com/Jeffail/benthos/lib/condition`.
- Interface `types.Cache` now has `types.Closable` embedded.
- Interface `types.RateLimit` now has `types.Closable` embedded.
- Add method `GetPlugin` to interface `types.Manager`.
- Add method `WithFields` to interface `log.Modular`.

## 1.20.4 - 2019-05-13

### Fixed

- Ensure `process_batch` processor gets normalised correctly.

## 1.20.3 - 2019-05-11

### Added

- New `for_each` processor with the same behaviour as `process_batch`,
  `process_batch` is now considered an alias for `for_each`.

## 1.20.2 - 2019-05-10

### Changed

- The `sql` processor now executes across the batch, documentation updated to
  clarify.

## 1.20.1 - 2019-05-10

### Fixed

- Corrected `result_codec` field in `sql` processor config.

## 1.20.0 - 2019-05-10

### Added

- New `sql` processor.

### Fixed

- Using `json_map_columns` with the `dynamodb` output should now correctly store
  `null` and array values within the target JSON structure.

## 1.19.2 - 2019-05-09

### Added

- New `encode` and `decode` scheme `hex`.

### Fixed

- Fixed potential panic when attempting an invalid HTTP client configuration.

## 1.19.1 - 2019-05-08

### Fixed

- Benthos in streams mode no longer tries to load directory `/benthos/streams`
  by default.

## 1.19.0 - 2019-05-07

### Added

- Field `json_map_columns` added to `dynamodb` output.

## 1.18.0 - 2019-05-06

### Added

- JSON references are now supported in configuration files.

## 1.17.0 - 2019-05-04

### Added

- The `hash` processor now supports `sha1`.
- Field `force_path_style_urls` added to `s3` components.
- Field `content_type` of the `s3` output is now interpolated.
- Field `content_encoding` added to `s3` output.

### Fixed

- The `benthos-lambda` distribution now correctly returns all message parts in
  synchronous execution.

### Changed

- Docker builds now use a locally cached `vendor` for dependencies.
- All `s3` components no longer default to enforcing path style URLs.

## 1.16.0 - 2019-04-30

### Added

- New output `drop_on_error`.
- Field `retry_until_success` added to `switch` output.

### Fixed

- Improved error and output logging for `subprocess` processor when the process
  exits unexpectedly.

## 1.15.0 - 2019-04-26

### Changed

- The main docker image is now based on busybox.
- Lint rule added for `batch` processors outside of the input section.

## 1.14.3 - 2019-04-25

### Fixed

- Removed potential `benthos-lambda` panic on shut down.

## 1.14.2 - 2019-04-25

### Fixed

- The `redis` cache no longer incorrectly returns a "key not found" error
  instead of connection errors.

## 1.14.1 - 2019-04-24

### Changed

- Changed docker tag format from `vX.Y.Z` to `X.Y.Z`.

## 1.14.0 - 2019-04-24

### Added

- Output `broker` pattern `fan_out_sequential`.
- Output type `drop` for dropping all messages.
- New interpolation function `timestamp_utc`.

## 1.13.0 - 2019-04-22

### Added

- New `benthos-lambda` distribution for running Benthos as a lambda function.

## 1.12.0 - 2019-04-21

### Added

- New `s3` cache implementation.
- New `file` cache implementation.
- Operators `quote` and `unquote` added to the `text` processor.
- Configs sent via the streams mode HTTP API are now interpolated with
  environment variable substitutes.

### Changed

- All AWS `s3` components now enforce path style syntax for bucket URLs. This
  improves compatibility with third party endpoints.

## 1.11.0 - 2019-04-12

### Added

- New `parallel` processor.

### Fixed

- The `dynamodb` cache `get` call now correctly reports key not found versus
  general request error.

## 1.10.10 - 2019-04-10

### Added

- New `sqs_bucket_path` field added to `s3` input.

### Fixed

- The `sqs` input now rejects messages that fail by resetting the visibility
  timeout.
- The `sqs` input no longer fails to delete consumed messages when the batch
  contains duplicate message IDs.

## 1.10.9 - 2019-04-05

### Fixed

- The `metric` processor no longer mixes label keys when processing across
  parallel pipelines.

## 1.10.8 - 2019-04-03

### Added

- Comma separated `kafka` and `kafka_balanced` address and topic values are now
  trimmed for whitespace.

## 1.10.6 - 2019-04-02

### Added

- Field `max_processing_period` added to `kafka` and `kafka_balanced` inputs.

### Fixed

- Compaction intervals are now respected by the `memory` cache type.

## 1.10.5 - 2019-03-29

### Fixed

- Improved `kafka_balanced` consumer group connection behaviour.

## 1.10.4 - 2019-03-29

### Added

- More `kafka_balanced` input config fields for consumer group timeouts.

## 1.10.3 - 2019-03-28

### Added

- New config interpolation function `uuid_v4`.

## 1.10.2 - 2019-03-21

### Fixed

- The `while` processor now correctly checks conditions against the first batch
  of the result of last processor loop.

## 1.10.1 - 2019-03-19

### Added

- Field `max_loops` added to `while` processor.

## 1.10.0 - 2019-03-18

### Added

- New `while` processor.

## 1.9.0 - 2019-03-17

### Added

- New `cache` processor.
- New `all` condition.
- New `any` condition.

## 1.8.0 - 2019-03-14

### Added

- Function interpolation for field `subject` added to `nats` output.

### Changed

- Switched underlying `kafka_balanced` implementation to sarama consumer.

## 1.7.10 - 2019-03-11

### Fixed

- Always allow acknowledgement flush during graceful termination.

## 1.7.9 - 2019-03-08

### Fixed

- Removed unnecessary subscription check from `gcp_pubsub` input.

## 1.7.7 - 2019-03-08

### Added

- New field `fields` added to `log` processor for structured log output.

## 1.7.3 - 2019-03-05

### Added

- Function interpolation for field `channel` added to `redis_pubsub` output.

## 1.7.2 - 2019-03-01

### Added

- Field `byte_size` added to `split` processor.

## 1.7.1 - 2019-02-27

### Fixed

- Field `dependencies` of children of the `process_dag` processor now correctly
  parsed from config files.

## 1.7.0 - 2019-02-26

### Added

- Field `push_job_name` added to `prometheus` metrics type.
- New `rename` metrics target.

### Fixed

- Removed potential race condition in `process_dag` with raw bytes conditions.

## 1.6.1 - 2019-02-21

### Added

- Field `max_batch_count` added to `s3` input.
- Field `max_number_of_messages` added to `sqs` input.

## 1.6.0 - 2019-02-20

### Added

- New `blacklist` metrics target.
- New `whitelist` metrics target.
- Initial support for opentracing, including a new `tracer` root component.
- Improved generated metrics documentation and config examples.
- The `nats_stream` input now has a field `unsubscribe_on_close` that when
  disabled allows durable subscription offsets to persist even when all
  connections are closed.
- Metadata field `nats_stream_sequence` added to `nats_stream` input.

## 1.5.1 - 2019-02-11

### Fixed

- The `subprocess` processor no longer sends unexpected empty lines when
  messages end with a line break.

## 1.5.0 - 2019-02-07

### Added

- New `switch` processor.

### Fixed

- Printing configs now sanitises resource sections.

## 1.4.1 - 2019-02-04

### Fixed

- The `headers` field in `http` configs now detects and applies `host` keys.

## 1.4.0 - 2019-02-04

### Added

- New `json_documents` format added to the `unarchive` processor.
- Field `push_interval` added to the `prometheus` metrics type.

## 1.3.2 - 2019-01-31

### Fixed

- Brokers now correctly parse configs containing plugin types as children.

## 1.3.1 - 2019-01-30

### Fixed

- Output broker types now correctly allocates nested processors for `fan_out`
  and `try` patterns.
- JSON formatted loggers now correctly escape error messages with line breaks.

## 1.3.0 - 2019-01-29

### Added

- Improved error logging for `s3` input download failures.
- More metadata fields copied to messages from the `s3` input.
- Field `push_url` added to the `prometheus` metrics target.

## 1.2.1 - 2019-01-28

### Added

- Resources (including plugins) that implement `Closable` are now shutdown
  cleanly.

## 1.2.0 - 2019-01-28

### Added

- New `json_array` format added to the `archive` and `unarchive` processors.
- Preliminary support added to the resource manager API to allow arbitrary
  shared resource plugins.

## 1.1.4 - 2019-01-23

### Fixed

- The `s3` input now caps and iterates batched SQS deletes.

## 1.1.3 - 2019-01-22

### Fixed

- The `archive` processor now interpolates the `path` per message of the batch.

## 1.1.2 - 2019-01-21

### Fixed

- Fixed environment variable interpolation when combined with embedded function
  interpolations.
- Fixed break down metric indexes for input and output brokers.

## 1.1.0 - 2019-01-17

### Added

- Input `s3` can now toggle the use of a download manager, switching off now
  downloads metadata from the target file.
- Output `s3` now writes metadata to the uploaded file.
- Operator `unescape_url_query` added to `text` processor.

### Fixed

- The `nats_steam` input and output now actively attempt to recover stale
  connections.
- The `awk` processor prints errors and flags failure when the program exits
  with a non-zero status.

## 1.0.2 - 2019-01-07

### Fixed

- The `subprocess` processor now attempts to read all flushed stderr output from
  a process when it fails.

## 1.0.1 - 2019-01-05

### Added

- Function `print_log` added to `awk` processor.

### Fixed

- The `awk` processor function `json_get` no longer returns string values with
  quotes.

## 1.0.0 - 2019-01-01

### Changed

- Processor `awk` codecs changed.

## 0.42.4 - 2018-12-31

### Changed

- Output type `sqs` now supports batched message sends.

## 0.42.3 - 2018-12-28

### Added

- Functions `json_get` and `json_set` added to `awk` processor.

## 0.42.1 - 2018-12-20

### Added

- Functions `timestamp_format`, `timestamp_format_nano`, `metadata_get` and
  `metadata_set` added to `awk` processor.

## 0.42.0 - 2018-12-19

### Added

- New `sleep` processor.
- New `awk` processor.

### Changed

- Converted all integer based time period fields to string based, e.g.
  `timeout_ms: 5000` would now be `timeout: 5s`. This will may potentially be
  disruptive but the `--strict` flag should catch all deprecated fields in an
  existing config.

## 0.41.0 - 2018-12-12

### Changed

- Renamed `max_batch_size` to `max_batch_count` for consistency.

## 0.40.2 - 2018-12-12

### Added

- New `max_batch_size` field added to `kafka`, `kafka_balanced` and `amqp`
  inputs. This provides a mechanism for creating message batches optimistically.

## 0.40.0 - 2018-12-10

### Added

- New `subprocess` processor.

### Changed

- API: The `types.Processor` interface has been changed in order to add lifetime
  cleanup methods (added `CloseAsync` and `WaitForClose`). For the overwhelming
  majority of processors these functions will be no-ops.
- More consistent `condition` metrics.

## 0.39.2 - 2018-12-07

### Added

- New `try` and `catch` processors for improved processor error handling.

## 0.39.1 - 2018-12-07

### Added

- All processors now attach error flags.
- S3 input is now more flexible with SNS triggered SQS events.

### Changed

- Processor metrics have been made more consistent.

## 0.39.0 - 2018-12-05

### Added

- New endpoint `/ready` that returns 200 when both the input and output
  components are connected, otherwise 503. This is intended to be used as a
  readiness probe.

### Changed

- Large simplifications to all metrics paths.
- Fully removed the previously deprecated `combine` processor.
- Input and output plugins updated to support new connection health checks.

## 0.38.10 - 2018-12-04

### Added

- Field `role_external_id` added to all S3 credential configs.
- New `processor_failed` condition and improved processor error handling which
  can be read about [here](./docs/error_handling.md)

## 0.38.8 - 2018-11-29

### Added

- New `content_type` field for the `s3` output.

## 0.38.6 - 2018-11-28

### Added

- New `group_by_value` processor.

## 0.38.5 - 2018-11-27

### Added

- Lint errors are logged (level INFO) during normal Benthos operation.
- New `--strict` command flag which causes Benthos to abort when linting errors
  are found in a config file.

## 0.38.4 - 2018-11-26

### Added

- New `--lint` command flag for linting config files.

## 0.38.1 - 2018-11-23

### Changed

- The `s3` output now attempts to batch uploads.
- The `s3` input now exposes errors in deleting SQS messages during acks.

## 0.38.0 - 2018-11-22

### Changed

- Resource based conditions no longer benefit from cached results. In practice
  this optimisation was easy to lose in config and difficult to maintain.

## 0.37.4 - 2018-11-22

### Added

- Metadata is now sent to `kafka` outputs.
- New `max_inflight` field added to the `nats_stream` input.

### Fixed

- Fixed relative path trimming for streams from file directories.

## 0.37.2 - 2018-11-15

### Fixed

- The `dynamodb` cache and output types now set TTL columns as unix timestamps.

## 0.37.1 - 2018-11-13

### Added

- New `escape_url_query` operator for the `text` processor.

## 0.37.0 - 2018-11-09

### Changed

- Removed submatch indexes in the `text` processor `find_regexp` operator and
  added documentation for expanding submatches in the `replace_regexp` operator.

## 0.36.4 - 2018-11-09

### Added

- Allow submatch indexes in the `find_regexp` operator for the `text` processor.

## 0.36.3 - 2018-11-08

### Added

- New `find_regexp` operator for the `text` processor.

## 0.36.1 - 2018-11-07

### Added

- New `aws` fields to the `elasticsearch` output to allow AWS authentication.

## 0.36.0 - 2018-11-06

### Added

- Add max-outstanding fields to `gcp_pubsub` input.
- Add new `dynamodb` output.

### Changed

- The `s3` output now calculates `path` field function interpolations per
  message of a batch.

## 0.35.1 - 2018-10-31

### Added

- New `set` operator for the `text` processor.

## 0.35.0 - 2018-10-30

### Added

- New `cache` output type.

## 0.34.13 - 2018-10-29

### Added

- New `group_by` processor.
- Add bulk send support to `elasticsearch` output.

## 0.34.8 - 2018-10-10

### Added

- New `content` interpolation function.

## 0.34.7 - 2018-10-04

### Added

- New `redis` cache type.

## 0.34.5 - 2018-10-02

### Changed

- The `process_map` processor now allows map target path overrides when a target
  is the parent of another target.

## 0.34.4 - 2018-10-02

### Added

- Field `pipeline` and `sniff` added to the `elasticsearch` output.
- Operators `to_lower` and `to_upper` added to the `text` processor.

## 0.34.3 - 2018-09-29

### Added

- Field `endpoint` added to all AWS types.

## 0.34.2 - 2018-09-27

### Changed

- Allow `log` config field `static_fields` to be fully overridden.

## 0.34.0 - 2018-09-27

### Added

- New `process_dag` processor.
- New `static_fields` map added to log config for setting static log fields.

### Changed

- JSON log field containing component path moved from `@service` to `component`.

## 0.33.0 - 2018-09-22

### Added

- New `gcp_pubsub` input and outputs.
- New `log` processor.
- New `lambda` processor.

## 0.32.0 - 2018-09-18

### Added

- New `process_batch` processor.
- Added `count` field to `batch` processor.
- Metrics for `kinesis` output throttles.

### Changed

- The `combine` processor is now considered DEPRECATED, please use the `batch`
  processor instead.
- The `batch` processor field `byte_size` is now set at 0 (and therefore
  ignored) by default. A log warning has been added in case anyone was relying
  on the default.

## 0.31.4 - 2018-09-16

### Added

- New `rate_limit` resource with a `local` type.
- Field `rate_limit` added to `http` based processors, inputs and outputs.

## 0.31.2 - 2018-09-14

### Added

- New `prefetch_count` field added to `nats` input.

## 0.31.0 - 2018-09-11

### Added

- New `bounds_check` condition type.
- New `check_field` condition type.
- New `queue` field added to `nats` input.
- Function interpolation for the `topic` field of the `nsq` output.

### Changed

- The `nats` input now defaults to joining a queue.

## 0.30.1 - 2018-09-06

### Changed

- The redundant `nsq` output field `max_in_flight` has been removed.
- The `files` output now interpolates paths per message part of a batch.

## 0.30.0 - 2018-09-06

### Added

- New `hdfs` input and output.
- New `switch` output.
- New `enum` and `has_prefix` operators for the `metadata` condition.
- Ability to set `tls` client certificate fields directly.

## 0.29.0 - 2018-09-02

### Added

- New `retry` output.
- Added `regex_partial` and `regex_exact` operators to the `metadata` condition.

### Changed

- The `kinesis` output field `retries` has been renamed `max_retries` in order
  to expose the difference in its zero value behaviour (endless retries) versus
  other `retry` fields (zero retries).

## 0.28.0 - 2018-09-01

### Added

- New `endpoint` field added to `kinesis` input.
- New `dynamodb` cache type.

## 0.27.0 - 2018-08-30

### Added

- Function interpolation for the `topic` field of the `kafka` output.
- New `target_version` field for the `kafka_balanced` input.
- TLS config fields for client certificates.

### Changed

- TLS config field `cas_file` has been renamed `root_cas_file`.

## 0.26.3 - 2018-08-29

### Added

- New `zip` option for the `archive` and `unarchive` processors.

### Changed

- The `kinesis` output type now supports batched sends and per message
  interpolation.

## 0.26.2 - 2018-08-27

### Added

- New `metric` processor.

## 0.26.1 - 2018-08-26

### Added

- New `redis_streams` input and output.

## 0.26.0 - 2018-08-25

### Added

- New `kinesis` input and output.

## 0.25.0 - 2018-08-22

### Added

- The `index` field of the `elasticsearch` output can now be dynamically set
  using function interpolation.
- New `hash` processor.

### Changed

- API: The `metrics.Type` interface has been changed in order to add labels.

## 0.24.0 - 2018-08-17

### Changed

- Significant restructuring of `amqp` inputs and outputs. These changes should
  be backwards compatible for existing pipelines, but changes the way in which
  queues, exchanges and bindings are declared using these types.

## 0.23.17 - 2018-08-17

### Added

- New durable fields for `amqp` input and output types.

## 0.23.15 - 2018-08-16

### Changed

- Improved statsd client with better cached aggregation.

## 0.23.14 - 2018-08-16

### Added

- New `tls` fields for `amqp` input and output types.

## 0.23.12 - 2018-08-14

### Added

- New `type` field for `elasticsearch` output.

## 0.23.9 - 2018-08-10

### Added

- New `throttle` processor.

## 0.23.6 - 2018-08-09

### Added

- New `less_than` and `greater_than` operators for `metadata` condition.

## 0.23.4 - 2018-08-09

### Added

- New `metadata` condition type.
- More metadata fields for `kafka` input.
- Field `commit_period_ms` for `kafka` and `kafka_balanced` inputs for
  specifying a commit period.

## 0.23.1 - 2018-08-06

### Added

- New `retries` field to `s3` input, to cap the number of download attempts made
  on the same bucket item.
- Added metadata based mechanism to detect final message from a `read_until`
  input.
- Added field to `split` processor for specifying target batch sizes.

## 0.23.0 - 2018-08-06

### Added

- Metadata fields are now per message part within a batch.
- New `metadata_json_object` function interpolation to return a JSON object of
  metadata key/value pairs.

### Changed

- The `metadata` function interpolation now allows part indexing and no longer
  returns a JSON object when no key is specified, this behaviour can now be done
  using the `metadata_json_object` function.

## 0.22.0 - 2018-08-03

### Added

- Fields for the `http` processor to enable parallel requests from message
  batches.

### Changed

- Broker level output processors are now applied _before_ the individual output
  processors.
- The `dynamic` input and output HTTP paths for CRUD operations are now
  `/inputs/{input_id}` and `/outputs/{output_id}` respectively.
- Removed deprecated `amazon_s3`, `amazon_sqs` and `scalability_protocols` input
  and output types.
- Removed deprecated `json_fields` field from the `dedupe` processor.

## 0.21.0 - 2018-07-31

### Added

- Add conditions to `process_map` processor.

### Changed

- TLS config fields have been cleaned up for multiple types. This affects
  the `kafka`, `kafka_balanced` and `http_client` input and output types, as
  well as the `http` processor type.

## 0.20.8 - 2018-07-30

### Added

- New `delete_all` and `delete_prefix` operators for `metadata` processor.
- More metadata fields extracted from the AMQP input.
- HTTP clients now support function interpolation on the URL and header values,
  this includes the `http_client` input and output as well as the `http`
  processor.

## 0.20.7 - 2018-07-27

### Added

- New `key` field added to the `dedupe` processor, allowing you to deduplicate
  using function interpolation. This deprecates the `json_paths` array field.

## 0.20.6 - 2018-07-27

### Added

- New `s3` and `sqs` input and output types, these replace the now deprecated
  `amazon_s3` and `amazon_sqs` types respectively, which will eventually be
  removed.
- New `nanomsg` input and output types, these replace the now deprecated
  `scalability_protocols` types, which will eventually be removed.

## 0.20.5 - 2018-07-27

### Added

- Metadata fields are now collected from MQTT input.
- AMQP output writes all metadata as headers.
- AMQP output field `key` now supports function interpolation.

## 0.20.1 - 2018-07-26

### Added

- New `metadata` processor and configuration interpolation function.

## 0.20.0 - 2018-07-26

### Added

- New config interpolator function `json_field` for extracting parts of a JSON
  message into a config value.

### Changed

- Log level config field no longer stutters, `logger.log_level` is now
  `logger.level`.

## 0.19.1 - 2018-07-25

### Added

- Ability to create batches via conditions on message payloads in the `batch`
  processor.
- New `--examples` flag for generating specific examples from Benthos.

## 0.19.0 - 2018-07-23

### Added

- New `text` processor.

### Changed

- Processor `process_map` replaced field `strict_premapping` with
  `premap_optional`.

## 0.18.0 - 2018-07-20

### Added

- New `process_field` processor.
- New `process_map` processor.

### Changed

- Removed mapping fields from the `http` processor, this behaviour has been put
  into the new `process_map` processor instead.

## 0.17.0 - 2018-07-17

### Changed

- Renamed `content` condition type to `text` in order to clarify its purpose.

## 0.16.4 - 2018-07-17

### Added

- Latency metrics for caches.
- TLS options for `kafka` and `kafka_partitions` inputs and outputs.

### Changed

- Metrics for items configured within the `resources` section are now namespaced
  under their identifier.

## 0.16.3 - 2018-07-16

### Added

- New `copy` and `move` operators for the `json` processor.

## 0.16.2 - 2018-07-12

### Added

- Metrics for recording `http` request latencies.

## 0.16.0 - 2018-07-09

### Changed

- Improved and rearranged fields for `http_client` input and output.

## 0.15.5 - 2018-07-08

### Added

- More compression and decompression targets.
- New `lines` option for archive/unarchive processors.
- New `encode` and `decode` processors.
- New `period_ms` field for the `batch` processor.
- New `clean` operator for the `json` processor.

## 0.15.4 - 2018-07-04

### Added

- New `http` processor, where payloads can be sent to arbitrary HTTP endpoints
  and the result constructed into a new payload.
- New `inproc` inputs and outputs for linking streams together.

## 0.15.3 - 2018-07-03

### Added

- New streams endpoint `/streams/{id}/stats` for obtaining JSON metrics for a
  stream.

### Changed

- Allow comma separated topics for `kafka_balanced`.

## 0.15.0 - 2018-06-28

### Added

- Support for PATCH verb on the streams mode `/streams/{id}` endpoint.

### Changed

- Sweeping changes were made to the environment variable configuration file.
  This file is now auto generated along with its supporting document. This
  change will impact the docker image.

## 0.14.7 - 2018-06-24

### Added

- New `filter_parts` processor for filtering individual parts of a message
  batch.
- New field `open_message` for `websocket` input.

### Changed

- No longer setting default input processor.

## 0.14.6 - 2018-06-21

### Added

- New `root_path` field for service wide `http` config.

## 0.14.5 - 2018-06-21

### Added

- New `regexp_exact` and `regexp_partial` content condition operators.

## 0.14.4 - 2018-06-19

## Changed

- The `statsd` metrics target will now periodically report connection errors.

## 0.14.2 - 2018-06-18

## Changed

- The `json` processor will now `append` array values in expanded form.

## 0.14.0 - 2018-06-15

### Added

- More granular config options in the `http_client` output for controlling retry
  logic.
- New `try` pattern for the output `broker` type, which can be used in order to
  configure fallback outputs.
- New `json` processor, this replaces `delete_json`, `select_json`, `set_json`.

### Changed

- The `streams` API endpoints have been changed to become more "RESTy".
- Removed the `delete_json`, `select_json` and `set_json` processors, please use
  the `json` processor instead.

## 0.13.5 - 2018-06-10

### Added

- New `grok` processor for creating structured objects from unstructured data.

## 0.13.4 - 2018-06-08

### Added

- New `files` input type for reading multiple files as discrete messages.

### Changed

- Increase default `max_buffer` for `stdin`, `file` and `http_client` inputs.
- Command flags `--print-yaml` and `--print-json` changed to provide sanitised
  outputs unless accompanied by new `--all` flag.

### Removed

- Badger based buffer option has been removed.

## 0.13.3 - 2018-06-06

### Added

- New metrics wrapper for more basic interface implementations.
- New `delete_json` processor.
- New field `else_processors` for `conditional` processor.

## 0.13.2 - 2018-06-03

### Added

- New websocket endpoint for `http_server` input.
- New websocket endpoint for `http_server` output.
- New `websocket` input type.
- New `websocket` output type.

## 0.13.1 - 2018-06-02

### Added

- Goreleaser config for generating release packages.

### Changed

- Back to using Scratch as base for Docker image, instead taking ca-certificates
  from the build image.

## 0.13.0 - 2018-06-02

### Added

- New `batch` processor for combining payloads up to a number of bytes.
- New `conditional` processor, allows you to configure a chain of processors to
  only be run if the payload passes a `condition`.
- New `--stream` mode features:
  + POST verb for `/streams` path now supported.
  + New `--streams-dir` flag for parsing a directory of stream configs.

### Changed

- The `condition` processor has been renamed `filter`.
- The `custom_delimiter` fields in any line reader types `file`, `stdin`,
  `stdout`, etc have been renamed `delimiter`, where the behaviour is the same.
- Now using Alpine as base for Docker image, includes ca-certificates.
