Changelog
=========

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- Field `pipeline` and `sniff` added to the `elasticsearch` output.

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
