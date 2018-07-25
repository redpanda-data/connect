Changelog
=========

All notable changes to this project will be documented in this file.

## Unreleased

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
