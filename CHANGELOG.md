Changelog
=========

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- More granular config options in the `http_client` output for controlling retry
  logic.
- New `try` pattern for the output `broker` type, which can be used in order to
  configure fallback outputs.

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
