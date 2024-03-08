Changelog
=========

All notable changes to this project will be documented in this file.

## Unreleased

### Added

- Field `credit` added to the `amqp_1` input to specify the maximum number of unacknowledged messages the sender can transmit.
- Bloblang now supports root-level `if` statements.
- New experimental `sql` cache.

### Changed

- The default value of the `amqp_1.credit` input has changed from `1` to `64`.

## 4.25.1 - 2024-03-01

### Fixed

- Fixed a regression in v4.25.0 where [template based components](https://www.benthos.dev/docs/configuration/templating) were not parsing correctly from configs.

## 4.25.0 - 2024-03-01

### Added

- Field `address_cache` added to the `socket_server` input.
- Field `read_header` added to the `amqp_1` input.
- All inputs with a `codec` field now support a new field `scanner` to replace it. Scanners are more powerful as they are configured in a structured way similar to other component types rather than via a single string field, for more information [check out the scanners page](https://www.benthos.dev/docs/components/scanners/about).
- New `diff` and `patch` Bloblang methods.
- New `processors` processor.
- Field `read_header` added to the `amqp_1` input.
- A debug endpoint `/debug/pprof/allocs` has been added for profiling allocations.
- New `cockroachdb_changefeed` input.
- The `open_telemetry_collector` tracer now supports sampling.
- The `aws_kinesis` input and output now support specifying ARNs as the stream target.
- New `azure_cosmosdb` input, processor and output.
- All `sql_*` components now support the `gocosmos` driver.
- New `opensearch` output.

### Fixed

- The `javascript` processor now handles module imports correctly.
- Bloblang `if` statements now provide explicit errors when query expressions resolve to non-boolean values.
- Some metadata fields from the `amqp_1` input were always empty due to type mismatch, this should no longer be the case.
- The `zip` Bloblang method no longer fails when executed without arguments.
- The `amqp_0_9` output no longer prints bogus exchange name when connecting to the server.
- The `generate` input no longer adds an extra second to `interval: '@every x'` syntax.
- The `nats_jetstream` input no longer fails to locate mirrored streams.
- Fixed a rare panic in batching mechanisms with a specified `period`, where data arrives in low volumes and is sporadic.
- Executing config unit tests should no longer fail due to output resources failing to connect.

### Changed

- The `parse_parquet` Bloblang function, `parquet_decode`, `parquet_encode` processors and the `parquet` input have all been upgraded to the latest version of the underlying Parquet library. Since this underlying library is experimental it is likely that behaviour changes will result. One significant change is that encoding numerical values that are larger than the column type (`float64` into `FLOAT`, `int64` into `INT32`, etc) will no longer be automatically converted.
- The `parse_log` processor field `codec` is now deprecated.
- *WARNING*: Many components have had their underlying implementations moved onto newer internal APIs for defining and extracting their configuration fields. It's recommended that upgrades to this version are performed cautiously.
- *WARNING*: All AWS components have been upgraded to the latest client libraries. Although lots of testing has been done, these libraries have the potential to differ in discrete ways in terms of how credentials are evaluated, cross-account connections are performed, and so on. It's recommended that upgrades to this version are performed cautiously.

## 4.24.0 - 2023-11-24

### Added

- Field `idempotent_write` added to the `kafka_franz` output.
- Field `idle_timeout` added to the `read_until` input.
- Field `delay_seconds` added to the `aws_sqs` output.
- Fields `discard_unknown` and `use_proto_names` added to the `protobuf` processors.

### Fixed

- Bloblang error messages for bad function/method names or parameters should now be improved in mappings that use shorthand for `root = ...`.
- All redis components now support usernames within the configured URL for authentication.
- The `protobuf` processor now supports targetting nested types from proto files.
- The `schema_registry_encode` and `schema_registry_decode` processors should no longer double escape URL unsafe characters within subjects when querying their latest versions.

## 4.23.0 - 2023-10-30

### Added

- The `amqp_0_9` output now supports dynamic interpolation functions within the `exchange` field.
- Field `custom_topic_creation` added to the `kafka` output.
- New Bloblang method `ts_sub`.
- The Bloblang method `abs` now supports integers in and integers out.
- Experimental `extract_tracing_map` field added to the `nats`, `nats_jetstream` and `nats_stream` inputs.
- Experimental `inject_tracing_map` field added to the `nats`, `nats_jetstream` and `nats_stream` outputs.
- New `_fail_fast` variants for the `broker` output `fan_out` and `fan_out_sequential` patterns.
- Field `summary_quantiles_objectives` added to the `prometheus` metrics exporter.
- The `metric` processor now supports floating point values for `counter_by` and `gauge` types.

### Fixed

- Allow labels on caches and rate limit resources when writing configs in CUE.
- Go API: `log/slog` loggers injected into a stream builder via `StreamBuilder.SetLogger` should now respect formatting strings.
- All Azure components now support container SAS tokens for authentication.
- The `kafka_franz` input now provides properly typed metadata values.
- The `trino` driver for the various `sql_*` components no longer panics when trying to insert nulls.
- The `http_client` input no longer sends a phantom request body on subsequent requests when an empty `payload` is specified.
- The `schema_registry_encode` and `schema_registry_decode` processors should no longer fail to obtain schemas containing slashes (or other URL path unfriendly characters).
- The `parse_log` processor no longer extracts structured fields that are incompatible with Bloblang mappings.
- Fixed occurrences where Bloblang would fail to recognise `float32` values.

## 4.22.0 - 2023-10-03

### Added

- The `-e/--env-file` cli flag for importing environment variable files now supports glob patterns.
- Environment variables imported via `-e/--env-file` cli flags now support triple quoted strings.
- New experimental `counter` function added to Bloblang. It is recommended that this function, although experimental, should be used instead of the now deprecated `count` function.
- The `schema_registry_encode` and `schema_registry_decode` processors now support JSONSchema.
- Field `metadata` added to the `nats` and `nats_jetstream` outputs.
- The `cached` processor field `ttl` now supports interpolation functions.
- Many new properties fields have been added to the `amqp_0_9` output.
- Field `command` added to the `redis_list` input and output.

### Fixed

- Corrected a scheduling error where the `generate` input with a descriptor interval (`@hourly`, etc) had a chance of firing twice.
- Fixed an issue where a `redis_streams` input that is rejected from read attempts enters a reconnect loop without backoff.
- The `sqs` input now periodically refreshes the visibility timeout of messages that take a significant amount of time to process.
- The `ts_add_iso8601` and `ts_sub_iso8601` bloblang methods now return the correct error for certain invalid durations.
- The `discord` output no longer ignores structured message fields containing underscores.
- Fixed an issue where the `kafka_franz` input was ignoring batching periods and stalling.

### Changed

- The `random_int` Bloblang function now prevents instantiations where either the `max` or `min` arguments are dynamic. This is in order to avoid situations where the random number generator is re-initialised across subsequent mappings in a way that surprises map authors.

## 4.21.0 - 2023-09-08

### Added

- Fields `client_id` and `rack_id` added to the `kafka_franz` input and output.
- New experimental `command` processor.
- Parameter `no_cache` added to the `file` and `env` Bloblang functions.
- New `file_rel` function added to Bloblang.
- Field `endpoint_params` added to the `oauth2` section of HTTP client components.

### Fixed

- Allow comments in single root and directly imported bloblang mappings.
- The `azure_blob_storage` input no longer adds `blob_storage_content_type` and `blob_storage_content_encoding` metadata values as string pointer types, and instead adds these values as string types only when they are present.
- The `http_server` input now returns a more appropriate 503 service unavailable status code during shutdown instead of the previous 404 status.
- Fixed a potential panic when closing a `pusher` output that was never initialised.
- The `sftp` output now reconnects upon being disconnected by the Azure idle timeout.
- The `switch` output now produces error logs when messages do not pass at least one case with `strict_mode` enabled, previously these rejected messages were potentially re-processed in a loop without any logs depending on the config. An inaccuracy to the documentation has also been fixed in order to clarify behaviour when strict mode is not enabled.
- The `log` processor `fields_mapping` field should no longer reject metadata queries using `@` syntax.
- Fixed an issue where heavily utilised streams with nested resource based outputs could lock-up when performing heavy resource mutating traffic on the streams mode REST API.
- The Bloblang `zip` method no longer produces values that yield an "Unknown data type".

## 4.20.0 - 2023-08-22

### Added

- The `amqp1` input now supports `anonymous` SASL authentication.
- New JWT Bloblang methods `parse_jwt_es256`, `parse_jwt_es384`, `parse_jwt_es512`, `parse_jwt_rs256`, `parse_jwt_rs384`, `parse_jwt_rs512`, `sign_jwt_es256`, `sign_jwt_es384` and `sign_jwt_es512` added.
- The `csv-safe` input codec now supports custom delimiters with the syntax `csv-safe:x`.
- The `open_telemetry_collector` tracer now supports secure connections, enabled via the `secure` field.
- Function `v0_msg_exists_meta` added to the `javascript` processor.

### Fixed

- Fixed an issue where saturated output resources could panic under intense CRUD activity.
- The config linter no longer raises issues with codec fields containing colons within their arguments.
- The `elasticsearch` output should no longer fail to send basic authentication passwords, this fixes a regression introduced in v4.19.0.

## 4.19.0 - 2023-08-17

### Added

- Field `topics_pattern` added to the `pulsar` input.
- Both the `schema_registry_encode` and `schema_registry_decode` processors now support protobuf schemas.
- Both the `schema_registry_encode` and `schema_registry_decode` processors now support references for AVRO and PROTOBUF schemas.
- New Bloblang method `zip`.
- New Bloblang `int8`, `int16`, `uint8`, `uint16`, `float32` and `float64` methods.

### Fixed

- Errors encountered by the `gcp_pubsub` output should now present more specific logs.
- Upgraded `kafka` input and output underlying sarama client library to v1.40.0 at new module path github.com/IBM/sarama
- The CUE schema for `switch` processor now correctly reflects that it takes a list of clauses.
- Fixed the CUE schema for fields that take a 2d-array such as `workflow.order`.
- The `snowflake_put` output has been added back to 32-bit ARM builds since the build incompatibilities have been resolved.
- The `snowflake_put` output and the `sql_*` components no longer trigger a panic when running on a readonly file system with the `snowflake` driver. This driver still requires access to write temporary files somewhere, which can be configured via the Go [`TMPDIR`](https://pkg.go.dev/os#TempDir) environment variable. Details [here](https://github.com/snowflakedb/gosnowflake/issues/700).
- The `http_server` input and output now follow the same multiplexer rules regardless of whether the general `http` server block is used or a custom endpoint.
- Config linting should now respect fields sourced via a merge key (`<<`).
- The `lint` subcommand should now lint config files pointed to via `-r`/`--resources` flags.

### Changed

- The `snowflake_put` output is now beta.
- Endpoints specified by `http_server` components using both the general `http` server block or their own custom server addresses should no longer be treated as path prefixes unless the path ends with a slash (`/`), in which case all extensions of the path will match. This corrects a behavioural change introduced in v4.14.0.

## 4.18.0 - 2023-07-02

### Added

- Field `logger.level_name` added for customising the name of log levels in the JSON format.
- Methods `sign_jwt_rs256`, `sign_jwt_rs384` and `sign_jwt_rs512` added to Bloblang.

### Fixed

- HTTP components no longer ignore `proxy_url` settings when OAuth2 is set.
- The `PATCH` verb for the streams mode REST API no longer fails to patch over newer components implemented with the latest plugin APIs.
- The `nats_jetstream` input no longer fails for configs that set `bind` to `true` and do not specify both a `stream` and `durable` together.
- The `mongodb` processor and output no longer ignores the `upsert` field.

### Changed

- The old `parquet` processor (now superseded by `parquet_encode` and `parquet_decode`) has been removed from 32-bit ARM builds due to build incompatibilities.
- The `snowflake_put` output has been removed from 32-bit ARM builds due to build incompatibilities.
- Plugin API: The `(*BatchError).WalkMessages` method has been deprecated in favour of `WalkMessagesIndexedBy`.

## 4.17.0 - 2023-06-13

### Added

- The `dynamic` input and output have a new endpoint `/input/{id}/uptime` and `/output/{id}/uptime` respectively for obtaining the uptime of a given input/output.
- Field `wait_time_seconds` added to the `aws_sqs` input.
- Field `timeout` added to the `gcp_cloud_storage` output.
- All NATS components now set the name of each connection to the component label when specified.

### Fixed

- Restore message ordering support to `gcp_pubsub` output. This issue was introduced in 4.16.0 as a result of [#1836](https://github.com/benthosdev/benthos/pull/1836).
- Specifying structured metadata values (non-strings) in unit test definitions should no longer cause linting errors.

### Changed

- The `nats` input default value of `prefetch_count` has been increased from `32` to a more appropriate `524288`.

## 4.16.0 - 2023-05-28

### Added

- Fields `auth.user_jwt` and `auth.user_nkey_seed` added to all NATS components.
- bloblang: added `ulid(encoding, random_source)` function to generate Universally Unique Lexicographically Sortable Identifiers (ULIDs).
- Field `skip_on` added to the `cached` processor.
- Field `nak_delay` added to the `nats` input.
- New `splunk_hec` output.
- Plugin API: New `NewMetadataExcludeFilterField` function and accompanying `FieldMetadataExcludeFilter` method added.
- The `pulsar` input and output are now included in the main distribution of Benthos again.
- The `gcp_pubsub` input now adds the metadata field `gcp_pubsub_delivery_attempt` to messages when dead lettering is enabled.
- The `aws_s3` input now adds `s3_version_id` metadata to versioned messages.
- All compress/decompress components (codecs, bloblang methods, processors) now support `pgzip`.
- Field `connection.max_retries` added to the `websocket` input.
- New `sentry_capture` processor.

### Fixed

- The `open_telemetry_collector` tracer option no longer blocks service start up when the endpoints cannot be reached, and instead manages connections in the background.
- The `gcp_pubsub` output should see significant performance improvements due to a client library upgrade.
- The stream builder APIs should now follow `logger.file` config fields.
- The experimental `cue` format in the cli `list` subcommand no longer introduces infinite recursion for `#Processors`.
- Config unit tests no longer execute linting rules for missing env var interpolations.

## 4.15.0 - 2023-05-05

### Added

- Flag `--skip-env-var-check` added to the `lint` subcommand, this disables the new linting behaviour where environment variable interpolations without defaults throw linting errors when the variable is not defined.
- The `kafka_franz` input now supports explicit partitions in the field `topics`.
- The `kafka_franz` input now supports batching.
- New `metadata` Bloblang function for batch-aware structured metadata queries.
- Go API: Running the Benthos CLI with a context set with a deadline now triggers graceful termination before the deadline is reached.
- Go API: New `public/service/servicetest` package added for functions useful for testing custom Benthos builds.
- New `lru` and `ttlru` in-memory caches.

### Fixed

- Provide msgpack plugins through `public/components/msgpack`.
- The `kafka_franz` input should no longer commit offsets one behind the next during partition yielding.
- The streams mode HTTP API should no longer route requests to `/streams/<stream-ID>` to the `/streams` handler. This issue was introduced in v4.14.0.

## 4.14.0 - 2023-04-25

### Added

- The `-e/--env-file` cli flag can now be specified multiple times.
- New `studio pull` cli subcommand for running [Benthos Studio](https://studio.benthos.dev) session deployments.
- Metadata field `kafka_tombstone_message` added to the `kafka` and `kafka_franz` inputs.
- Method `SetEnvVarLookupFunc` added to the stream builder API.
- The `discord` input and output now use the official chat client API and no longer rely on poll-based HTTP requests, this should result in more efficient and less erroneous behaviour.
- New bloblang timestamp methods `ts_add_iso8601` and `ts_sub_iso8601`.
- All SQL components now support the `trino` driver.
- New input codec `csv-safe`.
- Added `base64rawurl` scheme to both the `encode` and `decode` Bloblang methods.
- New `find_by` and `find_all_by` Bloblang methods.
- New `skipbom` input codec.
- New `javascript` processor.

### Fixed

- The `find_all` bloblang method no longer produces results that are of an `unknown` type.
- The `find_all` and `find` Bloblang methods no longer fail when the value argument is a field reference.
- Endpoints specified by HTTP server components using both the general `http` server block or their own custom server addresses should now be treated as path prefixes. This corrects a behavioural change that was introduced when both respective server options were updated to support path parameters.
- Prevented a panic caused when using the `encrypt_aes` and `decrypt_aes` Bloblang methods with a mismatched key/iv lengths.
- The `snowpipe` field of the `snowflake_put` output can now be omitted from the config without raising an error.
- Batch-aware processors such as `mapping` and `mutation` should now report correct error metrics.
- Running `benthos blobl server` should no longer panic when a mapping with variable read/writes is executed in parallel.
- Speculative fix for the `cloudwatch` metrics exporter rejecting metrics due to `minimum field size of 1, PutMetricDataInput.MetricData[0].Dimensions[0].Value`.
- The `snowflake_put` output now prevents silent failures under certain conditions. Details [here](https://github.com/snowflakedb/gosnowflake/issues/701).
- Reduced the amount of pre-compilation of Bloblang based linting rules for documentation fields, this should dramatically improve the start up time of Benthos (~1s down to ~200ms).
- Environment variable interpolations with an empty fallback (`${FOO:}`) are now valid.
- Fixed an issue where the `mongodb` output wasn't using bulk send requests according to batching policies.
- The `amqp_1` input now falls back to accessing `Message.Value` when the data is empty.

### Changed

- When a config contains environment variable interpolations without a default value (i.e. `${FOO}`), if that environment variable is not defined a linting error will be emitted. Shutting down due to linting errors can be disabled with the `--chilled` cli flag, and variables can be specified with an empty default value (`${FOO:}`) in order to make the previous behaviour explicit and prevent the new linting error.
- The `find` and `find_all` Bloblang methods no longer support query arguments as they were incompatible with supporting value arguments. For query based arguments use the new `find_by` and `find_all_by` methods.

## 4.13.0 - 2023-03-15

### Added

- Fix vulnerability [GO-2023-1571](https://pkg.go.dev/vuln/GO-2023-1571)
- New `nats_kv` processor, input and output.
- Field `partition` added to the `kafka_franz` output, allowing for manual partitioning.

### Fixed

- The `broker` output with the pattern `fan_out_sequential` will no longer abandon in-flight requests that are error blocked until the full shutdown timeout has occurred.
- Fixed a regression bug in the `sequence` input where the returned messages have type `unknown`. This issue was introduced in v4.10.0 (cefa288).
- The `broker` input no longer reports itself as unavailable when a child input has intentionally closed.
- Config unit tests that check for structured data should no longer fail in all cases.
- The `http_server` input with a custom address now supports path variables.

## 4.12.1 - 2023-02-23

### Fixed

- Fixed a regression bug in the `nats` components where panics occur during a flood of messages. This issue was introduced in v4.12.0 (45f785a).

## 4.12.0 - 2023-02-20

### Added

- Format `csv:x` added to the `unarchive` processor.
- Field `max_buffer` added to the `aws_s3` input.
- Field `open_message_type` added to the `websocket` input.
- The experimental `--watcher` cli flag now takes into account file deletions and new files that match wildcard patterns.
- Field `dump_request_log_level` added to HTTP components.
- New `couchbase` cache implementation.
- New `compress` and `decompress` Bloblang methods.
- Field `endpoint` added to the `gcp_pubsub` input and output.
- Fields `file_name`, `file_extension` and `request_id` added to the `snowflake_put` output.
- Add interpolation support to the `path` field of the `snowflake_put` output.
- Add ZSTD compression support to the `compression` field of the `snowflake_put` output.
- New Bloblang method `concat`.
- New `redis` ratelimit.
- The `socket_server` input now supports `tls` as a network type.
- New bloblang function `timestamp_unix_milli`.
- New bloblang method `ts_unix_milli`.
- JWT based HTTP authentication now supports `EdDSA`.
- New `flow_control` fields added to the `gcp_pubsub` output.
- Added bloblang methods `sign_jwt_hs256`, `sign_jwt_hs384` and `sign_jwt_hs512`
- New bloblang methods `parse_jwt_hs256`, `parse_jwt_hs384`, `parse_jwt_hs512`.
- The `open_telemetry_collector` tracer now automatically sets the `service.name` and `service.version` tags if they are not configured by the user.
- New bloblang string methods `trim_prefix` and `trim_suffix`.

### Fixed

- Fixed an issue where messages caught in a retry loop from inputs that do not support nacks (`generate`, `kafka`, `file`, etc) could be retried in their post-mutation form from the `switch` output rather than the original copy of the message.
- The `sqlite` buffer should no longer print `Failed to ack buffer message` logs during graceful termination.
- The default value of the `conn_max_idle` field has been changed from 0 to 2 for all `sql_*` components in accordance
to the [`database/sql` docs](https://pkg.go.dev/database/sql#DB.SetMaxIdleConns).
- The `parse_csv` bloblang method with `parse_header_row` set to `false` no longer produces rows that are of an `unknown` type.
- Fixed a bug where the `oracle` driver for the `sql_*` components was returning timestamps which were getting marshalled into an empty JSON object instead of a string.
- The `aws_sqs` input no longer backs off on subsequent empty requests when long polling is enabled.
- It's now possible to mock resources within the main test target file in config unit tests.
- Unit test linting no longer incorrectly expects the `json_contains` predicate to contain a string value only.
- Config component initialisation errors should no longer show nested path annotations.
- Prevented panics from the `jq` processor when querying invalid types.
- The `jaeger` tracer no longer emits the `service.version` tag automatically if the user sets the `service.name` tag explicitly.
- The `int64()`, `int32()`, `uint64()` and `uint32()` bloblang methods can now infer the number base as documented [here](https://pkg.go.dev/strconv#ParseInt).
- The `mapping` and `mutation` processors should provide metrics and tracing events again.
- Fixed a data race in the `redis_streams` input.
- Upgraded the Redis components to `github.com/redis/go-redis/v9`.

## 4.11.0 - 2022-12-21

### Added

- Field `default_encoding` added to the `parquet_encode` processor.
- Field `client_session_keep_alive` added to the `snowflake_put` output.
- Bloblang now supports metadata access via `@foo` syntax, which also supports arbitrary values.
- TLS client certs now support both PKCS#1 and PKCS#8 encrypted keys.
- New `redis_script` processor.
- New `wasm` processor.
- Fields marked as secrets will no longer be printed with `benthos echo` or debug HTTP endpoints.
- Add `no_indent` parameter to the `format_json` bloblang method.
- New `format_xml` bloblang method.
- New `batched` higher level input type.
- The `gcp_pubsub` input now supports optionally creating subscriptions.
- New `sqlite` buffer.
- Bloblang now has `int64`, `int32`, `uint64` and `uint32` methods for casting explicit integer types.
- Field `application_properties_map` added to the `amqp1` output.
- Param `parse_header_row`, `delimiter` and `lazy_quotes` added to the `parse_csv` bloblang method.
- Field `delete_on_finish` added to the `csv` input.
- Metadata fields `header`, `path`, `mod_time_unix` and `mod_time` added to the `csv` input.
- New `couchbase` processor.
- Field `max_attempts` added to the `nsq` input.
- Messages consumed by the `nsq` input are now enriched with metadata.
- New Bloblang method `parse_url`.

### Fixed

- Fixed a regression bug in the `mongodb` processor where message errors were not set any more. This issue was introduced in v4.7.0 (64eb72).
- The `avro-ocf:marshaler=json` input codec now omits unexpected logical type fields.
- Fixed a bug in the `sql_insert` output (see commit c6a71e9) where transaction-based drivers (`clickhouse` and `oracle`) would fail to roll back an in-progress transaction if any of the messages caused an error.
- The `resource` input should no longer block the first layer of graceful termination.

### Changed

- The `catch` method now defines the context of argument mappings to be the string of the caught error. In previous cases the context was undocumented, vague and would often bind to the outer context. It's still possible to reference this outer context by capturing the error (e.g. `.catch(_ -> this)`).
- Field interpolations that fail due to mapping errors will no longer produce placeholder values and will instead provide proper errors that result in nacks or retries similar to other issues.

## 4.10.0 - 2022-10-26

### Added

- The `nats_jetstream` input now adds a range of useful metadata information to messages.
- Field `transaction_type` added to the `azure_table_storage` output, which deprecates the previous `insert_type` field and supports interpolation functions.
- Field `logged_batch` added to the `cassandra` output.
- All `sql` components now support Snowflake.
- New `azure_table_storage` input.
- New `sql_raw` input.
- New `tracing_id` bloblang function.
- New `with` bloblang method.
- Field `multi_header` added to the `kafka` and `kafka_franz` inputs.
- New `cassandra` input.
- New `base64_encode` and `base64_decode` functions for the awk processor.
- Param `use_number` added to the `parse_json` bloblang method.
- Fields `init_statement` and `init_files` added to all sql components.
- New `find` and `find_all` bloblang array methods.

### Fixed

- The `gcp_cloud_storage` output no longer ignores errors when closing a written file, this was masking issues when the target bucket was invalid.
- Upgraded the `kafka_franz` input and output to use github.com/twmb/franz-go@v1.9.0 since some [bug fixes](https://github.com/twmb/franz-go/blob/master/CHANGELOG.md#v190) were made recently.
- Fixed an issue where a `read_until` child input with processors affiliated would block graceful termination.
- The `--labels` linting option no longer flags resource components.

## 4.9.1 - 2022-10-06

### Added

- Go API: A new `BatchError` type added for distinguishing errors of a given batch.

### Fixed

- Rolled back `kafka` input and output underlying sarama client library to fix a regression introduced in 4.9.0 😅 where `invalid configuration (Consumer.Group.Rebalance.GroupStrategies and Consumer.Group.Rebalance.Strategy cannot be set at the same time)` errors would prevent consumption under certain configurations. We've decided to roll back rather than upgrade as a breaking API change was introduced that could cause issues for Go API importers (more info here: https://github.com/Shopify/sarama/issues/2358).

## 4.9.0 - 2022-10-03

### Added

- New `parquet` input for reading a batch of Parquet files from disk.
- Field `max_in_flight` added to the `redis_list` input.

### Fixed

- Upgraded `kafka` input and output underlying sarama client library to fix a regression introduced in 4.7.0 where `The requested offset is outside the range of offsets maintained by the server for the given topic/partition` errors would prevent consumption of partitions.
- The `cassandra` output now inserts logged batches of data rather than the less efficient (and unnecessary) unlogged form.

## 4.8.0 - 2022-09-30

### Added

- All `sql` components now support Oracle DB.

### Fixed

- All SQL components now accept an empty or unspecified `args_mapping` as an alias for no arguments.
- Field `unsafe_dynamic_query` added to the `sql_raw` output.
- Fixed a regression in 4.7.0 where HTTP client components were sending duplicate request headers.

## 4.7.0 - 2022-09-27

### Added

- Field `avro_raw_json` added to the `schema_registry_decode` processor.
- Field `priority` added to the `gcp_bigquery_select` input.
- The `hash` bloblang method now supports `crc32`.
- New `tracing_span` bloblang function.
- All `sql` components now support SQLite.
- New `beanstalkd` input and output.
- Field `json_marshal_mode` added to the `mongodb` input.
- The `schema_registry_encode` and `schema_registry_decode` processors now support Basic, OAuth and JWT authentication.

### Fixed

- The streams mode `/ready` endpoint no longer returns status `503` for streams that gracefully finished.
- The performance of the bloblang `.explode` method now scales linearly with the target size.
- The `influxdb` and `logger` metrics outputs should no longer mix up tag names.
- Fix a potential race condition in the `read_until` connect check on terminated input.
- The `parse_parquet` bloblang method and `parquet_decode` processor now automatically parse `BYTE_ARRAY` values as strings when the logical type is UTF8.
- The `gcp_cloud_storage` output now correctly cleans up temporary files on error conditions when the collision mode is set to append.

## 4.6.0 - 2022-08-31

### Added

- New `squash` bloblang method.
- New top-level config field `shutdown_delay` for delaying graceful termination.
- New `snowflake_id` bloblang function.
- Field `wait_time_seconds` added to the `aws_sqs` input.
- New `json_path` bloblang method.
- New `file_json_contains` predicate for unit tests.
- The `parquet_encode` processor now supports the `UTF8` logical type for columns.

### Fixed

- The `schema_registry_encode` processor now correctly assumes Avro JSON encoded documents by default.
- The `redis` processor `retry_period` no longer shows linting errors for duration strings.
- The `/inputs` and `/outputs` endpoints for dynamic inputs and outputs now correctly render configs, both structured within the JSON response and the raw config string.
- Go API: The stream builder no longer ignores `http` configuration. Instead, the value of `http.enabled` is set to `false` by default.

## 4.5.1 - 2022-08-10

### Fixed

- Reverted `kafka_franz` dependency back to `1.3.1` due to a regression in TLS/SASL commit retention.
- Fixed an unintentional linting error when using interpolation functions in the `elasticsearch` outputs `action` field.

## 4.5.0 - 2022-08-07

### Added

- Field `batch_size` added to the `generate` input.
- The `amqp_0_9` output now supports setting the `timeout` of publish.
- New experimental input codec `avro-ocf:marshaler=x`.
- New `mapping` and `mutation` processors.
- New `parse_form_url_encoded` bloblang method.
- The `amqp_0_9` input now supports setting the `auto-delete` bit during queue declaration.
- New `open_telemetry_collector` tracer.
- The `kafka_franz` input and output now supports no-op SASL options with the mechanism `none`.
- Field `content_type` added to the `gcp_cloud_storage` cache.

### Fixed

- The `mongodb` processor and output default `write_concern.w_timeout` empty value no longer causes configuration issues.
- Field `message_name` added to the logger config.
- The `amqp_1` input and output should no longer spam logs with timeout errors during graceful termination.
- Fixed a potential crash when the `contains` bloblang method was used to compare complex types.
- Fixed an issue where the `kafka_franz` input or output wouldn't use TLS connections without custom certificate configuration.
- Fixed structural cycle in the CUE representation of the `retry` output.
- Tracing headers from HTTP requests to the `http_server` input are now correctly extracted.

### Changed

- The `broker` input no longer applies processors before batching as this was unintentional behaviour and counter to documentation. Users that rely on this behaviour are advised to place their pre-batching processors at the level of the child inputs of the broker.
- The `broker` output no longer applies processors after batching as this was unintentional behaviour and counter to documentation. Users that rely on this behaviour are advised to place their post-batching processors at the level of the child outputs of the broker.

## 4.4.1 - 2022-07-19

### Fixed

- Fixed an issue where an `http_server` input or output would fail to register prometheus metrics when combined with other inputs/outputs.
- Fixed an issue where the `jaeger` tracer was incapable of sending traces to agents outside of the default port.

## 4.4.0 - 2022-07-18

### Added

- The service-wide `http` config now supports basic authentication.
- The `elasticsearch` output now supports upsert operations.
- New `fake` bloblang function.
- New `parquet_encode` and `parquet_decode` processors.
- New `parse_parquet` bloblang method.
- CLI flag `--prefix-stream-endpoints` added for disabling streams mode API prefixing.
- Field `timestamp_name` added to the logger config.

## 4.3.0 - 2022-06-23

### Added

- Timestamp Bloblang methods are now able to emit and process `time.Time` values.
- New `ts_tz` method for switching the timezone of timestamp values.
- The `elasticsearch` output field `type` now supports interpolation functions.
- The `redis` processor has been reworked to be more generally useful, the old `operator` and `key` fields are now deprecated in favour of new `command` and `args_mapping` fields.
- Go API: Added component bundle `./public/components/aws` for all AWS components, including a `RunLambda` function.
- New `cached` processor.
- Go API: New APIs for registering both metrics exporters and open telemetry tracer plugins.
- Go API: The stream builder API now supports configuring a tracer, and tracer configuration is now isolated to the stream being executed.
- Go API: Plugin components can now access input and output resources.
- The `redis_streams` output field `stream` field now supports interpolation functions.
- The `kafka_franz` input and outputs now support `AWS_MSK_IAM` as a SASL mechanism.
- New `pusher` output.
- Field `input_batches` added to config unit tests for injecting a series of message batches.

### Fixed

- Corrected an issue where Prometheus metrics from batching at the buffer level would be skipped when combined with input/output level batching.
- Go API: Fixed an issue where running the CLI API without importing a component package would result in template init crashing.
- The `http` processor and `http_client` input and output no longer have default headers as part of their configuration. A `Content-Type` header will be added to requests with a default value of `application/octet-stream` when a message body is being sent and the configuration has not added one explicitly.
- Logging in `logfmt` mode with `add_timestamp` enabled now works.

## 4.2.0 - 2022-06-03

### Added

- Field `credentials.from_ec2_role` added to all AWS based components.
- The `mongodb` input now supports aggregation filters by setting the new `operation` field.
- New `gcp_cloudtrace` tracer.
- New `slug` bloblang string method.
- The `elasticsearch` output now supports the `create` action.
- Field `tls.root_cas_file` added to the `pulsar` input and output.
- The `fallback` output now adds a metadata field `fallback_error` to messages when shifted.
- New bloblang methods `ts_round`, `ts_parse`, `ts_format`, `ts_strptime`, `ts_strftime`, `ts_unix` and `ts_unix_nano`. Most are aliases of (now deprecated) time methods with `timestamp_` prefixes.
- Ability to write logs to a file (with optional rotation) instead of stdout.

### Fixed

- The default docker image no longer throws configuration errors when running streams mode without an explicit general config.
- The field `metrics.mapping` now allows environment functions such as `hostname` and `env`.
- Fixed a lock-up in the `amqp_0_9` output caused when messages sent with the `immediate` or `mandatory` flags were rejected.
- Fixed a race condition upon creating dynamic streams that self-terminate, this was causing panics in cases where the stream finishes immediately.

## 4.1.0 - 2022-05-11

### Added

- The `nats_jetstream` input now adds headers to messages as metadata.
- Field `headers` added to the `nats_jetstream` output.
- Field `lazy_quotes` added to the CSV input.

### Fixed

- Fixed an issue where resource and stream configs imported via wildcard pattern could not be live-reloaded with the watcher (`-w`) flag.
- Bloblang comparisons between numerical values (including `match` expression patterns) no longer require coercion into explicit types.
- Reintroduced basic metrics from the `twitter` and `discord` template based inputs.
- Prevented a metrics label mismatch when running in streams mode with resources and `prometheus` metrics.
- Label mismatches with the `prometheus` metric type now log errors and skip the metric without stopping the service.
- Fixed a case where empty files consumed by the `aws_s3` input would trigger early graceful termination.

## 4.0.0 - 2022-04-20

This is a major version release, for more information and guidance on how to migrate please refer to [https://benthos.dev/docs/guides/migration/v4](https://www.benthos.dev/docs/guides/migration/v4).

### Added

- In Bloblang it is now possible to reference the `root` of the document being created within a mapping query.
- The `nats_jetstream` input now supports pull consumers.
- Field `max_number_of_messages` added to the `aws_sqs` input.
- Field `file_output_path` added to the `prometheus` metrics type.
- Unit test definitions can now specify a label as a `target_processors` value.
- New connection settings for all sql components.
- New experimental `snowflake_put` output.
- New experimental `gcp_cloud_storage` cache.
- Field `regexp_topics` added to the `kafka_franz` input.
- The `hdfs` output `directory` field now supports interpolation functions.
- The cli `list` subcommand now supports a `cue` format.
- Field `jwt.headers` added to all HTTP client components.
- Output condition `file_json_equals` added to config unit test definitions.

### Fixed

- The `sftp` output no longer opens files in both read and write mode.
- The `aws_sqs` input with `reset_visibility` set to `false` will no longer reset timeouts on pending messages during gracefully shutdown.
- The `schema_registry_decode` processor now handles AVRO logical types correctly. Details in [#1198](https://github.com/benthosdev/benthos/pull/1198) and [#1161](https://github.com/benthosdev/benthos/issues/1161) and also in https://github.com/linkedin/goavro/issues/242.

### Changed

- All components, features and configuration fields that were marked as deprecated have been removed.
- The `pulsar` input and output are no longer included in the default Benthos builds.
- The field `pipeline.threads` field now defaults to `-1`, which automatically matches the host machine CPU count.
- Old style interpolation functions (`${!json:foo,1}`) are removed in favour of the newer Bloblang syntax (`${! json("foo") }`).
- The Bloblang functions `meta`, `root_meta`, `error` and `env` now return `null` when the target value does not exist.
- The `clickhouse` SQL driver Data Source Name format parameters have been changed due to a client library update. This also means placeholders in `sql_raw` components should use dollar syntax.
- Docker images no longer come with a default config that contains generated environment variables, use `-s` flag arguments instead.
- All cache components have had their retry/backoff fields modified for consistency.
- All cache components that support a general default TTL now have a field `default_ttl` with a duration string, replacing the previous field.
- The `http` processor and `http_client` output now execute message batch requests as individual requests by default. This behaviour can be disabled by explicitly setting `batch_as_multipart` to `true`.
- Outputs that traditionally wrote empty newlines at the end of batches with >1 message when using the `lines` codec (`socket`, `stdout`, `file`, `sftp`) no longer do this by default.
- The `switch` output field `retry_until_success` now defaults to `false`.
- All AWS components now have a default `region` field that is empty, allowing environment variables or profile values to be used by default.
- Serverless distributions of Benthos (AWS lambda, etc) have had the default output config changed to reject messages when the processing fails, this should make it easier to handle errors from invocation.
- The standard metrics emitted by Benthos have been largely simplified and improved, for more information [check out the metrics page](https://www.benthos.dev/docs/components/metrics/about).
- The default metrics type is now `prometheus`.
- The `http_server` metrics type has been renamed to `json_api`.
- The `stdout` metrics type has been renamed to `logger`.
- The `logger` configuration section has been simplified, with `logfmt` being the new default format.
- The `logger` field `add_timestamp` is now `false` by default.
- Field `parts` has been removed from all processors.
- Field `max_in_flight` has been removed from a range of output brokers as it no longer required.
- The `dedupe` processor now acts upon individual messages by default, and the `hash` field has been removed.
- The `log` processor now executes for each individual message of a batch.
- The `sleep` processor now executes for each individual message of a batch.
- The `benthos test` subcommand no longer walks when targetting a directory, instead use triple-dot syntax (`./dir/...`) or wildcard patterns.
- Go API: Module name has changed to `github.com/benthosdev/benthos/v4`.
- Go API: All packages within the `lib` directory have been removed in favour of the newer [APIs within `public`](https://pkg.go.dev/github.com/benthosdev/benthos/v4/public).
- Go API: Distributed tracing is now via the Open Telemetry client library.

## 3.65.0 - 2022-03-07

### Added

- New `sql_raw` processor and output.

### Fixed

- Corrected a case where nested `parallel` processors that result in emptied batches (all messages filtered) would propagate an unack rather than an acknowledgement.

### Changed

- The `sql` processor and output are no longer marked as deprecated and will therefore not be removed in V4. This change was made in order to provide more time to migrate to the new `sql_raw` processor and output.

## 3.64.0 - 2022-02-23

### Added

- Field `nack_reject_patterns` added to the `amqp_0_9` input.
- New experimental `mongodb` input.
- Field `cast` added to the `xml` processor and `parse_xml` bloblang method.
- New experimental `gcp_bigquery_select` processor.
- New `assign` bloblang method.
- The `protobuf` processor now supports `Any` fields in protobuf definitions.
- The `azure_queue_storage` input field `queue_name` now supports interpolation functions.

### Fixed

- Fixed an issue where manually clearing errors within a `catch` processor would result in subsequent processors in the block being skipped.
- The `cassandra` output should now automatically match `float` columns.
- Fixed an issue where the `elasticsearch` output would collapse batched messages of matching ID rather than send as individual items.
- Running streams mode with `--no-api` no longer removes the `/ready` endpoint.

### Changed

- The `throttle` processor has now been marked as deprecated.

## 3.63.0 - 2022-02-08

### Added

- Field `cors` added to the `http_server` input and output, for supporting CORS requests when custom servers are used.
- Field `server_side_encryption` added to the `aws_s3` output.
- Field `use_histogram_timing` and `histogram_buckets` added to the `prometheus` metrics exporter.
- New duration string and back off field types added to plugin config builders.
- Experimental field `multipart` added to the `http_client` output.
- Codec `regex` added to inputs.
- Field `timeout` added to the `cassandra` output.
- New experimental `gcp_bigquery_select` input.
- Field `ack_wait` added to the `nats_jetstream` input.

### Changed

- The old map-style resource config fields (`resources.processors.<name>`, etc) are now marked as deprecated. Use the newer list based fields (`processor_resources`, etc) instead.

### Fixed

- The `generate` input now supports zeroed duration strings (`0s`, etc) for unbounded document creation.
- The `aws_dynamodb_partiql` processor no longer ignores the `endpoint` field.
- Corrected duplicate detection for custom cache implementations.
- Fixed panic caused by invalid bounds in the `range` function.
- Resource config files imported now allow (and ignore) a `tests` field.
- Fixed an issue where the `aws_kinesis` input would fail to back off during unyielding read attempts.
- Fixed a linting error with `zmq4` input/output `urls` fields that was incorrectly expecting a string.

## 3.62.0 - 2022-01-21

### Added

- Field `sync` added to the `gcp_pubsub` input.
- New input, processor, and output config field types added to the plugin APIs.
- Added new experimental `parquet` processor.
- New Bloblang method `format_json`.
- Field `collection` in `mongodb` processor and output now supports interpolation functions.
- Field `output_raw` added to the `jq` processor.
- The lambda distribution now supports a `BENTHOS_CONFIG_PATH` environment variable for specifying a custom config path.
- Field `metadata` added to `http` and `http_client` components.
- Field `ordering_key` added to the `gcp_pubsub` output.
- A suite of new experimental `geoip_` methods have been added.
- Added flag `--deprecated` to the `benthos lint` subcommand for detecting deprecated fields.

### Changed

- The `sql` processor and output have been marked deprecated in favour of the newer `sql_insert`, `sql_select` alternatives.

### Fixed

- The input codec `chunked` is no longer capped by the packet size of the incoming streams.
- The `schema_registry_decode` and `schema_registry_encode` processors now honour trailing slashes in the `url` field.
- Processors configured within `pipeline.processors` now share processors across threads rather than clone them.
- Go API: Errors returned from input/output plugin `Close` methods no longer cause shutdown to block.
- The `pulsar` output should now follow authentication configuration.
- Fixed an issue where the `aws_sqs` output might occasionally retry a failed message send with an invalid empty message body.

## 3.61.0 - 2021-12-28

### Added

- Field `json_marshal_mode` added to the MongoDB processor.
- Fields `extract_headers.include_prefixes` and `extract_headers.include_patterns` added to the `http_client` input and output and to the `http` processor.
- Fields `sync_response.metadata_headers.include_prefixes` and `sync_response.metadata_headers.include_patterns` added to the `http_server` input.
- The `http_client` input and output and the `http` processor field `copy_response_headers` has been deprecated in favour of the `extract_headers` functionality.
- Added new cli flag `--no-api` for the `streams` subcommand to disable the REST API.
- New experimental `kafka_franz` input and output.
- Added new Bloblang function `ksuid`.
- All `codec` input fields now support custom csv delimiters.

### Fixed

- Streams mode paths now resolve glob patterns in all cases.
- Prevented the `nats` input from error logging when acknowledgments can't be fulfilled due to the lack of message replies.
- Fixed an issue where GCP inputs and outputs could terminate requests early due to a cancelled client context.
- Prevented more parsing errors in Bloblang mappings with windows style line endings.

## 3.60.1 - 2021-12-03

### Fixed

- Fixed an issue where the `mongodb` output would incorrectly report upsert not allowed on valid operators.

## 3.60.0 - 2021-12-01

### Added

- The `pulsar` input and output now support `oauth2` and `token` authentication mechanisms.
- The `pulsar` input now enriches messages with more metadata.
- Fields `message_group_id`, `message_deduplication_id`, and `metadata` added to the `aws_sns` output.
- Field `upsert` added to the `mongodb` processor and output.

### Fixed

- The `schema_registry_encode` and `schema_registry_decode` processors now honour path prefixes included in the `url` field.
- The `mqtt` input and output `keepalive` field is now interpreted as seconds, previously it was being erroneously interpreted as nanoseconds.
- The header `Content-Type` in the field `http_server.sync_response.headers` is now detected in a case insensitive way when populating multipart message encoding types.
- The `nats_jetstream` input and outputs should now honour `auth.*` config fields.

## 3.59.0 - 2021-11-22

### Added

- New Bloblang method `parse_duration_iso8601` for parsing ISO-8601 duration strings into an integer.
- The `nats` input now supports metadata from headers when supported.
- Field `headers` added to the `nats` output.
- Go API: Optional field definitions added for config specs.
- New (experimental) `sql_select` input.
- New (experimental) `sql_select` and `sql_insert` processors, which will supersede the existing `sql` processor.
- New (experimental) `sql_insert` output, which will supersede the existing `sql` output.
- Field `retained_interpolated` added to the `mqtt` output.
- Bloblang now allows optional carriage returns before line feeds at line endings.
- New CLI flag `-w`/`-watcher` added for automatically detecting and applying configuration file changes.
- Field `avro_raw_json` added to the `schema_registry_encode` processor.
- New (experimental) `msgpack` processor.
- New `parse_msgpack` and `format_msgpack` Bloblang methods.

### Fixed

- Fixed an issue where the `azure_table_storage` output would attempt to send >100 size batches (and fail).
- Fixed an issue in the `subprocess` input where saturated stdout streams could become corrupted.

## 3.58.0 - 2021-11-02

### Added

- `amqp_0_9` components now support TLS EXTERNAL auth.
- Field `urls` added to the `amqp_0_9` input and output.
- New experimental `schema_registry_encode` processor.
- Field `write_timeout` added to the `mqtt` output, and field `connect_timeout` added to both the input and output.
- The `websocket` input and output now support custom `tls` configuration.
- New output broker type `fallback` added as a drop-in replacement for the now deprecated `try` broker.

### Fixed

- Removed a performance bottleneck when consuming a large quantity of small files with the `file` input.

## 3.57.0 - 2021-10-14

### Added

- Go API: New config field types `StringMap`, `IntList`, and `IntMap`.
- The `http_client` input, output and processor now include the response body in request error logs for more context.
- Field `dynamic_client_id_suffix` added to the `mqtt` input and output.

### Fixed

- Corrected an issue where the `sftp` input could consume duplicate documents before shutting down when ran in batch mode.

## 3.56.0 - 2021-09-22

### Added

- Fields `cache_control`, `content_disposition`, `content_language` and `website_redirect_location` added to the `aws_s3` output.
- Field `cors.enabled` and `cors.allowed_origins` added to the server wide `http` config.
- For Kafka components the config now supports the `rack_id` field which may contain a rack identifier for the Kafka client.
- Allow mapping imports in Bloblang environments to be disabled.
- Go API: Isolated Bloblang environments are now honored by all components.
- Go API: The stream builder now evaluates environment variable interpolations.
- Field `unsafe_dynamic_query` added to the `sql` processor.
- The `kafka` output now supports `zstd` compression.

### Fixed

- The `test` subcommand now expands resource glob patterns (`benthos -r "./foo/*.yaml" test ./...`).
- The Bloblang equality operator now returns `false` when comparing non-null values with `null` rather than a mismatched types error.

## 3.55.0 - 2021-09-08

### Added

- New experimental `gcp_bigquery` output.
- Go API: It's now possible to parse a config spec directly with `ParseYAML`.
- Bloblang methods and functions now support named parameters.
- Field `args_mapping` added to the `cassandra` output.
- For NATS, NATS Streaming and Jetstream components the config now supports specifying either `nkey_file` or `user_credentials_file` to configure authentication.

## 3.54.0 - 2021-09-01

### Added

- The `mqtt` input and output now support sending a last will, configuring a keep alive timeout, and setting retained out output messages.
- Go API: New stream builder `AddBatchProducerFunc` and `AddBatchConsumerFunc` methods.
- Field `gzip_compression` added to the `elasticsearch` output.
- The `redis_streams` input now supports creating the stream with the `MKSTREAM` command (enabled by default).
- The `kafka` output now supports manual partition allocation using interpolation functions in the field `partition`.

### Fixed

- The bloblang method `contains` now correctly compares numerical values in arrays and objects.

## 3.53.0 - 2021-08-19

### Added

- Go API: Added ability to create and register `BatchBuffer` plugins.
- New `system_window` buffer for processing message windows (sliding or tumbling) following the system clock.
- Field `root_cas` added to all TLS configuration blocks.
- The `sftp` input and output now support key based authentication.
- New Bloblang function `nanoid`.
- The `gcp_cloud_storage` output now supports custom collision behaviour with the field `collision_mode`.
- Field `priority` added to the `amqp_0_9` output.
- Operator `keys` added to the `redis` processor.
- The `http_client` input when configured in stream mode now allows message body interpolation functions within the URL and header parameters.

### Fixed

- Fixed a panic that would occur when executing a pipeline where processor or input resources reference rate limits.

## 3.52.0 - 2021-08-02

### Added

- The `elasticsearch` output now supports delete, update and index operations.
- Go API: Added ability to create and register `BatchInput` plugins.

### Fixed

- Prevented the `http_server` input from blocking graceful pipeline termination indefinitely.
- Removed annoying nil error log from HTTP client components when parsing responses.

## 3.51.0 - 2021-07-26

### Added

- The `redis_streams`, `redis_pubsub` and `redis_list` outputs now all support batching for higher throughput.
- The `amqp_1` input and output now support passing and receiving metadata as annotations.
- Config unit test definitions can now use files for both the input and expected output.
- Field `track_properties` added to the `azure_queue_storage` input for enriching messages with properties such as the message backlog.
- Go API: The new plugin APIs, available at `./public/service`, are considered stable.
- The streams mode API now uses the setting `http.read_timeout` for timing out stream CRUD endpoints.

### Fixed

- The Bloblang function `random_int` now only resolves dynamic arguments once during the lifetime of the mapping. Documentation has been updated in order to clarify the behaviour with dynamic arguments.
- Fixed an issue where plugins registered would return `failed to obtain docs for X type Y` linting errors.
- HTTP client components are now more permissive regarding invalid Content-Type headers.

## 3.50.0 - 2021-07-19

### Added

- New CLI flag `--set` (`-s`) for overriding arbitrary fields in a config. E.g. `-s input.type=http_server` would override the config setting the input type to `http_server`.
- Unit test definitions now support mocking components.

## 3.49.0 - 2021-07-12

### Added

- The `nats` input now supports acks.
- The `memory` and `file` cache types now expose metrics akin to other caches.

### Fixed

- The `switch` output when `retry_until_success` is set to `false` will now provide granular nacks to pre-batched messages.
- The URL printed in error messages when HTTP client components fail should now show interpolated values as they were interpreted.
- Go Plugins API V2: Batched processors should now show in tracing, and no longer complain about spans being closed more than once.

## 3.48.0 - 2021-06-25

### Added

- Algorithm `lz4` added to the `compress` and `decompress` processors.
- New experimental `aws_dynamodb_partiql` processor.
- Go Plugins API: new run opt `OptUseContext` for an extra shutdown mechanism.

### Fixed

- Fixed an issue here the `http_client` would prematurely drop connections when configured with `stream.enabled` set to `true`.
- Prevented closed output brokers from leaving child outputs running when they've failed to establish a connection.
- Fixed metrics prefixes in streams mode for nested components.

## 3.47.0 - 2021-06-16

### Added

- CLI flag `max-token-length` added to the `blobl` subcommand.
- Go Plugins API: Plugin components can now be configured seamlessly like native components, meaning the namespace `plugin` is no longer required and configuration fields can be placed within the namespace of the plugin itself. Note that the old style (within `plugin`) is still supported.
- The `http_client` input fields `url` and `headers` now support interpolation functions that access metadata and contents of the last received message.
- Rate limit resources now emit `checked`, `limited` and `error` metrics.
- A new experimental plugins API is available for early adopters, and can be found at `./public/x/service`.
- A new experimental template system is available for early adopters, examples can be found in `./template`.
- New beta Bloblang method `bloblang` for executing dynamic mappings.
- All `http` components now support a beta `jwt` authentication mechanism.
- New experimental `schema_registry_decode` processor.
- New Bloblang method `parse_duration` for parsing duration strings into an integer.
- New experimental `twitter_search` input.
- New field `args_mapping` added to the `sql` processor and output for mapping explicitly typed arguments.
- Added format `csv` to the `unarchive` processor.
- The `redis` processor now supports `incrby` operations.
- New experimental `discord` input and output.
- The `http_server` input now adds a metadata field `http_server_verb`.
- New Bloblang methods `parse_yaml` and `format_yaml`.
- CLI flag `env-file` added to Benthos for parsing dotenv files.
- New `mssql` SQL driver for the `sql` processor and output.
- New POST endpoint `/resources/{type}/{id}` added to Benthos streams mode for dynamically mutating resource configs.

### Changed

- Go Plugins API: The Bloblang `ArgSpec` now returns a public error type `ArgError`.
- Components that support glob paths (`file`, `csv`, etc) now also support super globs (double asterisk).
- The `aws_kinesis` input is now stable.
- The `gcp_cloud_storage` input and output are now beta.
- The `kinesis` input is now deprecated.
- Go Plugins API: the minimum version of Go required is now 1.16.

### Fixed

- Fixed a rare panic caused when executing a `workflow` resource processor that references `branch` resources across parallel threads.
- The `mqtt` input with multiple topics now works with brokers that would previously error on multiple subscriptions.
- Fixed initialisation of components configured as resources that reference other resources, where under certain circumstances the components would fail to obtain a true reference to the target resource. This fix makes it so that resources are accessed only when used, which will also make it possible to introduce dynamic resources in future.
- The streams mode endpoint `/streams/{id}/stats` should now work again provided the default manager is used.

## 3.46.1 - 2021-05-19

### Fixed

- The `branch` processor now writes error logs when the request or result map fails.
- The `branch` processor (and `workflow` by proxy) now allow errors to be mapped into the branch using `error()` in the `request_map`.
- Added a linting rule that warns against having a `reject` output under a `switch` broker without `retry_until_success` disabled.
- Prevented a panic or variable corruption that could occur when a Bloblang mapping is executed by parallel threads.

## 3.46.0 - 2021-05-06

### Added

- The `create` subcommand now supports a `--small`/`-s` flag that reduces the output down to only core components and common fields.
- Go Plugins API: Added method `Overlay` to the public Bloblang package.
- The `http_server` input now adds path parameters (`/{foo}/{bar}`) to the metadata of ingested messages.
- The `stdout` output now has a `codec` field.
- New Bloblang methods `format_timestamp_strftime` and `parse_timestamp_strptime`.
- New experimental `nats_jetstream` input and output.

### Fixed

- Go Plugins API: Bloblang method and function plugins now automatically resolve dynamic arguments.

## 3.45.1 - 2021-04-27

### Fixed

- Fixed a regression where the `http_client` input with an empty `payload` would crash with a `url` containing interpolation functions.
- Broker output types (`broker`, `try`, `switch`) now automatically match the highest `max_in_flight` of their children. The field `max_in_flight` can still be manually set in order to enforce a minimum value for when inference isn't possible, such as with dynamic output resources.

## 3.45.0 - 2021-04-23

### Added

- Experimental `azure_renew_lock` field added to the `amqp_1` input.
- New beta `root_meta` function.
- Field `dequeue_visibility_timeout` added to the `azure_queue_storage` input.
- Field `max_in_flight` added to the `azure_queue_storage` output.
- New beta Bloblang methods `format_timestamp_unix` and `format_timestamp_unix_nano`.
- New Bloblang methods `reverse` and `index_of`.
- Experimental `extract_tracing_map` field added to the `kafka` input.
- Experimental `inject_tracing_map` field added to the `kafka` output.
- Field `oauth2.scopes` added to HTTP components.
- The `mqtt` input and output now support TLS.
- Field `enable_renegotiation` added to `tls` configurations.
- Bloblang `if` expressions now support an arbitrary number of `else if` blocks.

### Fixed

- The `checkpoint_limit` field for the `kafka` input now works according to explicit messages in flight rather than the actual offset. This means it now works as expected with compacted topics.
- The `aws_kinesis` input should now automatically recover when the shard iterator has expired.
- Corrected an issue where messages prefixed with valid JSON documents or values were being decoded in truncated form when the remainder was invalid.

### Changed

- The following beta components have been promoted to stable:
  + `ristretto` cache
  + `csv` and `generate` inputs
  + `reject` output
  + `branch`, `jq` and `workflow` processors

## 3.44.1 - 2021-04-15

### Fixed

- Fixed an issue where the `kafka` input with partition balancing wasn't committing offsets.

## 3.44.0 - 2021-04-09

### Added

- The `http_server` input now provides a metadata field `http_server_request_path`.
- New methods `sort_by` and `key_values` added to Bloblang.

### Fixed

- Glob patterns for various components no longer resolve to bad paths in the absence of matches.
- Fixed an issue where acknowledgements from the `azure_queue_storage` input would timeout prematurely, resulting in duplicated message delivery.
- Unit test definitions no longer have implicit test cases when omitted.

## 3.43.1 - 2021-04-05

### Fixed

- Vastly improved Bloblang mapping errors.
- The `azure_blob_storage` input will now gracefully terminate if the client credentials become invalid.
- Prevented the experimental `gcp_cloud_storage` input from closing early during large file consumption.

## 3.43.0 - 2021-03-31

### New

- New (experimental) Apache Pulsar input and output.
- Field `codec` added to the `socket` output.
- New Bloblang method `map_each_key`.
- General config linting improvements.
- Bloblang mappings and interpolated fields within configs are now compile checked during linting.
- New output level `metadata.exclude_prefixes` config field for restricting metadata values sent to the following outputs: `kafka`, `aws_s3`, `amqp_0_9`, `redis_streams`, `aws_sqs`, `gcp_pubsub`.
- All NATS components now have `tls` support.
- Bloblang now supports context capture in query lambdas.
- New subcommand `benthos blobl server` that hosts a Bloblang editor web application.
- New (experimental) `mongodb` output, cache and processor.
- New (experimental) `gcp_cloud_storage` input and output.
- Field `batch_as_multipart` added to the `http_client` output.
- Inputs, outputs, processors, caches and rate limits now have a component level config field `label`, which sets the metrics and logging prefix.
- Resources can now be declared in the new `<component>_resources` fields at the root of config files, the old `resources.<component>s.<label>` style is still valid for backwards compatibility reasons.
- Bloblang mappings now support importing the entirety of a map from a path using `from "<path>"` syntax.

### Fixed

- Corrected ack behaviour for the beta `azure_queue_storage` input.
- Bloblang compressed arithmetic expressions with field names (`foo+bar`) now correctly parse.
- Fixed throughput issues with the `aws_sqs` input.
- Prevented using the `root` keyword within Bloblang queries, returning an error message explaining alternative options. Eventually `root` references within queries will be fully supported and so returning clear errors messages is a temporary fix.
- Increased the offset commit API version used by the `kafka` input to v0.8.2 when consuming explicit partitions.

### Changed

- Go API: Component implementations now require explicit import from `./public/components/all` in order to be invokable. This should be done automatically at all plugin and custom build entry points. If, however, you notice that your builds have begun complaining that known components do not exist then you will need to explicitly import the package with `_ "github.com/Jeffail/benthos/v3/public/components/all"`, if this is the case then please report it as an issue so that it can be dealt with.

## 3.42.1 - 2021-03-26

### Fixed

- Fixed a potential pipeline stall that would occur when non-batched outputs receive message batches.

## 3.42.0 - 2021-02-22

### New

- New `azure_queue_storage` input.
- All inputs with a `codec` field now support multipart.
- New `codec` field added to the `http_client`, `socket`, `socket_server` and `stdin` inputs.
- The `kafka` input now allows an empty consumer group for operating without stored offsets.
- The `kafka` input now supports partition ranges.

### Fixed

- The bloblang `encode` method algorithm `ascii85` no longer returns an error when the input is misaligned.

## 3.41.1 - 2021-02-15

### Fixed

- The `catch` method now properly executes dynamic argument functions.

## 3.41.0 - 2021-02-15

### New

- New `http` fields `cert_file` and `key_file`, which when specified enforce HTTPS for the general Benthos server.
- Bloblang method `catch` now supports `deleted()` as an argument.

### Fixed

- Fixed an issue with custom labels becoming stagnant with the `influxdb` metrics type.
- Fixed a potential unhandled error when writing to the `azure_queue_storage` output.

## 3.40.0 - 2021-02-08

### New

- Experimental `sharded_join` fields added to the `sequence` input.
- Added a new API for writing Bloblang plugins in Go at [`./public/bloblang`](https://pkg.go.dev/github.com/Jeffail/benthos/v3/public/bloblang).
- Field `fields_mapping` added to the `log` processor.

### Fixed

- Prevented pre-existing errors from failing/aborting branch execution in the `branch` and `workflow` processors.
- Fixed `subprocess` processor message corruption with codecs `length_prefixed_uint32_be` and `netstring`.

### Changed

- The `bloblang` input has been renamed to `generate`. This change is backwards compatible and `bloblang` will still be recognized until the next major version release.
- Bloblang more often preserves integer precision in arithmetic operations.

## 3.39.0 - 2021-02-01

### New

- Field `key` in output `redis_list` now supports interpolation functions.
- Field `tags` added to output `aws_s3`.
- New experimental `sftp` input and output.
- New input codec `chunker`.
- New field `import_paths` added to the `protobuf` processor, replaces the now deprecated `import_path` field.
- Added format `concatenate` to the `archive` processor.

### Changed

- The `aws_lambda` processor now adds a metadata field `lambda_function_error` to messages when the function invocation suffers a runtime error.

### Fixed

- Fixed an issue with the `azure_blob_storage` output where `blob_type` set to `APPEND` could result in send failures.
- Fixed a potential panic when shutting down a `socket_server` input with messages in flight.
- The `switch` processor now correctly flags errors on messages that cause a check to throw an error.

## 3.38.0 - 2021-01-18

### New

- New bloblang method `bytes`.
- The bloblang method `index` now works on byte arrays.
- Field `branch_resources` added to the `workflow` processor.
- Field `storage_sas_token` added to the `azure_blob_storage` input and output.
- The bloblang method `hash` and the `hash` processor now support `md5`.
- Field `collector_url` added to the `jaeger` tracer.
- The bloblang method `strip_html` now allows you to specify a list of allowed elements.
- New bloblang method `parse_xml`.
- New bloblang method `replace_many`.
- New bloblang methods `filepath_split` and `filepath_join`.

### Changed

- The `cassandra` outputs `backoff.max_elapsed_time` field was unused and has been hidden from docs.

## 3.37.0 - 2021-01-06

### New

- Field `content_type` and `content_encoding` added to the `amqp_0_9` output.
- Batching fields added to the `hdfs` output.
- Field `codec_send` and `codec_recv` added to the `subprocess` processor.
- Methods `min`, `max`, `abs`, `log`, `log10` and `ceil` added to Bloblang.
- Added field `pattern_paths` to the `grok` processor.
- The `grok` processor now supports dots within field names for nested values.
- New `drop_on` output.

### Fixed

- The `xml` processor now supports non UTF-8 encoding schemes.

### Changed

- The `drop_on_error` output has been deprecated in favour of the new `drop_on` output.

## 3.36.0 - 2020-12-24

### New

- New `influxdb` metrics target.
- New `azure_blob_storage` input.
- New `azure_queue_storage` output.
- The `bloblang` input field `interval` now supports cron expressions.
- New beta `aws_kinesis` and `aws_sqs` inputs.
- The `bool` bloblang method now supports a wider range of string values.
- New `reject` output type for conditionally rejecting messages.
- All Redis components now support clustering and fail-over patterns.
- The `compress` and `decompress` processors now support snappy.

### Fixed

- Fixed a panic on startup when using `if` statements within a `workflow` branch request or response map.
- The `meta` bloblang function error messages now include the name of the required value.
- Config unit tests now report processor errors when checks fail.
- Environment variable interpolations now allow dots within the variable name.

### Changed

- The experimental `aws_s3` input is now marked as beta.
- The beta `kinesis_balanced` input is now deprecated.
- All Azure components have been renamed to include the prefix `azure_`, e.g. `blob_storage` is now `azure_blob_storage`. The old names can still be used for backwards compatibility.
- All AWS components have been renamed to include the prefix `aws_`, e.g. `s3` is now `aws_s3`. The old names can still be used for backwards compatibility.

## 3.35.0 - 2020-12-07

### New

- New field `retry_as_batch` added to the `kafka` output to assist in ensuring message ordering through retries.
- Field `delay_period` added to the experimental `aws_s3` input.
- Added service options for adding API middlewares and specify TLS options for plugin builds.
- Method `not_empty` added to Bloblang.
- New `bloblang` predicate type added to unit tests.
- Unit test case field `target_processors` now allows you to optionally specify a target file.
- Basic auth support added to the `prometheus` metrics pusher.

### Changed

- Unit tests that define environment variables that are run serially (`parallel: false`) will retain those environment variables during execution, as opposed to only at config parse time.
- Lambda distributions now look for config files relative to the binary location, allowing you to deploy configs from the same zip as the binary.

### Fixed

- Add `Content-Type` headers in streams API responses.
- Field `delete_objects` is now respected by the experimental `aws_s3` input.
- Fixed a case where resource processors couldn't access rate limit resources.
- Input files that are valid according to the codec but empty now trigger acknowledgements.
- Mapping `deleted()` within Bloblang object and array literals now correctly omits the values.

## 3.34.0 - 2020-11-20

### New

- New field `format` added to `logger` supporting `json` and `logfmt`.
- The `file` input now provides the metadata field `path` on payloads.

### Fixed

- The `output.sent` metric now properly represents the number of individual messages sent even after archiving batches.
- Fixed a case where metric processors in streams mode pipelines and dynamic components would hang.
- Sync responses of >1 payloads should now get a correct rfc1341 multipart header.
- The `cassandra` output now correctly marshals float and double values.
- The `nanomsg` input with a `SUB` socket no longer attempts to set invalid timeout.

## 3.33.0 - 2020-11-16

### Added

- Added field `codec` to the `file` output.
- The `file` output now supports dynamic file paths.
- Added field `ttl` to the `cache` processor and output.
- New `sql` output, which is similar to the `sql` processor and currently supports Clickhouse, PostgreSQL and MySQL.
- The `kafka` input now supports multiple topics, topic partition balancing, and checkpointing.
- New `cassandra` output.
- Field `allowed_verbs` added to the `http_server` input and output.
- New bloblang function `now`, and method `parse_timestamp`.
- New bloblang methods `floor` and `round`.
- The bloblang method `format_timestamp` now supports strings in ISO 8601 format as well as unix epochs with decimal precision up to nanoseconds.

## Changed

- The `files` output has been deprecated as its behaviour is now covered by `file`.
- The `kafka_balanced` input has now been deprecated as its functionality has been added to the `kafka` input.
- The `cloudwatch` metrics aggregator is now considered stable.
- The `sequence` input is now considered stable.
- The `switch` processor no longer permits cases with no processors.

## Fixed

- Fixed the `tar` and `tar-gzip` input codecs in experimental inputs.
- Fixed a crash that could occur when referencing contextual fields within interpolation functions.
- The `noop` processor can now be inferred with an empty object (`noop: {}`).
- Fixed potential message corruption with the `file` input when using the `lines` codec.

## 3.32.0 - 2020-10-29

### Added

- The `csv` input now supports glob patterns in file paths.
- The `file` input now supports multiple paths, glob patterns, and a range of codecs.
- New experimental `aws_s3` input.
- All `redis` components now support TLS.
- The `-r` cli flag now supports glob patterns.

### Fixed

- Bloblang literals, including method and function arguments, can now be mutated without brackets regardless of where they appear.
- Bloblang maps now work when running bloblang with the `blobl` subcommand.

### Changed

- The `ristretto` cache no longer forces retries on get commands, and the retry fields have been changed in order to reflect this behaviour.
- The `files` input has been deprecated as its behaviour is now covered by `file`.
- Numbers within JSON documents are now parsed in a way that preserves precision even in cases where the number does not fit a 64-bit signed integer or float. When arithmetic is applied to those numbers (either in Bloblang or by other means) the number is converted (and precision lost) at that point based on the operation itself.

  This change means that string coercion on large numbers (e.g. `root.foo = this.large_int.string()`) should now preserve the original form. However, if you are using plugins that interact with JSON message payloads you must ensure that your plugins are able to process the [`json.Number`](https://golang.org/pkg/encoding/json/#Number) type.

  This change should otherwise not alter the behaviour of your configs, but if you notice odd side effects you can disable this feature by setting the environment variable `BENTHOS_USE_NUMBER` to `false` (`BENTHOS_USE_NUMBER=false benthos -c ./config.yaml`). Please [raise an issue](https://github.com/Jeffail/benthos/issues/new) if this is the case so that it can be looked into.

## 3.31.0 - 2020-10-15

### Added

- New input `subprocess`.
- New output `subprocess`.
- Field `auto_ack` added to the `amqp_0_9` input.
- Metric labels can be renamed for `prometheus` and `cloudwatch` metrics components using `path_mapping` by assigning meta fields.

### Fixed

- Metrics labels registered using the `rename` metrics component are now sorted before registering, fixing incorrect values that could potentially be seen when renaming multiple metrics to the same name.

## 3.30.0 - 2020-10-06

### Added

- OAuth 2.0 using the client credentials token flow is now supported by the `http_client` input and output, and the `http` processor.
- Method `format_timestamp` added to Bloblang.
- Methods `re_find_object` and `re_find_all_object` added to Bloblang.
- Field `connection_string` added to the Azure `blob_storage` and `table_storage` outputs.
- Field `public_access_level` added to the Azure `blob_storage` output.
- Bloblang now supports trailing commas in object and array literals and function and method parameters.

### Fixed

- The `amqp_1` input and output now re-establish connections to brokers on any unknown error.
- Batching components now more efficiently attempt a final flush of data during graceful shutdown.
- The `dynamic` output is now more flexible with removing outputs, and should no longer block the API as aggressively.

## 3.29.0 - 2020-09-21

### Added

- New cli flag `log.level` for overriding the configured logging level.
- New integration test suite (much more dapper and also a bit more swanky than the last).

### Changed

- The default value for `batching.count` fields is now zero, which means adding a non-count based batching mechanism without also explicitly overriding `count` no longer incorrectly caps batches at one message. This change is backwards compatible in that working batching configs will not change in behaviour. However, a broken batching config will now behave as expected.

### Fixed

- Improved Bloblang parser error messages for function and method parameters.

## 3.28.0 - 2020-09-14

### Added

- New methods `any`, `all` and `json_schema` added to Bloblang.
- New function `file` added to Bloblang.
- The `switch` output can now route batched messages individually (when using the new `cases` field).
- The `switch` processor now routes batched messages individually (when using the new `cases` field).
- The `workflow` processor can now reference resource configured `branch` processors.
- The `metric` processor now has a field `name` that replaces the now deprecated field `path`. When used the processor now applies to all messages of a batch and the name of the metric is now absolute, without being prefixed by a path generated based on its position within the config.
- New field `check` added to `group_by` processor children, which now replaces the old `condition` field.
- New field `check` added to `while` processor, which now replaces the old `condition` field.
- New field `check` added to `read_until` input, which now replaces the old `condition` field.

### Changed

- The `bloblang` input with an interval configured now emits the first message straight away.

## 3.27.0 - 2020-09-07

### Added

- New function `range` added to Bloblang.
- New beta `jq` processor.
- New driver `clickhouse` added to the `sql` processor.

### Changed

- New field `data_source_name` replaces `dsn` for the `sql` processor, and when using this field each message of a batch is processed individually. When using the field `dsn` the behaviour remains unchanged for backwards compatibility.

### Fixed

- Eliminated situations where an `amqp_0_9` or `amqp_1` component would abandon a connection reset due to partial errors.
- The Bloblang parser now allows naked negation of queries.
- The `cache` processor interpolations for `key` and `value` now cross-batch reference messages before processing.

## 3.26.0 - 2020-08-30

### Added

- New Bloblang methods `not_null` and `filter`.
- New Bloblang function `env`.
- New field `path_mapping` added to all metrics types.
- Field `max_in_flight` added to the `dynamic` output.
- The `workflow` processor has been updated to use `branch` processors with the new field `branches`, these changes are backwards compatible with the now deprecated `stages` field.

### Changed

- The `rename`, `whitelist` and `blacklist` metrics types are now deprecated, and the `path_mapping` field should be used instead.
- The `conditional`, `process_map` and `process_dag` processors are now deprecated and are superseded by the `switch`, `branch` and `workflow` processors respectively.

### Fixed

- Fixed `http` processor error log messages that would print incorrect URLs.
- The `http_server` input now emits `latency` metrics.
- Fixed a panic that could occur during the shutdown of an `http_server` input serving a backlog of requests.
- Explicit component types (`type: foo`) are now checked by the config linter.
- The `amqp_1` input and output should now reconnect automatically after an unexpected link detach.

## 3.25.0 - 2020-08-16

### Added

- Improved parser error messages with the `blobl` subcommand.
- Added flag `file` to the `blobl` subcommand.
- New Bloblang method `parse_timestamp_unix`.
- New beta `protobuf` processor.
- New beta `branch` processor.
- Batching fields added to `s3` output.

### Changed

- The `http` processor field `max_parallel` has been deprecated in favour of rate limits, and the fields within `request` have been moved to the root of the `http` namespace. This change is backwards compatible and `http.request` fields will still be recognized until the next major version release.
- The `process_field` processor is now deprecated, and `branch` should be used instead.

### Fixed

- Wholesale metadata mappings (`meta = {"foo":"bar"}`) in Bloblang now correctly clear pre-existing fields.

## 3.24.1 - 2020-08-03

### Fixed

- Prevented an issue where batched outputs would terminate at start up. Fixes a regression introduced in v3.24.0.

## 3.24.0 - 2020-08-02

### Added

- Endpoint `/ready` added to streams mode API.
- Azure `table_storage` output now supports batched sends.
- All HTTP components are now able to configure a proxy URL.
- New `ristretto` cache.
- Field `shards` added to `memory` cache.

### Fixed

- Batch error handling and retry logic has been improved for the `kafka` and `dynamodb` outputs.
- Bloblang now allows non-matching not-equals comparisons, allowing `foo != null` expressions.

### Changed

- Condition `check_interpolation` has been deprecated.

## 3.23.0 - 2020-07-26

### Added

- Path segments in Bloblang mapping targets can now be quote-escaped.
- New beta `sequence` input, for sequentially chaining inputs.
- New beta `csv` input for consuming CSV files.
- New beta Azure `table_storage` output.
- New `parse_csv` Bloblang method.
- New `throw` Bloblang function.
- The `slice` Bloblang method now supports negative low and high arguments.

### Fixed

- Manual `mqtt` connection handling for both the input and output. This should fix some cases where connections were dropped and never recovered.
- Fixed Bloblang error where calls to a `.get` method would return `null` after the first query.
- The `for_each` processor no longer interlaces child processors during split processing.

## 3.22.0 - 2020-07-19

### Added

- Added TLS fields to `elasticsearch` output.
- New Bloblang methods `encrypt_aes` and `decrypt_aes` added.
- New field `static_headers` added to the `kafka` output.
- New field `enabled` added to the `http` config section.
- Experimental CLI flag `-resources` added for specifying files containing extra resources.

### Fixed

- The `amqp_0_9` now resolves `type` and `key` fields per message of a batch.

## 3.21.0 - 2020-07-12

### Added

- New beta `bloblang` input for generating documents.
- New beta Azure `blob_storage` output.
- Field `sync_response.status` added to `http_server` input.
- New Bloblang `errored` function.

### Fixed

- The `json_schema` processor no longer lower cases fields within error messages.
- The `dynamodb` cache no longer creates warning logs for get misses.

## 3.20.0 - 2020-07-05

### Added

- SASL config fields added to `amqp_1` input and output.
- The `lint` subcommand now supports triple dot wildcard paths: `./foo/...`.
- The `test` subcommand now supports tests defined within the target config file being tested.

### Fixed

- Bloblang boolean operands now short circuit.

## 3.19.0 - 2020-06-28

### Added

- Fields `strict_mode` and `max_in_flight` added to the `switch` output.
- New beta `amqp_1` input and output added.

## 3.18.0 - 2020-06-14

### Added

- Field `drop_empty_bodies` added to the `http_client` input.

### Fixed

- Fixed deleting and skipping maps with the `blobl` subcommand.

## 3.17.0 - 2020-06-07

### Added

- New field `type` added to the `amqp_0_9` output.
- New bloblang methods `explode` and `without`.

### Fixed

- Message functions such as `json` and `content` now work correctly when executing bloblang with the `blobl` sub command.

## 3.16.0 - 2020-05-31

### Added

- New bloblang methods `type`, `join`, `unique`, `escape_html`, `unescape_html`, `re_find_all` and `re_find_all_submatch`.
- Bloblang `sort` method now allows custom sorting functions.
- Bloblang now supports `if` expressions.
- Bloblang now allows joining strings with the `+` operator.
- Bloblang now supports multiline strings with triple quotes.

### Changed

- The `xml` processor is now less strict with XML parsing, allowing unrecognised escape sequences to be passed through unchanged.

### Fixed

- The bloblang method `map_each` now respects `Nothing` mapping by copying the underlying value unchanged.
- It's now possible to reference resource inputs and outputs in streams mode.
- Fixed a problem with compiling old interpolation functions with arguments containing colons (i.e. `${!timestamp_utc:2006-01-02T15:04:05.000Z}`)

## 3.15.0 - 2020-05-24

### Added

- Flag `log` added to `test` sub command to allow logging during tests.
- New subcommand `blobl` added for convenient mapping over the command line.
- Lots of new bloblang methods.

### Fixed

- The `redis_streams` input no longer incorrectly copies message data into a metadata field.

### Changed

- Bloblang is no longer considered beta. Therefore, no breaking changes will be introduced outside of a major version release.

## 3.14.0 - 2020-05-17

### Added

- New `ascii85` and `z85` options have been added to the `encode` and `decode` processors.

### Bloblang BETA Changes

- The `meta` function no longer reflects changes made within the map itself.
- Extracting data from other messages of a batch using `from` no longer reflects changes made within a map.
- Meta assignments are no longer allowed within named maps.
- Assigning `deleted()` to `root` now filters out a message entirely.
- Lots of new methods and goodies.

## 3.13.0 - 2020-05-10

### Added

- New HMAC algorithms added to `hash` processor.
- New beta `bloblang` processor.
- New beta `bloblang` condition.

### Fixed

- Prevented a crash that might occur with high-concurrent access of `http_server` metrics with labels.
- The `http_client` output now respects the `copy_response_headers` field.

## 3.12.0 - 2020-04-19

### Added

- Vastly improved function interpolations, including better batch handling and arithmetic operators.
- The `gcp_pubsub` output now supports function interpolation on the field `topic`.
- New `contains_any` and `contains_any_cs` operators added to the `text` condition.
- Support for input and output `resource` types.
- The `broker` and `switch` output types now allow async messages and batching within child outputs.
- Field `schema_path` added to the `avro` processor.
- The `redis` cache, `redis_list` inputs and outputs now support selecting a database with the URL path.
- New field `max_in_flight` added to the `broker` output.

### Changed

- Benthos now runs in strict mode, but this can be disabled with `--chilled`.
- The Benthos CLI has been revamped, the old flags are still supported but are deprecated.
- The `http_server` input now accepts requests without a content-type header.

### Fixed

- Outputs that resolve function interpolations now correctly resolve the `batch_size` function.
- The `kinesis_balanced` input now correctly establishes connections.
- Fixed an auth transport issue with the `gcp_pubsub` input and output.

## 3.11.0 - 2020-03-08

### Added

- Format `syslog_rfc3164` added to the `parse_log` processor.
- New `multilevel` cache.
- New `json_append`, `json_type` and `json_length` functions added to the `awk` processor.
- New `flatten` operator added to the `json` processor.

### Changed

- Processors that fail now set the opentracing tag `error` to `true`.

### Fixed

- Kafka connectors now correctly set username and password for all SASL strategies.

## 3.10.0 - 2020-02-05

### Added

- Field `delete_files` added to `files` input.
- TLS fields added to `nsq` input and output.
- Field `processors` added to batching fields to easily accommodate aggregations and archiving of batched messages.
- New `parse_log` processor.
- New `json` condition.
- Operators `flatten_array`, `fold_number_array` and `fold_string_array` added to `json` processor.

### Changed

- The `redis_streams` input no longer flushes >1 fetched messages as a batch.

### Fixed

- Re-enabled Kafka connections using SASL without TLS.

## 3.9.0 - 2020-01-27

### Added

- New `socket`, `socket_server` inputs.
- New `socket` output.
- Kafka connectors now support SASL using `OAUTHBEARER`, `SCRAM-SHA-256`, `SCRAM-SHA-512` mechanisms.
- Experimental support for AWS CloudWatch metrics.

### Changed

- The `tcp`, `tcp_server` and `udp_server` inputs have been deprecated and moved into the `socket` and `socket_server` inputs respectively.
- The `udp` and `tcp` outputs have been deprecated and moved into the `socket` output.

### Fixed

- The `subprocess` processor now correctly flags errors that occur.

## 3.8.0 - 2020-01-17

### Added

- New field `max_in_flight` added to the following outputs:
  + `amqp_0_9`
  + `cache`
  + `dynamodb`
  + `elasticsearch`
  + `gcp_pubsub`
  + `hdfs`
  + `http_client`
  + `kafka`
  + `kinesis`
  + `kinesis_firehose`
  + `mqtt`
  + `nanomsg`
  + `nats`
  + `nats_stream`
  + `nsq`
  + `redis_hash`
  + `redis_list`
  + `redis_pubsub`
  + `redis_streams`
  + `s3`
  + `sns`
  + `sqs`
- Batching fields added to the following outputs:
  + `dynamodb`
  + `elasticsearch`
  + `http_client`
  + `kafka`
  + `kinesis`
  + `kinesis_firehose`
  + `sqs`
- More TRACE level logs added throughout the pipeline.
- Operator `delete` added to `cache` processor.
- Operator `explode` added to `json` processor.
- Field `storage_class` added to `s3` output.
- Format `json_map` added to `unarchive` processor.

### Fixed

- Function interpolated strings within the `json` processor `value` field are now correctly unicode escaped.
- Retry intervals for `kafka` output have been tuned to prevent circuit breaker throttling.

## 3.7.0 - 2019-12-21

### Added

- New `try` output, which is a drop-in replacement for a `broker` with the `try` pattern.
- Field `successful_on` added to the `http` processor.
- The `statsd` metrics type now supports Datadog or InfluxDB tagging.
- Field `sync_response.headers` added to `http_server` input.
- New `sync_response` processor.
- Field `partitioner` added to the `kafka` output.

### Changed

- The `http` processor now gracefully handles empty responses.

### Fixed

- The `kafka` input should now correctly recover from coordinator failures during an offset commit.
- Attributes permitted by the `sqs` output should now have parity with real limitations.

## 3.6.1 - 2019-12-05

### Fixed

- Batching using an input `broker` now works with only one child input configured.
- The `zmq4` input now correctly supports broker based batching.

## 3.6.0 - 2019-12-03

### Added

- New `workflow` processor.
- New `resource` processor.
- Processors can now be registered within the `resources` section of a config.

### Changed

- The `mqtt` output field `topic` field now supports interpolation functions.

### Fixed

- The `kafka` output no longer attempts to send headers on old versions of the protocol.

## 3.5.0 - 2019-11-26

### Added

- New `regexp_expand` operator added to the `text` processor.
- New `json_schema` processor.

## 3.4.0 - 2019-11-12

### Added

- New `amqp_0_9` output which replaces the now deprecated `amqp` output.
- The `broker` output now supports batching.

### Fixed

- The `memory` buffer now allows parallel processing of batched payloads.
- Version and date information should now be correctly displayed in archive distributions.

## 3.3.1 - 2019-10-21

### Fixed

- The `s3` input now correctly unescapes bucket keys when streaming from SQS.

## 3.3.0 - 2019-10-20

### Added

- Field `sqs_endpoint` added to the `s3` input.
- Field `kms_key_id` added to the `s3` output.
- Operator `delete` added to `metadata` processor.
- New experimental metrics aggregator `stdout`.
- Field `ack_wait` added to `nats_stream` input.
- New `batching` field added to `broker` input for batching merged streams.
- Field `healthcheck` added to `elasticsearch` output.
- New `json_schema` condition.

### Changed

- Experimental `kafka_cg` input has been removed.
- The `kafka_balanced` inputs underlying implementation has been replaced with the `kafka_cg` one.
- All inputs have been updated to automatically utilise >1 processing threads, with the exception of `kafka` and `kinesis`.

## 3.2.0 - 2019-09-27

### Added

- New `is` operator added to `text` condition.
- New config unit test condition `content_matches`.
- Field `init_values` added to the `memory` cache.
- New `split` operator added to `json` processor.
- Fields `user` and `password` added to `mqtt` input and output.
- New experimental `amqp_0_9` input.

### Changed

- Linting is now disabled for the environment var config shipped with docker images, this should prevent the log spam on start up.
- Go API: Experimental `reader.Async` component methods renamed.

## 3.1.1 - 2019-09-23

### Fixed

- Prevented `kafka_cg` input lock up after batch policy period trigger with no backlog.

## 3.1.0 - 2019-09-23

### Added

- New `redis` processor.
- New `kinesis_firehose` output.
- New experimental `kafka_cg` input.
- Go API: The `metrics.Local` aggregator now supports labels.

### Fixed

- The `json` processor no longer removes content moved from a path to the same path.

## 3.0.0 - 2019-09-17

This is a major version release, for more information and guidance on how to migrate please refer to [https://benthos.dev/docs/guides/migration/v3](https://www.benthos.dev/docs/guides/migration/v3).

### Added

- The `json` processor now allows you to `move` from either a root source or to a root destination.
- Added interpolation to the `metadata` processor `key` field.
- Granular retry fields added to `kafka` output.

### Changed

- Go modules are now fully supported, imports must now include the major version (e.g. `github.com/Jeffail/benthos/v3`).
- Removed deprecated `mmap_file` buffer.
- Removed deprecated (and undocumented) metrics paths.
- Moved field `prefix` from root of `metrics` into relevant child components.
- Names of `process_dag` stages must now match the regexp `[a-zA-Z0-9_-]+`.
- Go API: buffer constructors now take a `types.Manager` argument in parity with other components.
- JSON dot paths within the following components have been updated to allow array-based operations:
  + `awk` processor
  + `json` processor
  + `process_field` processor
  + `process_map` processor
  + `check_field` condition
  + `json_field` function interpolation
  + `s3` input
  + `dynamodb` output

### Fixed

- The `sqs` output no longer attempts to send invalid attributes with payloads from metadata.
- During graceful shutdown Benthos now scales the attempt to propagate acks for sent messages with the overall system shutdown period.

## 2.15.1 - 2019-09-10

### Fixed

- The `s3` and `sqs` inputs should now correctly log handles and codes from failed SQS message deletes and visibility timeout changes.

## 2.15.0 - 2019-09-03

### Added

- New `message_group_id` and `message_deduplication_id` fields added to `sqs` output for supporting FIFO queues.

## 2.14.0 - 2019-08-29

### Added

- Metadata field `gcp_pubsub_publish_time_unix` added to `gcp_pubsub` input.
- New `tcp` and `tcp_server` inputs.
- New `udp_server` input.
- New `tcp` and `udp` outputs.
- Metric paths `output.batch.bytes` and `output.batch.latency` added.
- New `rate_limit` processor.

### Fixed

- The `json` processor now correctly stores parsed `value` JSON when using `set` on the root path.

## 2.13.0 - 2019-08-27

### Added

- The `sqs` input now adds some message attributes as metadata.
- Added field `delete_message` to `sqs` input.
- The `sqs` output now sends metadata as message attributes.
- New `batch_policy` field added to `memory` buffer.
- New `xml` processor.

### Fixed

- The `prometheus` metrics exporter adds quantiles back to timing metrics.

## 2.12.2 - 2019-08-19

### Fixed

- Capped slices from lines reader are now enforced.
- The `json` processor now correctly honours a `null` value.

## 2.12.1 - 2019-08-16

### Changed

- Disabled `kinesis_balanced` input for WASM builds.

## 2.12.0 - 2019-08-16

### Added

- Field `codec` added to `process_field` processor.
- Removed experimental status from sync responses components, which are now considered stable.
- Field `pattern_definitions` added to `grok` processor.

### Changed

- Simplified serverless lambda main function body for improving plugin documentation.

### Fixed

- Fixed a bug where the `prepend` and `append` operators of the `text` processor could result in invalid messages when consuming line-based inputs.

## 2.11.2 - 2019-08-06

### Added

- Field `clean_session` added to `mqtt` input.
- The `http_server` input now adds request query parameters to messages as metadata.

## 2.11.1 - 2019-08-05

### Fixed

- Prevent concurrent access race condition on nested parallel `process_map` processors.

## 2.11.0 - 2019-08-03

### Added

- New beta input `kinesis_balanced`.
- Field `profile` added to AWS components credentials config.

## 2.10.0 - 2019-07-29

### Added

- Improved error messages attached to payloads that fail `process_dag`. post mappings.
- New `redis_hash` output.
- New `sns` output.

## 2.9.3 - 2019-07-18

### Added

- Allow extracting metric `rename` submatches into labels.
- Field `use_patterns` added to `redis_pubsub` input for subscribing to channels using glob-style patterns.

## 2.9.2 - 2019-07-17

### Changed

- Go API: It's now possible to specify a custom config unit test file path suffix.

## 2.9.1 - 2019-07-15

### Added

- New rate limit and websocket message fields added to `http_server` input.
- The `http` processor now optionally copies headers from response into resulting message metadata.
- The `http` processor now sets a `http_status_code` metadata value into resulting messages (provided one is received.)

### Changed

- Go API: Removed experimental `Block` functions from the cache and rate limit packages.

## 2.9.0 - 2019-07-12

### Added

- New (experimental) command flags `--test` and `--gen-test` added.
- All http client components output now set a metric `request_timeout`.

## 2.8.6 - 2019-07-10

### Added

- All errors caught by processors should now be accessible via the `${!error}` interpolation function, rather than just flagged as `true`.

### Fixed

- The `process_field` processor now propagates metadata to the original payload with the `result_type` set to discard. This allows proper error propagation.

## 2.8.5 - 2019-07-03

### Added

- Field `max_buffer` added to `subprocess` processor.

### Fixed

- The `subprocess` processor now correctly logs and recovers subprocess pipeline related errors (such as exceeding buffer limits.)

## 2.8.4 - 2019-07-02

### Added

- New `json_delete` function added to the `awk` processor.

### Fixed

- SQS output now correctly waits between retry attempts and escapes error loops during shutdown.

## 2.8.3 - 2019-06-28

### Added

- Go API: Add `RunWithOpts` opt `OptOverrideConfigDefaults`.

### Fixed

- The `filter` and `filter_parts` config sections now correctly marshall when printing with `--all`.

## 2.8.2 - 2019-06-28

### Added

- Go API: A new service method `RunWithOpts` has been added in order to accomodate service customisations with opt funcs.

## 2.8.1 - 2019-06-28

- New interpolation function `error`.

## 2.8.0 - 2019-06-24

### Added

- New `number` condition.
- New `number` processor.
- New `avro` processor.
- Operator `enum` added to `text` condition.
- Field `result_type` added to `process_field` processor for marshalling results into non-string types.
- Go API: Plugin APIs now allow nil config constructors.
- Registering plugins automatically adds plugin documentation flags to the main Benthos service.

## 2.7.0 - 2019-06-20

### Added

- Output `http_client` is now able to propagate responses from each request back to inputs supporting sync responses.
- Added support for Gzip compression to `http_server` output sync responses.
- New `check_interpolation` condition.

## 2.6.0 - 2019-06-18

### Added

- New `sync_response` output type, with experimental support added to the `http_server` input.
- SASL authentication fields added to all Kafka components.

## 2.5.0 - 2019-06-14

### Added

- The `s3` input now sets `s3_content_encoding` metadata (when not using the download manager.)
- New trace logging for the `rename`, `blacklist` and `whitelist` metric components to assist with debugging.

## 2.4.0 - 2019-06-06

### Added

- Ability to combine sync and async responses in serverless distributions.

### Changed

- The `insert_part`, `merge_json` and `unarchive` processors now propagate message contexts.

## 2.3.2 - 2019-06-05

### Fixed

- JSON processors no longer escape `&`, `<`, and `>` characters by default.

## 2.3.1 - 2019-06-04

### Fixed

- The `http` processor now preserves message metadata and contexts.
- Any `http` components that create requests with messages containing empty bodies now correctly function in WASM.

## 2.3.0 - 2019-06-04

### Added

- New `fetch_buffer_cap` field for `kafka` and `kafka_balanced` inputs.
- Input `gcp_pubsub` now has the field `max_batch_count`.

### Changed

- Reduced allocations under most JSON related processors.
- Streams mode API now logs linting errors.

## 2.2.4 - 2019-06-02

### Added

- New interpolation function `batch_size`.

## 2.2.3 - 2019-05-31

### Fixed

- Output `elasticsearch` no longer reports index not found errors on connect.

## 2.2.2 - 2019-05-30

### Fixed

- Input reader no longer overrides message contexts for opentracing spans.

## 2.2.1 - 2019-05-29

### Fixed

- Improved construction error messages for `broker` and `switch` input and outputs.

### Changed

- Plugins that don't use a configuration structure can now return nil in their sanitise functions in order to have the plugin section omitted.

## 2.2.0 - 2019-05-22

### Added

- The `kafka` and `kafka_balanced` inputs now set a `kafka_lag` metadata field to incoming messages.
- The `awk` processor now has a variety of typed `json_set` functions `json_set_int`, `json_set_float` and `json_set_bool`.
- Go API: Add experimental function for blocking cache and ratelimit constructors.

### Fixed

- The `json` processor now defaults to an executable operator (clean).

## 2.1.3 - 2019-05-20

### Added

- Add experimental function for blocking processor constructors.

## 2.1.2 - 2019-05-20

### Added

- Core service logic has been moved into new package `service`, making it easier to maintain plugin builds that match upstream Benthos.

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

This is a major version released due to a series of minor breaking changes, you can read the [full migration guide here](https://www.benthos.dev/docs/guides/migration/v2).

#### Configuration

- Benthos now attempts to infer the `type` of config sections whenever the field is omitted, for more information please read this overview: [Concise Configuration](https://www.benthos.dev/docs/configuration/about#concise-configuration).
- Field `unsubscribe_on_close` of the `nats_stream` input is now `false` by default.

#### Service

- The following commandline flags have been removed: `swap-envs`, `plugins-dir`, `list-input-plugins`, `list-output-plugins`, `list-processor-plugins`, `list-condition-plugins`.

#### Go API

- Package `github.com/Jeffail/benthos/lib/processor/condition` changed to `github.com/Jeffail/benthos/lib/condition`.
- Interface `types.Cache` now has `types.Closable` embedded.
- Interface `types.RateLimit` now has `types.Closable` embedded.
- Add method `GetPlugin` to interface `types.Manager`.
- Add method `WithFields` to interface `log.Modular`.

## 1.20.4 - 2019-05-13

### Fixed

- Ensure `process_batch` processor gets normalised correctly.

## 1.20.3 - 2019-05-11

### Added

- New `for_each` processor with the same behaviour as `process_batch`, `process_batch` is now considered an alias for `for_each`.

## 1.20.2 - 2019-05-10

### Changed

- The `sql` processor now executes across the batch, documentation updated to clarify.

## 1.20.1 - 2019-05-10

### Fixed

- Corrected `result_codec` field in `sql` processor config.

## 1.20.0 - 2019-05-10

### Added

- New `sql` processor.

### Fixed

- Using `json_map_columns` with the `dynamodb` output should now correctly store `null` and array values within the target JSON structure.

## 1.19.2 - 2019-05-09

### Added

- New `encode` and `decode` scheme `hex`.

### Fixed

- Fixed potential panic when attempting an invalid HTTP client configuration.

## 1.19.1 - 2019-05-08

### Fixed

- Benthos in streams mode no longer tries to load directory `/benthos/streams` by default.

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

- The `benthos-lambda` distribution now correctly returns all message parts in synchronous execution.

### Changed

- Docker builds now use a locally cached `vendor` for dependencies.
- All `s3` components no longer default to enforcing path style URLs.

## 1.16.0 - 2019-04-30

### Added

- New output `drop_on_error`.
- Field `retry_until_success` added to `switch` output.

### Fixed

- Improved error and output logging for `subprocess` processor when the process exits unexpectedly.

## 1.15.0 - 2019-04-26

### Changed

- The main docker image is now based on busybox.
- Lint rule added for `batch` processors outside of the input section.

## 1.14.3 - 2019-04-25

### Fixed

- Removed potential `benthos-lambda` panic on shut down.

## 1.14.2 - 2019-04-25

### Fixed

- The `redis` cache no longer incorrectly returns a "key not found" error instead of connection errors.

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
- Configs sent via the streams mode HTTP API are now interpolated with environment variable substitutes.

### Changed

- All AWS `s3` components now enforce path style syntax for bucket URLs. This improves compatibility with third party endpoints.

## 1.11.0 - 2019-04-12

### Added

- New `parallel` processor.

### Fixed

- The `dynamodb` cache `get` call now correctly reports key not found versus general request error.

## 1.10.10 - 2019-04-10

### Added

- New `sqs_bucket_path` field added to `s3` input.

### Fixed

- The `sqs` input now rejects messages that fail by resetting the visibility timeout.
- The `sqs` input no longer fails to delete consumed messages when the batch contains duplicate message IDs.

## 1.10.9 - 2019-04-05

### Fixed

- The `metric` processor no longer mixes label keys when processing across parallel pipelines.

## 1.10.8 - 2019-04-03

### Added

- Comma separated `kafka` and `kafka_balanced` address and topic values are now trimmed for whitespace.

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

- The `while` processor now correctly checks conditions against the first batch of the result of last processor loop.

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

- Field `dependencies` of children of the `process_dag` processor now correctly parsed from config files.

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
- The `nats_stream` input now has a field `unsubscribe_on_close` that when disabled allows durable subscription offsets to persist even when all connections are closed.
- Metadata field `nats_stream_sequence` added to `nats_stream` input.

## 1.5.1 - 2019-02-11

### Fixed

- The `subprocess` processor no longer sends unexpected empty lines when messages end with a line break.

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

- Output broker types now correctly allocates nested processors for `fan_out` and `try` patterns.
- JSON formatted loggers now correctly escape error messages with line breaks.

## 1.3.0 - 2019-01-29

### Added

- Improved error logging for `s3` input download failures.
- More metadata fields copied to messages from the `s3` input.
- Field `push_url` added to the `prometheus` metrics target.

## 1.2.1 - 2019-01-28

### Added

- Resources (including plugins) that implement `Closable` are now shutdown cleanly.

## 1.2.0 - 2019-01-28

### Added

- New `json_array` format added to the `archive` and `unarchive` processors.
- Preliminary support added to the resource manager API to allow arbitrary shared resource plugins.

## 1.1.4 - 2019-01-23

### Fixed

- The `s3` input now caps and iterates batched SQS deletes.

## 1.1.3 - 2019-01-22

### Fixed

- The `archive` processor now interpolates the `path` per message of the batch.

## 1.1.2 - 2019-01-21

### Fixed

- Fixed environment variable interpolation when combined with embedded function interpolations.
- Fixed break down metric indexes for input and output brokers.

## 1.1.0 - 2019-01-17

### Added

- Input `s3` can now toggle the use of a download manager, switching off now downloads metadata from the target file.
- Output `s3` now writes metadata to the uploaded file.
- Operator `unescape_url_query` added to `text` processor.

### Fixed

- The `nats_steam` input and output now actively attempt to recover stale connections.
- The `awk` processor prints errors and flags failure when the program exits with a non-zero status.

## 1.0.2 - 2019-01-07

### Fixed

- The `subprocess` processor now attempts to read all flushed stderr output from a process when it fails.

## 1.0.1 - 2019-01-05

### Added

- Function `print_log` added to `awk` processor.

### Fixed

- The `awk` processor function `json_get` no longer returns string values with quotes.

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

- Functions `timestamp_format`, `timestamp_format_nano`, `metadata_get` and `metadata_set` added to `awk` processor.

## 0.42.0 - 2018-12-19

### Added

- New `sleep` processor.
- New `awk` processor.

### Changed

- Converted all integer based time period fields to string based, e.g. `timeout_ms: 5000` would now be `timeout: 5s`. This will may potentially be disruptive but the `--strict` flag should catch all deprecated fields in an existing config.

## 0.41.0 - 2018-12-12

### Changed

- Renamed `max_batch_size` to `max_batch_count` for consistency.

## 0.40.2 - 2018-12-12

### Added

- New `max_batch_size` field added to `kafka`, `kafka_balanced` and `amqp` inputs. This provides a mechanism for creating message batches optimistically.

## 0.40.0 - 2018-12-10

### Added

- New `subprocess` processor.

### Changed

- API: The `types.Processor` interface has been changed in order to add lifetime cleanup methods (added `CloseAsync` and `WaitForClose`). For the overwhelming majority of processors these functions will be no-ops.
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

- New endpoint `/ready` that returns 200 when both the input and output components are connected, otherwise 503. This is intended to be used as a readiness probe.

### Changed

- Large simplifications to all metrics paths.
- Fully removed the previously deprecated `combine` processor.
- Input and output plugins updated to support new connection health checks.

## 0.38.10 - 2018-12-04

### Added

- Field `role_external_id` added to all S3 credential configs.
- New `processor_failed` condition and improved processor error handling which can be read about [here](./docs/error_handling.md)

## 0.38.8 - 2018-11-29

### Added

- New `content_type` field for the `s3` output.

## 0.38.6 - 2018-11-28

### Added

- New `group_by_value` processor.

## 0.38.5 - 2018-11-27

### Added

- Lint errors are logged (level INFO) during normal Benthos operation.
- New `--strict` command flag which causes Benthos to abort when linting errors are found in a config file.

## 0.38.4 - 2018-11-26

### Added

- New `--lint` command flag for linting config files.

## 0.38.1 - 2018-11-23

### Changed

- The `s3` output now attempts to batch uploads.
- The `s3` input now exposes errors in deleting SQS messages during acks.

## 0.38.0 - 2018-11-22

### Changed

- Resource based conditions no longer benefit from cached results. In practice this optimisation was easy to lose in config and difficult to maintain.

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

- Removed submatch indexes in the `text` processor `find_regexp` operator and added documentation for expanding submatches in the `replace_regexp` operator.

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

- The `s3` output now calculates `path` field function interpolations per message of a batch.

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

- The `process_map` processor now allows map target path overrides when a target is the parent of another target.

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

- The `combine` processor is now considered DEPRECATED, please use the `batch` processor instead.
- The `batch` processor field `byte_size` is now set at 0 (and therefore ignored) by default. A log warning has been added in case anyone was relying on the default.

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

- The `kinesis` output field `retries` has been renamed `max_retries` in order to expose the difference in its zero value behaviour (endless retries) versus other `retry` fields (zero retries).

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

- The `kinesis` output type now supports batched sends and per message interpolation.

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

- The `index` field of the `elasticsearch` output can now be dynamically set using function interpolation.
- New `hash` processor.

### Changed

- API: The `metrics.Type` interface has been changed in order to add labels.

## 0.24.0 - 2018-08-17

### Changed

- Significant restructuring of `amqp` inputs and outputs. These changes should be backwards compatible for existing pipelines, but changes the way in which queues, exchanges and bindings are declared using these types.

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
- Field `commit_period_ms` for `kafka` and `kafka_balanced` inputs for specifying a commit period.

## 0.23.1 - 2018-08-06

### Added

- New `retries` field to `s3` input, to cap the number of download attempts made on the same bucket item.
- Added metadata based mechanism to detect final message from a `read_until` input.
- Added field to `split` processor for specifying target batch sizes.

## 0.23.0 - 2018-08-06

### Added

- Metadata fields are now per message part within a batch.
- New `metadata_json_object` function interpolation to return a JSON object of metadata key/value pairs.

### Changed

- The `metadata` function interpolation now allows part indexing and no longer returns a JSON object when no key is specified, this behaviour can now be done using the `metadata_json_object` function.

## 0.22.0 - 2018-08-03

### Added

- Fields for the `http` processor to enable parallel requests from message batches.

### Changed

- Broker level output processors are now applied _before_ the individual output processors.
- The `dynamic` input and output HTTP paths for CRUD operations are now `/inputs/{input_id}` and `/outputs/{output_id}` respectively.
- Removed deprecated `amazon_s3`, `amazon_sqs` and `scalability_protocols` input and output types.
- Removed deprecated `json_fields` field from the `dedupe` processor.

## 0.21.0 - 2018-07-31

### Added

- Add conditions to `process_map` processor.

### Changed

- TLS config fields have been cleaned up for multiple types. This affects the `kafka`, `kafka_balanced` and `http_client` input and output types, as well as the `http` processor type.

## 0.20.8 - 2018-07-30

### Added

- New `delete_all` and `delete_prefix` operators for `metadata` processor.
- More metadata fields extracted from the AMQP input.
- HTTP clients now support function interpolation on the URL and header values, this includes the `http_client` input and output as well as the `http` processor.

## 0.20.7 - 2018-07-27

### Added

- New `key` field added to the `dedupe` processor, allowing you to deduplicate using function interpolation. This deprecates the `json_paths` array field.

## 0.20.6 - 2018-07-27

### Added

- New `s3` and `sqs` input and output types, these replace the now deprecated `amazon_s3` and `amazon_sqs` types respectively, which will eventually be removed.
- New `nanomsg` input and output types, these replace the now deprecated `scalability_protocols` types, which will eventually be removed.

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

- New config interpolator function `json_field` for extracting parts of a JSON message into a config value.

### Changed

- Log level config field no longer stutters, `logger.log_level` is now `logger.level`.

## 0.19.1 - 2018-07-25

### Added

- Ability to create batches via conditions on message payloads in the `batch` processor.
- New `--examples` flag for generating specific examples from Benthos.

## 0.19.0 - 2018-07-23

### Added

- New `text` processor.

### Changed

- Processor `process_map` replaced field `strict_premapping` with `premap_optional`.

## 0.18.0 - 2018-07-20

### Added

- New `process_field` processor.
- New `process_map` processor.

### Changed

- Removed mapping fields from the `http` processor, this behaviour has been put into the new `process_map` processor instead.

## 0.17.0 - 2018-07-17

### Changed

- Renamed `content` condition type to `text` in order to clarify its purpose.

## 0.16.4 - 2018-07-17

### Added

- Latency metrics for caches.
- TLS options for `kafka` and `kafka_partitions` inputs and outputs.

### Changed

- Metrics for items configured within the `resources` section are now namespaced under their identifier.

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

- New `http` processor, where payloads can be sent to arbitrary HTTP endpoints and the result constructed into a new payload.
- New `inproc` inputs and outputs for linking streams together.

## 0.15.3 - 2018-07-03

### Added

- New streams endpoint `/streams/{id}/stats` for obtaining JSON metrics for a stream.

### Changed

- Allow comma separated topics for `kafka_balanced`.

## 0.15.0 - 2018-06-28

### Added

- Support for PATCH verb on the streams mode `/streams/{id}` endpoint.

### Changed

- Sweeping changes were made to the environment variable configuration file. This file is now auto generated along with its supporting document. This change will impact the docker image.

## 0.14.7 - 2018-06-24

### Added

- New `filter_parts` processor for filtering individual parts of a message batch.
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

- More granular config options in the `http_client` output for controlling retry logic.
- New `try` pattern for the output `broker` type, which can be used in order to configure fallback outputs.
- New `json` processor, this replaces `delete_json`, `select_json`, `set_json`.

### Changed

- The `streams` API endpoints have been changed to become more "RESTy".
- Removed the `delete_json`, `select_json` and `set_json` processors, please use the `json` processor instead.

## 0.13.5 - 2018-06-10

### Added

- New `grok` processor for creating structured objects from unstructured data.

## 0.13.4 - 2018-06-08

### Added

- New `files` input type for reading multiple files as discrete messages.

### Changed

- Increase default `max_buffer` for `stdin`, `file` and `http_client` inputs.
- Command flags `--print-yaml` and `--print-json` changed to provide sanitised outputs unless accompanied by new `--all` flag.

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

- Back to using Scratch as base for Docker image, instead taking ca-certificates from the build image.

## 0.13.0 - 2018-06-02

### Added

- New `batch` processor for combining payloads up to a number of bytes.
- New `conditional` processor, allows you to configure a chain of processors to only be run if the payload passes a `condition`.
- New `--stream` mode features:
  + POST verb for `/streams` path now supported.
  + New `--streams-dir` flag for parsing a directory of stream configs.

### Changed

- The `condition` processor has been renamed `filter`.
- The `custom_delimiter` fields in any line reader types `file`, `stdin`, `stdout`, etc have been renamed `delimiter`, where the behaviour is the same.
- Now using Alpine as base for Docker image, includes ca-certificates.
