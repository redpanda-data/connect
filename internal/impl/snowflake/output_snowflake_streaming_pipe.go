// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	neturl "net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	v2 "github.com/redpanda-data/connect/v4/internal/impl/snowflake/streamingv2"
	"github.com/redpanda-data/connect/v4/internal/license"
	"github.com/redpanda-data/connect/v4/internal/pool"
)

const (
	ssopFieldAccount     = "account"
	ssopFieldURL         = "url"
	ssopFieldAccountHost = "account_host"
	ssopFieldUser        = "user"
	ssopFieldRole        = "role"
	ssopFieldDB          = "database"
	ssopFieldSchema      = "schema"
	ssopFieldPipe        = "pipe"
	ssopFieldKey         = "private_key"
	ssopFieldKeyFile     = "private_key_file"
	ssopFieldKeyPass     = "private_key_pass"
	ssopFieldBatching    = "batching"
	ssopFieldChannelName = "channel_name"
	ssopFieldOffsetToken = "offset_token"
	// commit_timeout replaces snowflake_streaming's commit_backoff object:
	// WaitUntilCommitted takes a single deadline, not a backoff schedule.
	ssopFieldCommitTimeout = "commit_timeout"
	// tolerate_row_errors mirrors the Kafka Connector's errors.tolerance;
	// false (fail the batch) matches its default.
	ssopFieldTolerateRowErrors           = "tolerate_row_errors"
	ssopFieldCompression                 = "compression"
	ssopFieldCommitPollInterval          = "commit_poll_interval"
	ssopFieldRequestTimeout              = "request_timeout"
	ssopFieldChannelOpenRetry            = "channel_open_retry"
	ssopFieldChannelOpenRetryMaxAttempts = "max_attempts"
	ssopFieldChannelOpenRetryInitial     = "initial_interval"
	ssopFieldChannelOpenRetryMax         = "max_interval"
)

// defaultChannelName is the channel used when channel_name is unset: one
// constant channel per pipe, which works for any input shape. Per-partition
// parallelism is opt-in via channel_name.
func defaultChannelName(db, schema, pipe string) string {
	return fmt.Sprintf("%s.%s.%s", db, schema, pipe)
}

func snowpipeStreamingPipeOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Beta().
		Version("4.115.0").
		Summary("Ingest data into Snowflake using Snowpipe Streaming, writing through a Snowflake pipe.").
		Description(`
Ingest data into Snowflake using Snowpipe Streaming, writing through a Snowflake pipe rather than directly into a table.

Unlike `+"`snowflake_streaming`"+`, which opens channels on a table, this output opens them on a pipe, so any transformation defined in the pipe's `+"`COPY INTO`"+` (stream-static join enrichment, `+"`WHERE`"+` filtering) runs before rows land. If you don't need those, `+"`snowflake_streaming`"+` is the simpler choice: it creates its table for you and needs no pipe.

ifndef::env-cloud[]
This is an Enterprise feature and requires a valid Redpanda Enterprise Edition license that includes Connect. See xref:get-started:licensing.adoc[Redpanda Connect licensing].
endif::[]

The `+"`"+ssopFieldPipe+"`"+` (and its table) must already exist; this output never issues `+"`CREATE PIPE`"+` or `+"`CREATE TABLE`"+`. A missing pipe is reported when the output connects, before any message is written. Fields with no matching column are dropped by Snowflake unless the table has `+"`ENABLE_SCHEMA_EVOLUTION = TRUE`"+` and the pipe uses `+"`MATCH_BY_COLUMN_NAME`"+`, in which case new fields become columns.

Moving an existing `+"`snowflake_streaming`"+` pipeline across is not a rename: several fields are removed and `+"`"+ssopFieldCommitTimeout+"`"+` changes meaning. See the xref:guides:migrating-to-snowflake-streaming-pipe.adoc[migration guide].

== Data types

Each message is sent to Snowflake as one JSON row and Snowflake maps its fields to columns server-side; this output performs no type conversion of its own. What each column type accepts is therefore Snowflake's rule, documented with the https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-limitations[high-performance architecture's limitations^]. Verified against a live account:

[%header,format=dsv]
|===
Snowflake column type:JSON value
NUMBER, FLOAT:number, or a string holding one
BOOLEAN:boolean
CHAR, VARCHAR:string
TIMESTAMP_NTZ(9), TIMESTAMP_LTZ, DATE, TIME:RFC 3339 string; nanosecond precision is kept
VARIANT, OBJECT, ARRAY, MAP:any JSON value, including nested objects and arrays
|===

A JSON `+"`null`"+` into a VARIANT column lands as a VARIANT null, not SQL NULL. GEOGRAPHY and GEOMETRY are not supported by Snowpipe Streaming.

Authentication can be configured using a https://docs.snowflake.com/en/user-guide/key-pair-auth[RSA Key Pair^].

== Iceberg tables

Snowflake-managed Iceberg tables are supported as a target: Snowflake writes the Iceberg files server-side, so nothing in this output is table-type-specific, and partitioned tables work through a pipe whose `+"`COPY INTO`"+` reads `+"`FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))`"+`. Externally managed Iceberg catalogs are not supported by Snowpipe Streaming. Caveats seen on live tables:

- `+"`VARCHAR(n)`"+` is not allowed on Iceberg tables; use `+"`VARCHAR`"+`.
- VARIANT columns need an Iceberg v3 table (`+"`ICEBERG_VERSION = 3`"+`).
- With server-side schema evolution, a new field holding a timestamp-like string can put the channel into `+"`ERR_CHANNEL_SCHEMA_EVOLUTION_ICEBERG_TIME_TYPE_SCALE_NOT_SUPPORTED`"+`. This output reports that as a permanent error naming the fix; pre-create timestamp columns with the type you want.
- Each committed batch writes one file per table partition it touched, so a low-volume pipeline into a partitioned table produces many small files. Larger batches (see <<request-size-and-throughput,Request size and throughput>>) and a `+"`"+ssopFieldChannelName+"`"+` per source partition help.

Messages with an empty or whitespace-only body (Kafka/Redpanda tombstones, typically) are not sent: a Snowflake row has no representation for them. They are acknowledged upstream, counted in the `+"`snowflake_empty_rows_skipped`"+` metric and logged at debug level. If `+"`"+ssopFieldOffsetToken+"`"+` is set it is still evaluated for them, so use a metadata-based expression such as `+"`${! @kafka_offset }`"+`, not one derived from the body. To land tombstones as rows, map them with a processor first.

[[choosing-a-channel-name]]
== Choosing a `+"`"+ssopFieldChannelName+"`"+`

By default every message shares one constant channel, `+"`<database>.<schema>.<pipe>`"+`. Set `+"`"+ssopFieldChannelName+"`"+` to split that up, most commonly one channel per Kafka/Redpanda partition.

*The requirement:* a channel must be written by one ordered source with at most one batch in flight. `+"`"+ssopFieldOffsetToken+"`"+` dedup compares each token against the channel's single last-committed token and cannot tell "already committed by me" from "an unrelated writer got ahead of me", so two token sequences sharing a channel lose rows silently.

*For a partitioned Kafka/Redpanda input*, scope the channel per partition:

[source,yaml]
----
channel_name: "<pipe>-p${! @kafka_partition }"
----

This is safe at any `+"`max_in_flight`"+` only if the input dispatches one batch per partition at a time. The `+"`redpanda`"+` input does (unless `+"`unordered.enabled`"+` is set); `+"`kafka_franz`"+` and `+"`kafka`"+` do not by default (`+"`checkpoint_limit`"+` is 1024), so set `+"`checkpoint_limit: 1`"+` there or `+"`max_in_flight: 1`"+` here. If the input consumes several topics, include `+"`${! @kafka_topic }`"+` in the name too. Never derive the name from the host: after a rebalance the new owner would start a channel with no committed history and re-send everything.

*For a single ordered stream* (Postgres logical replication, a single-partition topic): the default channel with `+"`max_in_flight: 1`"+` is the simple choice. It is *not* safe with `+"`"+ssopFieldOffsetToken+"`"+` for a partitioned source, even at `+"`max_in_flight: 1`"+`: each partition is its own token sequence, so whichever is numerically ahead makes the others' rows look like duplicates. The linter rejects the common form of this (`+"`@kafka_offset`"+` with no `+"`@kafka_partition`"+` in the channel name); for anything else, scope per partition or leave `+"`"+ssopFieldOffsetToken+"`"+` unset.

*If the requirement is violated anyway*, a batch whose tokens start below the lowest token this process has already handled on the channel fails with an error naming the channel and tokens, and `+"`snowflake_out_of_order_submissions`"+` is incremented; the error recurs on every retry until `+"`"+ssopFieldChannelName+"`"+` or `+"`max_in_flight`"+` is fixed. A redelivery from a source that keeps one batch in flight per channel never trips it: such a source never goes back below where it started, however its batches are cut. The detection is one-sided and process-local: it catches a second source whose tokens lie below this process's, but not one whose tokens lie above or between them, nor other replicas, nor anything after a restart; in those cases the rows would be dropped as duplicates with no error. Recover them from your dead-letter queue or the source; redelivery alone cannot re-insert them.

NOTE: There is a limit of 10,000 streams per table -- if using more than 10k streams please reach out to Snowflake support.
`+service.OutputPerformanceDocs(true, true)+`

[[request-size-and-throughput]]
== Request size and throughput

Each batch is one request per channel. Request bodies are zstd-compressed by default (`+"`"+ssopFieldCompression+"`"+`), and Snowflake's 4 MB per-request limit applies to the body as sent, after compression. A batch whose body is still over the limit is sent as several requests, split by message, each with its own offset-token range, so a large batch never fails for size; only a single message over the limit on its own cannot be sent (see `+"`"+ssopFieldTolerateRowErrors+"`"+`). Typical JSON compresses 5–8×, so 16–20 MB of messages usually fits in one request, and anything under about 3.5 MB always does. Memory per in-flight batch is roughly twice the uncompressed messages (the batch and the encoded body) plus the compressed body. Watch `+"`snowflake_compressed_output_size_bytes`"+` against the message bytes to see your ratio. Without `+"`"+ssopFieldOffsetToken+"`"+`, a batch sent as several requests can be duplicated if a later request fails after an earlier one committed: the whole batch is redelivered and the committed requests are sent again, so for large at-least-once batches prefer sizes that fit one request.

*With the `+"`redpanda`"+` input, size batches at the input*, through `+"`max_yield_batch_bytes`"+` (default `+"`32KB`"+`, far below what one request carries), rather than with an output `+"`batching`"+` policy: an output policy pools every partition's messages into one request, which serialises the partitions.

*A channel carries one batch at a time.* The next batch for a channel waits until the previous one has committed, so per-channel throughput is bounded by request size over commit latency (typically a few seconds in-region). Spread load across channels with a per-partition `+"`"+ssopFieldChannelName+"`"+`. Pipelining appends within a channel is planned.
`).
		Fields(
			service.NewStringField(ssopFieldAccount).
				Description(`The Snowflake https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier[Account name^]. Which should be formatted as `+"`<orgname>-<account_name>`"+` where `+"`<orgname>`"+` is the name of your Snowflake organization and `+"`<account_name>`"+` is the unique name of your account within your organization.
`).
				ShortDescription("The Snowflake account name, formatted as orgname-account_name.").Example("ORG-ACCOUNT"),
			service.NewStringField(ssopFieldURL).
				Description(`Override the URL for both the control-plane and ingest-plane connections (default `+"`https://ORG-ACCOUNT.snowflakecomputing.com`"+`). This skips ingest-hostname discovery, so it is for local and test deployments only. For PrivateLink or other non-standard hostnames use `+"`"+ssopFieldAccountHost+"`"+` instead. If both are set, this field wins.
`).Optional().Example("http://localhost:8080").Advanced(),
			service.NewStringField(ssopFieldAccountHost).
				Description(`Override the control-plane host (default `+"`<account>.snowflakecomputing.com`"+`) while keeping ingest-hostname discovery. Use this for https://docs.snowflake.com/en/user-guide/admin-security-privatelink[PrivateLink^] accounts. Ignored when `+"`"+ssopFieldURL+"`"+` is set.
`).Optional().Example("my-account.privatelink.snowflakecomputing.com").Advanced(),
			service.NewStringField(ssopFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user.").
				ShortDescription("The user to run the Snowpipe Stream as."),
			service.NewStringField(ssopFieldRole).Description("The role to ingest as. Every streaming call is authorized against a session token scoped to this role (not the user's default role), so it needs the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] on the pipe and table.").
				ShortDescription("The role to ingest as; sent as a session-scoped token, so it does constrain ingestion privileges.").Example("SNOWPIPE_STREAMING_ROLE"),
			service.NewStringField(ssopFieldDB).Description("The Snowflake database to ingest data into.").Example("MY_DATABASE"),
			service.NewStringField(ssopFieldSchema).Description("The Snowflake schema to ingest data into.").Example("PUBLIC"),
			service.NewStringField(ssopFieldPipe).Description("The Snowflake pipe to ingest data into. It must already exist; a missing pipe is reported when the output connects. Unlike `database` and `schema` this value is not upper-cased, so match the stored identifier exactly (`MY_PIPE` for a pipe created as `CREATE PIPE my_pipe`). Snowflake's managed default pipes (`<TABLE>-STREAMING`) only exist once a channel has been opened on the table through the table endpoint, which this output does not use, so a default pipe fails the connect-time check with `ERR_PIPE_DOES_NOT_EXIST_OR_NOT_AUTHORIZED` until something else has created it; create a named pipe instead.").Example("MY_PIPE"),
			service.NewStringField(ssopFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").
				ShortDescription("PEM encoded private RSA key for authenticating with Snowflake. Either this or private_key_file is required.").Optional().Secret(),
			service.NewStringField(ssopFieldKeyFile).Description("The file to load the private RSA key from. This should be a `.p8` PEM encoded file. Either this or `private_key` must be specified.").
				ShortDescription("File to load the private RSA key from, as a .p8 PEM file. Either this or private_key is required.").Optional(),
			service.NewStringField(ssopFieldKeyPass).Description("The RSA key passphrase if the RSA key is encrypted.").Optional().Secret(),
			service.NewBatchPolicyField(ssopFieldBatching),
			service.NewStringEnumField(ssopFieldCompression, "zstd", "none").
				Description(`How to compress each request body. Snowflake's 4 MB per-request limit applies to the payload as sent, after compression, so `+"`zstd`"+` (Snowflake's recommendation) lets a request carry several times more rows than `+"`none`"+`. A batch whose body is still over the limit is sent as several requests, split by message; a single message whose body is over the limit on its own is treated like a row Snowflake rejected (see `+"`"+ssopFieldTolerateRowErrors+"`"+`). The bytes sent are counted in `+"`snowflake_compressed_output_size_bytes`"+`.`).
				ShortDescription("Request body compression: zstd (default) or none. The 4 MB request limit applies after compression.").
				Default("zstd").
				Advanced(),
			service.NewOutputMaxInFlightField().Default(4),
			service.NewInterpolatedStringField(ssopFieldChannelName).
				Description(`The channel to write through. Resolved per message: a batch is grouped by channel name (preserving order within each group) and each group is appended, deduplicated when `+"`"+ssopFieldOffsetToken+"`"+` is set, and committed separately, so batch at the input so that a batch spans as few channels as possible. Two instances writing the same channel name conflict.

Defaults to a single constant channel, `+"`<database>.<schema>.<pipe>`"+`. See <<choosing-a-channel-name,Choosing a `+"`"+ssopFieldChannelName+"`"+`>> for when that is safe and the per-partition pattern for Kafka/Redpanda inputs.`).
				Optional().
				Advanced().
				Examples(`MYDB.PUBLIC.BETS_PIPE-p${!@kafka_partition}`),
			service.NewInterpolatedStringField(ssopFieldOffsetToken).
				Description(`The offset token used for exactly-once delivery. Each message's token is compared to the channel's last committed token (numerically when both are integers, otherwise byte-wise as strings); a message whose token is not newer is dropped as a duplicate. Delivery to the output must therefore be ordered, and messages within a batch must be in increasing token order; a retried batch may be dropped as duplicate if later batches succeeded meanwhile, so use a dead-letter queue for failures.

Non-integer tokens *must be fixed-width* for string order to be meaningful (`+"`0/F`"+` sorts after `+"`0/10`"+`). `+"`postgres_cdc`"+`'s `+"`@lsn`"+` already is; zero-pad anything you assemble yourself.

If unset, nothing is deduplicated and delivery is at-least-once; a retried batch spanning several channels re-appends on every channel that already landed.

*Don't add, remove, or change this expression on a pipeline that has already written to its channels.* A channel's tokens must stay one comparable sequence. A channel written without this field carries synthetic tokens (`+"`~rpcn-at-least-once-<digits>`"+`, visible as `+"`last_committed_offset_token`"+`) that compare newer than any real token, and a changed expression leaves new tokens judged against the old shape; this output refuses such batches where it can detect the mismatch (synthetic vs real, integer vs non-integer) and keeps refusing until you give the pipeline a fresh `+"`"+ssopFieldChannelName+"`"+`. Two different non-integer shapes cannot be told apart.

A duplicate (never a loss) is possible if an append is accepted but its commit is not confirmed within `+"`"+ssopFieldCommitTimeout+"`"+` and the redelivered copy is appended before the original commits; raise `+"`"+ssopFieldCommitTimeout+"`"+` (0 = no limit) to close that window.

For more information about offset tokens, see https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#offset-tokens[^Snowflake Documentation]`).
				ShortDescription("The offset token used for exactly-once delivery, compared against the latest token for a channel. If unset, this output doesn't deduplicate at all (at-least-once).").
				Optional().
				Advanced().
				Examples(`${! @kafka_offset }`, `postgres-${!@lsn}`),
			service.NewDurationField(ssopFieldCommitTimeout).
				Description("The maximum total time to wait for data to be committed to Snowflake before returning an error. If zero then no limit is used.").
				ShortDescription("The maximum time to wait for data to be committed before returning an error.").
				Default("60s").
				Advanced(),
			service.NewDurationField(ssopFieldCommitPollInterval).
				Description("How often to poll channel status while waiting for a commit (see `"+ssopFieldCommitTimeout+"`). Under backpressure from the status endpoint the interval doubles from this value up to 4s.").
				ShortDescription("How often to poll channel status while waiting for a commit.").
				Default(v2.DefaultCommitPollInterval.String()).
				Advanced(),
			service.NewDurationField(ssopFieldRequestTimeout).
				Description("The timeout for a single HTTP request to Snowflake. A batch's total wait is governed by `"+ssopFieldCommitTimeout+"`.").
				ShortDescription("Timeout for a single HTTP request to Snowflake.").
				Default(v2.DefaultRequestTimeout.String()).
				Advanced(),
			service.NewObjectField(ssopFieldChannelOpenRetry,
				service.NewIntField(ssopFieldChannelOpenRetryMaxAttempts).
					Description("Total attempts (including the first) before failing the batch. Only HTTP 429 and the 409 open-in-progress collision are retried.").
					Default(v2.DefaultOpenRetryMaxAttempts),
				service.NewDurationField(ssopFieldChannelOpenRetryInitial).
					Description("Wait before the first retry; each further retry doubles it.").
					Default(v2.DefaultOpenRetryInitialDelay.String()),
				service.NewDurationField(ssopFieldChannelOpenRetryMax).
					Description("Cap on the wait between retries.").
					Default(v2.DefaultOpenRetryMaxDelay.String()),
			).
				Description("Retry schedule for opening a channel and for the pipe pre-flight at connect time. The defaults allow about 22s of backoff. A channel open blocks this output's other channels, so keep the budget modest.").
				Advanced(),
			service.NewBoolField(ssopFieldTolerateRowErrors).
				Description(`Whether to tolerate rows Snowflake rejects after a batch is appended (malformed or uncastable values, reported asynchronously in the channel status). `+"`false`"+` (the default, matching the Kafka Connector's `+"`errors.tolerance=none`"+`) fails the batch with an error naming the channel and Snowflake's last error; `+"`true`"+` only logs a warning. Either way `+"`snowflake_rows_error_count`"+` is incremented and this output does not retry the rejected rows: Snowflake has already refused them and the redelivery dedupes to nothing, so failing the batch exists to alert an operator. To keep the rejected rows themselves, turn on error logging on the target table and Snowflake captures them in its error table; `+"`SNOWPIPE_STREAMING_CHANNEL_HISTORY`"+` has the error history with offsets (see https://docs.snowflake.com/en/user-guide/snowpipe-streaming/snowpipe-streaming-high-performance-error-handling[error handling^]). When `+"`"+ssopFieldOffsetToken+"`"+` is unset such a rejection never fails the batch even with `+"`false`"+`, since without dedup a retry would only re-insert the good rows as duplicates.

The same policy applies to a message this output cannot send at all: one that spans several lines but is not valid JSON (multi-line bodies are compacted to one NDJSON row each, so they must parse; a body holding several newline-separated documents becomes that many rows), or one whose body alone is over the 4 MB request limit after compression. Such a message is dropped rather than sent, and treated like a row Snowflake rejected: the batch's good rows land, `+"`snowflake_rows_error_count`"+` is incremented, and with `+"`"+ssopFieldOffsetToken+"`"+` set and `+"`false`"+` the batch fails once with the message number and parse error, after which its redelivery dedupes to nothing.`).
				ShortDescription("Whether to tolerate rows Snowflake rejects (equivalent to the Kafka Connector's `errors.tolerance`); false (default) fails the batch, true only warns.").
				Default(false).
				Advanced(),
		).
		// ConfigSpec.LintRule replaces rather than composes, so every rule
		// lives in this one match block. Rules 3 and 4 are config-time
		// backstops for the silent-data-loss shapes described in
		// channel_name's docs: a per-partition token on a shared channel, and
		// offset_token on the default channel with max_in_flight > 1 (unset
		// means the default of 4). The substring tests are crude by design.
		LintRule(`root = match {
  this.exists("private_key") && this.exists("private_key_file") => [ "both `+"`private_key`"+` and `+"`private_key_file`"+` can't be set simultaneously" ],
  !this.exists("private_key") && !this.exists("private_key_file") => [ "exactly one of `+"`private_key`"+` or `+"`private_key_file`"+` must be set" ],
  this.exists("offset_token") && this.offset_token.string().contains("kafka_offset") && !(this.exists("channel_name") && this.channel_name.string().contains("kafka_partition")) => [ "`+"`offset_token`"+` uses the per-partition `+"`@kafka_offset`"+` but `+"`channel_name`"+` does not scope the channel per partition (e.g. `+"`<pipe>-p${! @kafka_partition }`"+`): every partition's offsets would be compared against one shared committed token and rows would be silently dropped as duplicates. Include `+"`@kafka_partition`"+` in `+"`channel_name`"+`, or remove `+"`offset_token`"+` for at-least-once delivery" ],
  this.exists("offset_token") && !this.exists("channel_name") && (!this.exists("max_in_flight") || this.max_in_flight > 1) => [ "`+"`offset_token`"+` is set with the default single shared channel (no `+"`channel_name`"+`), so `+"`max_in_flight`"+` must be 1 (its default is 4): more than one batch in flight to one channel can arrive out of offset order, which exactly-once dedup then treats as duplicates. Set `+"`max_in_flight: 1`"+`, or give `+"`channel_name`"+` a per-partition value" ],
}`).
		Example(
			"Simple at-least-once ingestion",
			`The minimal config: no `+"`offset_token`"+` or `+"`channel_name`"+`, so rows land at-least-once through a single channel named after the pipe, whatever the input. To move to exactly-once later, add `+"`offset_token`"+` together with a *new* `+"`channel_name`"+` (see `+"`offset_token`"+`'s docs), scoped per partition if the input is partitioned.`,
			`
input:
  generate:
    mapping: 'root = { "id": uuid_v4(), "amount": random_int(min: 1, max: 1000) }'
    interval: 1s
output:
  snowflake_streaming_pipe:
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: SNOWPIPE_STREAMING_ROLE
    database: "MYDATABASE"
    schema: "PUBLIC"
    pipe: "MY_PIPE"
    private_key_file: "my/private/key.p8"
`).
		Example(
			"Exactly once CDC into Snowflake",
			`How to send data from a PostgreSQL table into Snowflake exactly once using Postgres Logical Replication.

NOTE: A single ordered stream feeding the default channel, so `+"`max_in_flight: 1`"+` is required (see `+"`"+ssopFieldChannelName+"`"+`'s docs). To run at-least-once instead, remove `+"`"+ssopFieldOffsetToken+"`"+` (on a fresh `+"`"+ssopFieldChannelName+"`"+` if the pipeline has already run).`,
			`
input:
  postgres_cdc:
    dsn: postgres://foouser:foopass@localhost:5432/foodb
    schema: "public"
    slot_name: "my_repl_slot"
    tables: ["my_pg_table"]
    # Large batches perform best. A batch over the 4 MB compressed request
    # limit is sent as several requests, so count only needs to fit memory.
    batching:
      count: 50000
      period: 45s
    # One batch in flight at a time, so a retried batch never trails a newer
    # one into the channel.
    checkpoint_limit: 1
output:
  snowflake_streaming_pipe:
    # Postgres LSNs are fixed-width, so they order correctly as tokens.
    # Snapshot rows (stream_snapshot: true) carry no LSN; snapshot through a
    # separate at-least-once pipeline first.
    offset_token: "${!@lsn}"
    # Required with the default single channel; see channel_name's docs.
    max_in_flight: 1
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: SNOWPIPE_STREAMING_ROLE
    database: "MYDATABASE"
    schema: "PUBLIC"
    pipe: "MY_PG_PIPE"
    private_key_file: "my/private/key.p8"
`).
		Example(
			"Ingesting data exactly once from Redpanda",
			`How to ingest data from Redpanda with consumer groups, decode the schema using the schema registry, then write the corresponding data into Snowflake exactly once through a pipe.

NOTE: This is the pattern for any partitioned Kafka/Redpanda input: `+"`"+ssopFieldChannelName+"`"+` is scoped per partition, which is what makes exactly-once hold (see that field's docs). To run at-least-once instead, remove `+"`"+ssopFieldOffsetToken+"`"+` (on a fresh `+"`"+ssopFieldChannelName+"`"+` if the pipeline has already run).`,
			`
input:
  redpanda:
    topics: ["my_topic_going_to_snow"]
    consumer_group: "redpanda_connect_to_snowflake"
    # We want very large batches - each batch will be sent to Snowflake individually
    # so to optimize query performance we want as big of files as we have memory for
    fetch_max_bytes: 100MiB
    fetch_min_bytes: 50MiB
    partition_buffer_bytes: 100MiB
pipeline:
  processors:
    - schema_registry_decode:
        url: "redpanda.example.com:8081"
        basic_auth:
          enabled: true
          username: MY_USER_NAME
          password: "${REDPANDA_SCHEMA_REGISTRY_PASSWORD}"
output:
  fallback:
    - snowflake_streaming_pipe:
        # One channel per partition, so partitions never share a channel;
        # the redpanda input keeps one batch per partition in flight, which
        # makes this safe at any max_in_flight.
        channel_name: "MYDB.PUBLIC.BETS_PIPE-p${!@kafka_partition}"
        # No batching policy here: batch size is governed by the input's
        # max_yield_batch_bytes. An output policy would pool partitions into
        # one request and serialise them.
        offset_token: ${! @kafka_offset }
        account: "MYSNOW-ACCOUNT"
        user: MYUSER
        role: SNOWPIPE_STREAMING_ROLE
        database: "MYDATABASE"
        schema: "PUBLIC"
        pipe: "MYPIPE"
        private_key_file: "my/private/key.p8"
    # In order to prevent delivery orders from messing with the order of delivered records
    # it's important that failures are immediately sent to a dead letter queue and not retried
    # to Snowflake. See the ordering documentation for the "redpanda" input for more details.
    - retry:
        output:
          redpanda:
            topic: "dead_letter_queue"
`,
		).
		Example(
			"HTTP Server to push data to Snowflake",
			`This example demonstrates how to create an HTTP server input that can receive HTTP PUT requests
with JSON payloads, that are buffered locally then written to Snowflake in batches.

NOTE: This example uses a buffer to respond to the HTTP request immediately, so it's possible that failures to deliver data could result in data loss.
See the documentation about xref:components:buffers/memory.adoc[buffers] for more information, or remove the buffer entirely to respond to the HTTP request only once the data is written to Snowflake.`,
			`
input:
  http_server:
    path: /snowflake
buffer:
  memory:
    # Max inflight data before applying backpressure
    limit: 524288000 # 50MiB
    # Each flush is one batch. A batch over the 4 MB compressed request
    # limit is split into several requests, so this is a latency and
    # memory choice, not a hard cap.
    batch_policy:
      enabled: true
      byte_size: 2097152 # 2MiB
      period: "10s"
output:
  snowflake_streaming_pipe:
    account: "MYSNOW-ACCOUNT"
    user: MYUSER
    role: SNOWPIPE_STREAMING_ROLE
    database: "MYDATABASE"
    schema: "PUBLIC"
    pipe: "MYPIPE"
    private_key_file: "my/private/key.p8"
    # By default there is only a single channel per output pipe allowed
    # if we want to have multiple Redpanda Connect streams writing data
    # then we need a unique channel name per stream. We'll use the host
    # name to get a unique name in this example.
    channel_name: "snowflake-channel-for-${HOST}"
`,
		)
}

func init() {
	service.MustRegisterBatchOutput(
		"snowflake_streaming_pipe",
		snowpipeStreamingPipeOutputConfig(),
		newSnowpipeStreamingPipeOutput)
}

func newSnowpipeStreamingPipeOutput(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (
	output service.BatchOutput,
	batchPolicy service.BatchPolicy,
	maxInFlight int,
	err error,
) {
	if err = license.CheckRunningEnterprise(mgr); err != nil {
		return
	}
	if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
		return
	}
	if batchPolicy, err = conf.FieldBatchPolicy(ssopFieldBatching); err != nil {
		return
	}
	output, err = newSnowflakeStreamerPipe(conf, mgr)
	return
}

func newSnowflakeStreamerPipe(
	conf *service.ParsedConfig,
	mgr *service.Resources,
) (service.BatchOutput, error) {
	keypass := ""
	if conf.Contains(ssopFieldKeyPass) {
		pass, err := conf.FieldString(ssopFieldKeyPass)
		if err != nil {
			return nil, err
		}
		keypass = pass
	}
	var rsaKey *rsa.PrivateKey
	if conf.Contains(ssopFieldKey) {
		key, err := conf.FieldString(ssopFieldKey)
		if err != nil {
			return nil, err
		}
		rsaKey, err = getPrivateKey([]byte(key), keypass)
		if err != nil {
			return nil, err
		}
	} else if conf.Contains(ssopFieldKeyFile) {
		keyFile, err := conf.FieldString(ssopFieldKeyFile)
		if err != nil {
			return nil, err
		}
		rsaKey, err = getPrivateKeyFromFile(mgr.FS(), keyFile, keypass)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, fmt.Errorf("one of `%s` or `%s` is required", ssopFieldKey, ssopFieldKeyFile)
	}
	account, err := conf.FieldString(ssopFieldAccount)
	if err != nil {
		return nil, err
	}
	// BaseURL (url) overrides both planes and skips discovery; AccountHost
	// (account_host) overrides only the control plane. The client prefers
	// BaseURL when both are set.
	var accountHost, baseURL string
	if conf.Contains(ssopFieldAccountHost) {
		accountHost, err = conf.FieldString(ssopFieldAccountHost)
		if err != nil {
			return nil, err
		}
	} else {
		accountHost = account + ".snowflakecomputing.com"
	}
	if conf.Contains(ssopFieldURL) {
		baseURL, err = conf.FieldString(ssopFieldURL)
		if err != nil {
			return nil, err
		}
		if _, err := neturl.Parse(baseURL); err != nil {
			return nil, fmt.Errorf("invalid url: %w", err)
		}
	}
	user, err := conf.FieldString(ssopFieldUser)
	if err != nil {
		return nil, err
	}
	role, err := conf.FieldString(ssopFieldRole)
	if err != nil {
		return nil, err
	}
	db, err := conf.FieldString(ssopFieldDB)
	if err != nil {
		return nil, err
	}
	schema, err := conf.FieldString(ssopFieldSchema)
	if err != nil {
		return nil, err
	}
	pipe, err := conf.FieldString(ssopFieldPipe)
	if err != nil {
		return nil, err
	}

	hasChannelName := conf.Contains(ssopFieldChannelName)
	var channelName *service.InterpolatedString
	if hasChannelName {
		channelName, err = conf.FieldInterpolatedString(ssopFieldChannelName)
		if err != nil {
			return nil, err
		}
	}

	var offsetToken *service.InterpolatedString
	if conf.Contains(ssopFieldOffsetToken) {
		offsetToken, err = conf.FieldInterpolatedString(ssopFieldOffsetToken)
		if err != nil {
			return nil, err
		}
	}
	// A nil offsetToken means at-least-once: no dedup pass. Never substitute
	// a synthetic token here; that would silently disable exactly-once.

	commitTimeout, err := conf.FieldDuration(ssopFieldCommitTimeout)
	if err != nil {
		return nil, err
	}

	tolerateRowErrors, err := conf.FieldBool(ssopFieldTolerateRowErrors)
	if err != nil {
		return nil, err
	}
	compression, err := conf.FieldString(ssopFieldCompression)
	if err != nil {
		return nil, err
	}

	// Role, database and schema are case-sensitive in the REST API paths.
	role = strings.ToUpper(role)
	db = strings.ToUpper(db)
	schema = strings.ToUpper(schema)

	if channelName == nil {
		expr := defaultChannelName(db, schema, pipe)
		channelName, err = service.NewInterpolatedString(expr)
		if err != nil {
			return nil, fmt.Errorf("internal error building default %s template: %w", ssopFieldChannelName, err)
		}
	}

	commitPollInterval, err := conf.FieldDuration(ssopFieldCommitPollInterval)
	if err != nil {
		return nil, err
	}
	requestTimeout, err := conf.FieldDuration(ssopFieldRequestTimeout)
	if err != nil {
		return nil, err
	}
	retryConf := conf.Namespace(ssopFieldChannelOpenRetry)
	openRetryMaxAttempts, err := retryConf.FieldInt(ssopFieldChannelOpenRetryMaxAttempts)
	if err != nil {
		return nil, err
	}
	openRetryInitial, err := retryConf.FieldDuration(ssopFieldChannelOpenRetryInitial)
	if err != nil {
		return nil, err
	}
	openRetryMax, err := retryConf.FieldDuration(ssopFieldChannelOpenRetryMax)
	if err != nil {
		return nil, err
	}

	client, err := v2.NewClient(v2.Config{
		Account:               account,
		AccountHost:           accountHost,
		BaseURL:               baseURL,
		User:                  user,
		Role:                  role,
		PrivateKey:            rsaKey,
		Database:              db,
		Schema:                schema,
		Pipe:                  pipe,
		CommitPollInterval:    commitPollInterval,
		Compress:              compression == "zstd",
		RequestTimeout:        requestTimeout,
		OpenRetryMaxAttempts:  openRetryMaxAttempts,
		OpenRetryInitialDelay: openRetryInitial,
		OpenRetryMaxDelay:     openRetryMax,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create streaming client: %w", err)
	}
	// The network handshake runs in Connect (cancellable, retried by the
	// framework), not here.

	indexed := &snowpipeIndexedOutputPipe{
		client:            client,
		db:                db,
		schema:            schema,
		channelName:       channelName,
		offsetToken:       offsetToken,
		commitTimeout:     commitTimeout,
		tolerateRowErrors: tolerateRowErrors,
		compress:          compression == "zstd",
		logger:            mgr.Logger(),
		metrics:           newSnowpipePipeMetrics(mgr.Metrics()),
		openChannelFn: func(ctx context.Context, name string) (v2.IngestChannel, error) {
			return client.OpenChannel(ctx, name)
		},
	}
	indexed.channelPool = pool.NewIndexed(indexed.openChannel)
	return indexed, nil
}

type snowpipeIndexedOutputPipe struct {
	channelPool pool.Indexed[v2.IngestChannel]

	// client is nil when tests construct this struct directly, making
	// Connect a no-op. connected makes repeated Connect calls free.
	client    *v2.Client
	connected atomic.Bool

	openChannelFn func(ctx context.Context, name string) (v2.IngestChannel, error)

	db, schema string
	// offsetToken is nil when offset_token is unset: at-least-once, no dedup.
	offsetToken, channelName *service.InterpolatedString
	commitTimeout            time.Duration
	tolerateRowErrors        bool
	compress                 bool
	logger                   *service.Logger
	metrics                  *snowpipePipeMetrics

	// noDedupCommitToken supplies a per-call commit token when offsetToken
	// is nil, so WaitUntilCommitted can confirm this append rather than an
	// earlier one. Seeded from the wall clock (and bumped past any synthetic
	// token already committed on a channel) so a restarted process never
	// reuses a value at or below the channel's committed token, which would
	// make the commit wait return before the append was confirmed. Rendered
	// via v2.FormatSyntheticToken so it is recognisable as synthetic; the
	// mode-switch guards depend on that.
	noDedupCommitToken atomic.Int64
	syntheticSeedOnce  sync.Once

	// committedFloors is the start token of the first range this process
	// acked per channel; see checkSubmissionOrder.
	committedFloors   map[string]string
	committedFloorsMu sync.Mutex

	// invalidChannels holds channels whose status named a condition an
	// operator must fix (v2.KindChannelOperator). They are not reopened
	// when the failure is seen; the next batch for the channel reopens it
	// once, so the retry cadence is the input's redelivery, not a loop.
	invalidChannels   map[string]struct{}
	invalidChannelsMu sync.Mutex
}

// checkSubmissionOrder refuses a batch whose tokens start below the first
// range this process acked on the channel. One ordered source never goes
// back below where it started, so such a batch comes from a second source
// (or a rewound one), and letting it through would hand its rows to the
// dedup filter, which drops them as duplicates with no error.
//
// Only an acked range sets the floor (recordAckedRange), and a redelivery
// of a failed or re-cut batch starts at or above it, so replays are never
// refused and dedupe normally. The check is one-sided: a second source
// whose tokens lie above or between this process's is not caught, and the
// floor only sees this process's own history.
func (o *snowpipeIndexedOutputPipe) checkSubmissionOrder(channelName, start, end string) error {
	o.committedFloorsMu.Lock()
	defer o.committedFloorsMu.Unlock()
	if floor, ok := o.committedFloors[channelName]; ok && v2.CompareOffsetTokens(start, floor) < 0 {
		o.metrics.ReportOutOfOrderSubmission()
		return fmt.Errorf(
			"channel %q received offset tokens %s..%s, which start below %s, the lowest token this process has already "+
				"handled on it -- writes to this channel are out of order, so another source is writing to it, which breaks offset_token's exactly-once "+
				"guarantee (see the channel_name field's docs: a channel must be written to by only one ordered source, "+
				"with at most one batch in flight to it at a time). This will not resolve itself on retry: fix "+
				"channel_name (give each independent source its own channel) or max_in_flight (cap it at 1 for a "+
				"shared channel) rather than relying on redelivery",
			channelName, start, end, floor)
	}
	return nil
}

// recordAckedRange sets the channel's floor to start the first time a range
// is acked: committed, deduped to nothing, or all empty. Later acked ranges
// start at or above it (checkSubmissionOrder refused anything else), so the
// floor never moves. Only exactly-once mode tracks real tokens.
func (o *snowpipeIndexedOutputPipe) recordAckedRange(channelName, start string) {
	if o.offsetToken == nil {
		return
	}
	o.committedFloorsMu.Lock()
	defer o.committedFloorsMu.Unlock()
	if o.committedFloors == nil {
		o.committedFloors = map[string]string{}
	}
	if _, ok := o.committedFloors[channelName]; !ok {
		o.committedFloors[channelName] = start
	}
}

// seedSyntheticTokens seeds noDedupCommitToken from the wall clock on first
// use, so directly constructed outputs behave like config-built ones.
func (o *snowpipeIndexedOutputPipe) seedSyntheticTokens() {
	o.syntheticSeedOnce.Do(func() {
		o.noDedupCommitToken.Store(time.Now().UnixNano())
	})
}

// nextSyntheticCommitToken mints the per-call commit token used when
// offset_token is unset. See noDedupCommitToken.
func (o *snowpipeIndexedOutputPipe) nextSyntheticCommitToken() string {
	o.seedSyntheticTokens()
	return v2.FormatSyntheticToken(o.noDedupCommitToken.Add(1))
}

// syncSyntheticTokens runs at every channel open in at-least-once mode. It
// bumps the counter past any synthetic token already committed on the
// channel, and refuses a channel whose committed real token would compare
// at or above our synthetic ones (every commit wait would then succeed
// before its append was confirmed). It runs on every open, not once per
// name: a channel can be reopened after another process (a consumer-group
// member that took the partition for a while) committed higher synthetic
// tokens on it, and a check cached from before that would let a commit
// wait return before its own append committed. It reads the token from
// the open response, so it adds no request and nothing can fail between
// the open and the sync.
func (o *snowpipeIndexedOutputPipe) syncSyntheticTokens(channel v2.IngestChannel) error {
	// The open response carries the committed token, so this costs no
	// request and cannot fail after a successful open.
	latest := channel.CommittedAtOpen()
	o.seedSyntheticTokens()
	if prev, ok := v2.ParseSyntheticToken(latest); ok {
		// CAS to the max: another channel's check may race with a
		// different value.
		for {
			cur := o.noDedupCommitToken.Load()
			if cur >= prev || o.noDedupCommitToken.CompareAndSwap(cur, prev) {
				break
			}
		}
	} else if latest != "" {
		// Peek at the next token; the one minted for this batch is greater.
		next := v2.FormatSyntheticToken(o.noDedupCommitToken.Add(0) + 1)
		if v2.CompareOffsetTokens(latest, next) >= 0 {
			return fmt.Errorf(
				"channel %q was last written with %s set (its last committed offset token is %q), and that token "+
					"compares at or above the synthetic tokens this output uses when %s is unset -- every commit wait on "+
					"this channel would succeed before its own append was confirmed. This will not resolve itself on "+
					"retry: give this pipeline a fresh %s so it starts from an empty channel",
				channel.Name(), ssopFieldOffsetToken, latest, ssopFieldOffsetToken, ssopFieldChannelName)
		}
	}
	return nil
}

// openChannel opens (or reopens) a channel and, in at-least-once mode,
// re-syncs the synthetic token counter against what the channel has
// committed. Every open path goes through here: the pool's first open,
// reopenAfterFailure, and the one-shot reopen of an invalidated channel.
func (o *snowpipeIndexedOutputPipe) openChannel(ctx context.Context, name string) (v2.IngestChannel, error) {
	o.logger.Debugf("opening snowflake streaming channel for `%s.%s`: %s", o.db, o.schema, name)
	channel, err := o.openChannelFn(ctx, name)
	if err != nil {
		return nil, err
	}
	if o.offsetToken == nil {
		if err := o.syncSyntheticTokens(channel); err != nil {
			return nil, err
		}
	}
	return channel, nil
}

// logReopenFailure warns that a reopen failed, calling out the
// uncommitted-data conflict specifically since it means the previous
// append may still commit.
func (o *snowpipeIndexedOutputPipe) logReopenFailure(name string, reopenErr error) {
	if v2.IsUncommittedDataConflict(reopenErr) {
		o.logger.Warnf("unable to reopen channel %q: it still has uncommitted data from before the failure, so Snowflake refused the reopen rather than risk racing the pending commit: %v", name, reopenErr)
		return
	}
	o.logger.Warnf("unable to reopen channel %q after failure: %v", name, reopenErr)
}

// reportRejectedBeforeReopen counts and logs rows the channel rejected that
// have not been reported yet. A reopen resets the channel's error baseline,
// so this must run before one.
func (o *snowpipeIndexedOutputPipe) reportRejectedBeforeReopen(channel v2.IngestChannel) {
	if rejected := channel.RowsRejected(); rejected > 0 {
		o.metrics.ReportRowsRejected(rejected)
		o.logger.Warnf("channel %q has %d new row error(s) (total: %d) recorded before this batch failed. Last error message: %s%s",
			channel.Name(), rejected, channel.RowsErrorCount(), channel.LastErrorMessage(), errorBoundNote(channel))
	}
}

// reopenAfterFailure replaces a channel in the pool after an append or
// commit-wait error, keeping the old one if the reopen fails. Rejected rows
// are reported first: a reopen resets the channel's error baseline, and on
// redelivery an already-committed batch dedupes to nothing before the
// post-commit check, so this is the only place they would be counted.
func (o *snowpipeIndexedOutputPipe) reopenAfterFailure(ctx context.Context, channel v2.IngestChannel) {
	o.reportRejectedBeforeReopen(channel)
	reopened, reopenErr := o.openChannel(ctx, channel.Name())
	if reopenErr == nil {
		o.channelPool.Release(channel.Name(), reopened)
		return
	}
	o.logReopenFailure(channel.Name(), reopenErr)
	o.channelPool.Release(channel.Name(), channel)
}

// Connect runs the client handshake (hostname discovery, token exchange,
// pipe pre-flight) under the framework's ctx so it is cancellable and
// transient failures are retried. Idempotent after success.
func (o *snowpipeIndexedOutputPipe) Connect(ctx context.Context) error {
	if o.client == nil || o.connected.Load() {
		return nil
	}
	if err := o.client.Connect(ctx); err != nil {
		return fmt.Errorf("unable to connect streaming client: %w", err)
	}
	o.connected.Store(true)
	return nil
}

// channelGroup is one channel's share of a batch, in original relative order.
type channelGroup struct {
	name  string
	batch service.MessageBatch
}

// groupMessagesByChannel buckets a batch by each message's resolved channel
// name, in first-appearance order, preserving relative order within a group
// (dedup assumes increasing token order per channel).
func groupMessagesByChannel(exec *service.MessageBatchInterpolationExecutor, batch service.MessageBatch) ([]channelGroup, error) {
	groupIndex := make(map[string]int, 1)
	groups := make([]channelGroup, 0, 1)
	for i, msg := range batch {
		name, err := exec.TryString(i)
		if err != nil {
			return nil, fmt.Errorf("error executing %s: %w", ssopFieldChannelName, err)
		}
		idx, exists := groupIndex[name]
		if !exists {
			idx = len(groups)
			groupIndex[name] = idx
			groups = append(groups, channelGroup{name: name})
		}
		groups[idx].batch = append(groups[idx].batch, msg)
	}
	return groups, nil
}

// droppedBodies counts the messages a channel group dropped because they
// cannot be sent (a multi-line body that is not JSON, or a single message
// over the request limit), with the first for the log and error text.
type droppedBodies struct {
	count int
	first string
}

func (d *droppedBodies) add(delivered service.MessageBatch, msg *service.Message, err error) {
	if d.count == 0 {
		d.first = fmt.Sprintf("message %d of %d: %v", slices.Index(delivered, msg)+1, len(delivered), err)
	}
	d.count++
}

// prepareRows renders the bodies of one channel group's surviving messages
// as NDJSON rows, one slice per message, aligned with batch. A body with a
// line break inside it (pretty-printed JSON) would be split into several
// NDJSON rows, so v2.SplitRow compacts it; a body holding several
// newline-separated documents becomes that many rows, as before. Only those
// bodies are parsed: a single-line body that is not JSON is left for
// Snowflake to reject, as before, so tolerate_row_errors keeps its meaning.
//
// A multi-line body that is not JSON is dropped and recorded in dropped;
// writeChannelGroup then treats it like a row Snowflake rejected: the good
// rows land, the drop is counted, and with offset_token set and
// tolerate_row_errors false the batch fails once, so the redelivery dedupes
// to nothing. Empty bodies (tombstones) are counted in emptyRows. Both
// leave a nil entry. delivered is the whole delivered batch, for positions
// in log and error text.
func prepareRows(batch, delivered service.MessageBatch, dropped *droppedBodies) (rowsByMsg [][][]byte, emptyRows int, err error) {
	rowsByMsg = make([][][]byte, len(batch))
	for i, msg := range batch {
		body, err := msg.AsBytes()
		if err != nil {
			return nil, 0, fmt.Errorf("unable to serialize message: %w", err)
		}
		split, err := v2.SplitRow(body)
		if err != nil {
			dropped.add(delivered, msg, err)
			continue
		}
		if len(split) == 0 {
			emptyRows++
			continue
		}
		rowsByMsg[i] = split
	}
	return rowsByMsg, emptyRows, nil
}

// sendUnit is a row-producing message's rows together with the range of
// batch positions it covers. A message that produced no rows (empty or
// dropped) is folded into a neighbour's range, so every message's token is
// covered by some request's end token and the redelivery of such a message
// dedupes. Tokens are interpolated only where a request boundary falls.
type sendUnit struct {
	msg      *service.Message
	rows     [][]byte
	from, to int // batch positions covered: [from, to]
}

// buildUnits pairs each row-producing message with the batch positions it
// covers, folding rowless neighbours in.
func buildUnits(batch service.MessageBatch, rowsByMsg [][][]byte) []sendUnit {
	units := make([]sendUnit, 0, len(batch))
	pendingFrom := -1
	for i, msg := range batch {
		if rowsByMsg[i] == nil {
			if len(units) > 0 {
				units[len(units)-1].to = i
			} else if pendingFrom < 0 {
				pendingFrom = i
			}
			continue
		}
		u := sendUnit{msg: msg, rows: rowsByMsg[i], from: i, to: i}
		if len(units) == 0 && pendingFrom >= 0 {
			u.from = pendingFrom
		}
		units = append(units, u)
	}
	return units
}

// appendOutcome summarises appendInParts.
type appendOutcome struct {
	sent         bool
	parts        int
	payloadBytes int
	lastEnd      string // end token of the last request sent
}

// appendInParts appends units[from:to] as one request, splitting in half by
// unit when the body to send is over the request limit (the limit applies
// to the payload as sent, after compression, which is only known after
// encoding). Parts are sent in order, so one commit wait on the last
// part's end token covers them all. A single unit that is over the limit
// on its own even after compression is dropped under the
// tolerate_row_errors policy, like a row Snowflake rejected. (Units over
// the limit uncompressed were dropped before the units were built, so
// their tokens are covered by a neighbour; a unit that only crosses the
// limit after compression and is last in the group is not, and is dropped
// again on redelivery.) tokensFor supplies the request's start/end tokens
// (real ones in exactly-once mode, a fresh synthetic one otherwise).
func (o *snowpipeIndexedOutputPipe) appendInParts(ctx context.Context, channel v2.IngestChannel, units []sendUnit, from, to int,
	tokensFor func(units []sendUnit) (start, end string, err error), dropped *droppedBodies, delivered service.MessageBatch, out *appendOutcome,
) error {
	part := units[from:to]
	rows := make([][]byte, 0, len(part))
	for _, u := range part {
		rows = append(rows, u.rows...)
	}
	start, end, err := tokensFor(part)
	if err != nil {
		return err
	}
	sent, n, err := channel.AppendRows(ctx, rows, start, end)
	if err != nil {
		if !errors.Is(err, v2.ErrPayloadTooLarge) {
			return err
		}
		if to-from == 1 {
			dropped.add(delivered, part[0].msg, err)
			return nil
		}
		mid := from + (to-from)/2
		if err := o.appendInParts(ctx, channel, units, from, mid, tokensFor, dropped, delivered, out); err != nil {
			return err
		}
		return o.appendInParts(ctx, channel, units, mid, to, tokensFor, dropped, delivered, out)
	}
	if sent {
		out.sent = true
		out.parts++
		out.payloadBytes += n
		out.lastEnd = end
	}
	return nil
}

// reportDropped counts and logs a group's dropped messages once the group is
// acked or failed for them, so a redelivery after an unrelated failure does
// not count them again. (A group whose commit wait fails after its rows
// landed dedupes away on redelivery and is not counted then.) It returns
// the error to fail the batch with when the policy calls for that.
func (o *snowpipeIndexedOutputPipe) reportDropped(channelName string, dropped *droppedBodies, batchLen int, sent bool) error {
	if dropped.count == 0 {
		return nil
	}
	o.metrics.ReportRowsRejected(int64(dropped.count))
	msg := fmt.Sprintf("channel %q: dropped %d of %d messages that cannot be sent (first: %s)",
		channelName, dropped.count, batchLen, dropped.first)
	switch {
	case o.tolerateRowErrors:
		o.logger.Warnf("%s", msg)
	case o.offsetToken == nil:
		// Without dedup, failing the batch would re-insert its good rows
		// as duplicates and drop the same bodies again.
		o.logger.Warnf("%s -- not failing the batch despite %s: false, because with %s unset a retry would only re-insert the batch's good rows as duplicates",
			msg, ssopFieldTolerateRowErrors, ssopFieldOffsetToken)
	case !sent:
		// Nothing was appended, so nothing is committed for the redelivery
		// to dedupe against: failing would repeat forever.
		o.logger.Warnf("%s -- not failing the batch despite %s: false, because none of it was appended and a retry would only drop the same bodies again",
			msg, ssopFieldTolerateRowErrors)
	default:
		// The good rows are committed; failing once alerts an operator and
		// the redelivery dedupes to nothing, as for a Snowflake rejection.
		return fmt.Errorf("%s. Set %s: true to only warn; the redelivery of this batch dedupes to nothing", msg, ssopFieldTolerateRowErrors)
	}
	return nil
}

// errorBoundNote renders the channel's last_error_offset_upper_bound for a
// log line, or "" when Snowflake reported none.
func errorBoundNote(channel v2.IngestChannel) string {
	if b := channel.LastErrorOffsetUpperBound(); b != "" {
		return fmt.Sprintf(" (last error at or before offset token %s)", b)
	}
	return ""
}

// markInvalid records that channel's status named a condition an operator
// must fix, logging it once. The next batch for the channel reopens it.
func (o *snowpipeIndexedOutputPipe) markInvalid(name string, err error) {
	o.invalidChannelsMu.Lock()
	defer o.invalidChannelsMu.Unlock()
	if o.invalidChannels == nil {
		o.invalidChannels = map[string]struct{}{}
	}
	if _, already := o.invalidChannels[name]; !already {
		o.logger.Errorf("channel %q is invalid until an operator acts: %v", name, err)
	}
	o.invalidChannels[name] = struct{}{}
}

// isInvalid reports whether name is marked invalid.
func (o *snowpipeIndexedOutputPipe) isInvalid(name string) bool {
	o.invalidChannelsMu.Lock()
	defer o.invalidChannelsMu.Unlock()
	_, ok := o.invalidChannels[name]
	return ok
}

// clearInvalid removes name's invalid mark after a successful reopen.
func (o *snowpipeIndexedOutputPipe) clearInvalid(name string) {
	o.invalidChannelsMu.Lock()
	defer o.invalidChannelsMu.Unlock()
	delete(o.invalidChannels, name)
}

// failCommitWait handles a failed commit wait by the error's family and
// returns the error to fail the batch with. A status an operator must act
// on (the table, not the channel, is the problem) marks the channel
// invalid and holds it: reopening now would fail the same way, so the next
// batch reopens it once. Anything else reopens the channel on the spot, as
// Snowflake's guidance says for the reopen family.
func (o *snowpipeIndexedOutputPipe) failCommitWait(ctx context.Context, channel v2.IngestChannel, err error) error {
	if v2.Classify(err) == v2.KindChannelOperator {
		o.markInvalid(channel.Name(), err)
		o.channelPool.Release(channel.Name(), channel)
		return describeChannelError(err)
	}
	o.reopenAfterFailure(ctx, channel)
	return err
}

// describeChannelError adds the operator action to an error whose Kind is
// KindChannelOperator, so the message that reaches the logs says what to do
// and that retrying alone will not help. Other errors pass through.
func describeChannelError(err error) error {
	if v2.Classify(err) != v2.KindChannelOperator {
		return err
	}
	action := "check that the pipe and its table exist and that the role has access to them"
	cse, _ := errors.AsType[*v2.ChannelStatusError](err)
	if cse != nil && strings.HasPrefix(cse.Code, "ERR_CHANNEL_SCHEMA_EVOLUTION_") {
		action = "Snowflake could not evolve the table to fit this data (for example a column type or Iceberg time scale it does not support); pre-create the column with the right type, or disable schema evolution on the table"
	}
	if cse != nil && cse.OnOpen {
		return fmt.Errorf("%w. This will not resolve itself on retry: %s", err, action)
	}
	return fmt.Errorf("%w. This will not resolve itself on retry or by reopening the channel: %s. The channel is reopened on the next batch once that is done", err, action)
}

func (o *snowpipeIndexedOutputPipe) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	exec := batch.InterpolationExecutor(o.channelName)
	groups, err := groupMessagesByChannel(exec, batch)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := o.writeChannelGroup(ctx, group, batch); err != nil {
			// An error nacks the whole batch; on retry, groups that already
			// committed dedupe to nothing (exactly-once) or are re-sent
			// (at-least-once), so there is nothing to gain from continuing.
			return err
		}
	}
	return nil
}

// writeChannelGroup runs the acquire, dedup, append, commit-wait, report,
// release cycle for one channel's share of a batch. delivered is the whole
// delivered batch, for positions in log and error text.
func (o *snowpipeIndexedOutputPipe) writeChannelGroup(ctx context.Context, group channelGroup, delivered service.MessageBatch) error {
	// Acquire keys by group.name and Release by channel.Name(); these agree
	// because OpenChannel always names the channel as asked.
	channel, err := o.channelPool.Acquire(ctx, group.name)
	if err != nil {
		// Nothing is pooled after a failed open, so the next Acquire opens
		// afresh on its own; no invalid mark is needed.
		return fmt.Errorf("unable to open snowflake streaming channel: %w", describeChannelError(err))
	}
	if o.isInvalid(group.name) {
		// The channel was invalidated by a condition an operator had to
		// fix; try once to reopen it now that a new batch has arrived. The
		// mark stays until a reopen succeeds, so a transient reopen
		// failure does not send the next batch to the dead channel.
		o.reportRejectedBeforeReopen(channel)
		reopened, rerr := o.openChannel(ctx, group.name)
		if rerr != nil {
			o.channelPool.Release(channel.Name(), channel)
			return fmt.Errorf("unable to reopen snowflake streaming channel: %w", describeChannelError(rerr))
		}
		o.clearInvalid(group.name)
		channel = reopened
	}
	batch := group.batch
	var origFirst string
	// tokenAt interpolates offset_token for a position in the surviving
	// batch, once; only request boundaries need it, so the common path
	// costs two interpolations however large the batch.
	var tokenAt func(i int) (string, error)
	if o.offsetToken != nil {
		// Ordering is checked on the unfiltered token range, so a violation
		// is caught even when the filter would treat the group as a
		// duplicate.
		var origLast string
		exec := batch.InterpolationExecutor(o.offsetToken)
		if origFirst, err = requireOffsetToken(exec, 0); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		if origLast, err = requireOffsetToken(exec, len(batch)-1); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		if err = o.checkSubmissionOrder(channel.Name(), origFirst, origLast); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		batch, err = preprocessForExactlyOncePipe(ctx, channel, o.offsetToken, batch)
		if err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		if len(batch) == 0 {
			// Every row is at or below the committed token, so the whole
			// range has landed already.
			o.recordAckedRange(channel.Name(), origFirst)
			o.channelPool.Release(channel.Name(), channel)
			return nil
		}
		exec = batch.InterpolationExecutor(o.offsetToken)
		memo := make(map[int]string, 2)
		tokenAt = func(i int) (string, error) {
			if tok, ok := memo[i]; ok {
				return tok, nil
			}
			tok, err := requireOffsetToken(exec, i)
			if err == nil {
				memo[i] = tok
			}
			return tok, err
		}
	}
	// At-least-once needs no dedup; the commit wait's call-unique synthetic
	// token is minted per request in tokensFor, and the counter was synced
	// against the channel when it was opened.
	var dropped droppedBodies
	rowsByMsg, emptyRows, err := prepareRows(batch, delivered, &dropped)
	if err != nil {
		o.channelPool.Release(channel.Name(), channel)
		return err
	}
	if emptyRows > 0 {
		o.metrics.ReportEmptyRowsSkipped(int64(emptyRows))
		o.logger.Debugf("channel %s: skipping %d of %d messages with an empty or whitespace-only body (e.g. tombstones); Snowflake has no row representation for them", channel.Name(), emptyRows, len(batch))
	}
	// A message whose rows alone are over the request limit can never be
	// sent. Find those here, before the units are built, so their tokens
	// fold into a neighbour's range and the committed token still covers
	// them on redelivery. The limit is on the body as sent, so with
	// compression on a message over the limit uncompressed is encoded on
	// its own to see whether it fits; that only happens for messages
	// already over the limit raw, which is rare.
	for i, msgRows := range rowsByMsg {
		if msgRows == nil {
			continue
		}
		raw := 0
		for _, r := range msgRows {
			raw += len(r) + 1
		}
		// zstd's frame overhead on incompressible input is small but not
		// zero, so a message just under the limit raw can still cross it
		// compressed; the margin keeps such a message in this pre-check.
		if raw <= v2.MaxRequestBytes-4096 {
			continue
		}
		if _, _, err := v2.EncodeRows(msgRows, o.compress); errors.Is(err, v2.ErrPayloadTooLarge) {
			dropped.add(delivered, batch[i], err)
			rowsByMsg[i] = nil
		}
	}
	units := buildUnits(batch, rowsByMsg)
	tokensFor := func(part []sendUnit) (string, string, error) {
		if tokenAt == nil {
			tok := o.nextSyntheticCommitToken()
			return tok, tok, nil
		}
		start, err := tokenAt(part[0].from)
		if err != nil {
			return "", "", err
		}
		end, err := tokenAt(part[len(part)-1].to)
		return start, end, err
	}
	var out appendOutcome
	appendStart := time.Now()
	if len(units) > 0 {
		o.logger.Debugf("inserting %d messages using channel %s", len(units), channel.Name())
		if err := o.appendInParts(ctx, channel, units, 0, len(units), tokensFor, &dropped, delivered, &out); err != nil {
			if out.sent {
				// Earlier parts reached the server. Let them commit before
				// failing: in exactly-once mode so the redelivery dedupes
				// against them, and in every mode so a reopen does not race
				// their pending commit.
				o.metrics.ReportPayloadSent(int64(out.payloadBytes))
				if werr := channel.WaitUntilCommitted(ctx, out.lastEnd, o.commitTimeout); werr != nil {
					return fmt.Errorf("%w (and the parts already sent did not commit: %w)", err, o.failCommitWait(ctx, channel, werr))
				}
			}
			switch v2.Classify(err) {
			case v2.KindBackpressure, v2.KindLocalEncoding:
				// Backpressure leaves the channel usable; reopening would
				// only add load to a saturated control plane. A local
				// encoding failure never reached the server at all.
				o.channelPool.Release(channel.Name(), channel)
			default:
				o.reopenAfterFailure(ctx, channel)
			}
			return err
		}
	}
	appendDuration := time.Since(appendStart)
	if !out.sent {
		// Nothing was sent (every body was empty or dropped), so there is
		// no token to wait for; waiting would time out on every redelivery.
		// The range is acked all the same, so it counts for the ordering
		// check.
		o.logger.Debugf("channel %s: every message in this batch of %d was empty or dropped, skipping the commit wait", channel.Name(), len(batch))
		o.recordAckedRange(channel.Name(), origFirst)
		o.channelPool.Release(channel.Name(), channel)
		return o.reportDropped(channel.Name(), &dropped, len(delivered), false)
	}
	o.metrics.ReportPayloadSent(int64(out.payloadBytes))
	if out.parts > 1 {
		o.logger.Debugf("channel %s: batch sent as %d requests (%d bytes) because one request would exceed the %d byte limit", channel.Name(), out.parts, out.payloadBytes, v2.MaxRequestBytes)
	}
	o.logger.Debugf("done inserting %d messages using channel %s in %s (%d request(s), %d bytes)", len(units), channel.Name(), appendDuration, out.parts, out.payloadBytes)
	commitStart := time.Now()
	if err := channel.WaitUntilCommitted(ctx, out.lastEnd, o.commitTimeout); err != nil {
		return o.failCommitWait(ctx, channel, err)
	}
	commitDuration := time.Since(commitStart)
	o.logger.Debugf("batch of %d messages committed using channel %s in %s", len(units), channel.Name(), commitDuration)
	// RowsRejected is the delta since last reported; RowsErrorCount is the
	// channel's lifetime total. The metric is incremented in every mode.
	if rejected := channel.RowsRejected(); rejected > 0 {
		total := channel.RowsErrorCount()
		initial := total - rejected
		o.metrics.ReportRowsRejected(rejected)
		msg := fmt.Sprintf("channel %q has %d new errors (total: %d, initial: %d). Last error message: %s%s",
			channel.Name(), rejected, total, initial, channel.LastErrorMessage(), errorBoundNote(channel))
		switch {
		case o.tolerateRowErrors:
			o.logger.Warnf("%s", msg)
		case o.offsetToken == nil:
			// Without dedup, failing the batch would re-insert its good rows
			// as duplicates and hit the same rejection forever.
			o.logger.Warnf("%s -- not failing the batch despite %s: false, because with %s unset a retry would only re-insert the batch's good rows as duplicates and be rejected again",
				msg, ssopFieldTolerateRowErrors, ssopFieldOffsetToken)
		default:
			// The channel is healthy; only this batch fails.
			o.channelPool.Release(channel.Name(), channel)
			return fmt.Errorf("%s", msg)
		}
	} else if total := channel.RowsErrorCount(); total > 0 {
		// Historical errors this batch did not add to: debug only, or every
		// batch after one bad row would re-warn forever.
		o.logger.Debugf("channel %s has %d pre-existing row error(s) not from this batch: %s", channel.Name(), total, channel.LastErrorMessage())
	}
	// The range is committed either way, so the floor is set even when the
	// dropped-body policy fails the batch below: the redelivery dedupes.
	o.recordAckedRange(channel.Name(), origFirst)
	o.metrics.Report(appendDuration, commitDuration)
	o.channelPool.Release(channel.Name(), channel)
	return o.reportDropped(channel.Name(), &dropped, len(delivered), true)
}

func (o *snowpipeIndexedOutputPipe) Close(context.Context) error {
	o.channelPool.Reset()
	return nil
}

// preprocessForExactlyOncePipe drops messages whose offset token is not newer
// than the channel's last committed token and returns the filtered batch with
// its first and last raw tokens. The batch must be in increasing token order.
// It refuses a committed token whose shape is incomparable with the batch's
// (synthetic vs real, integer vs non-integer), since every row would
// otherwise be dropped as a duplicate.
func preprocessForExactlyOncePipe(
	ctx context.Context,
	channel v2.IngestChannel,
	offsetTokenMapping *service.InterpolatedString,
	batch service.MessageBatch,
) (service.MessageBatch, error) {
	latest, err := channel.LatestOffsetToken(ctx)
	if err != nil {
		return nil, err
	}
	if v2.IsSyntheticToken(latest) {
		return nil, fmt.Errorf(
			"channel %q was last written with %s unset (its last committed offset token %q is a synthetic one this "+
				"output minted for at-least-once delivery), so no real %s value can ever compare as newer than it -- "+
				"every row would be dropped as an already-committed duplicate. This will not resolve itself on retry: "+
				"give this pipeline a fresh %s so it starts from an empty channel",
			channel.Name(), ssopFieldOffsetToken, latest, ssopFieldOffsetToken, ssopFieldChannelName)
	}
	exec := batch.InterpolationExecutor(offsetTokenMapping)
	firstRawToken, err := requireOffsetToken(exec, 0)
	if err != nil {
		return nil, err
	}
	if _, err := requireOffsetToken(exec, len(batch)-1); err != nil {
		return nil, err
	}
	if latest != "" && v2.MixedNumericTokens(firstRawToken, latest) {
		return nil, fmt.Errorf(
			"channel %q's last committed offset token %q and this batch's %s value %q cannot be compared (one is a "+
				"bare integer, the other is not), so exactly-once dedup would be arbitrary -- this usually means %s's "+
				"expression was changed after the channel was already written to. This will not resolve itself on retry: "+
				"give this pipeline a fresh %s so it starts from an empty channel",
			channel.Name(), latest, ssopFieldOffsetToken, firstRawToken, ssopFieldOffsetToken, ssopFieldChannelName)
	}
	// fast path: nothing committed yet, or the whole batch is ahead -> no filtering needed
	if latest == "" || v2.CompareOffsetTokens(firstRawToken, latest) > 0 {
		return batch, nil
	}
	// We need to filter out data that is too old.
	filteredBatch := make(service.MessageBatch, 0, len(batch))
	for i := range batch {
		rawToken, err := requireOffsetToken(exec, i)
		if err != nil {
			return nil, err
		}
		// per-row filter: at or below the committed token -> already landed, drop it
		if v2.CompareOffsetTokens(rawToken, latest) <= 0 {
			continue
		}
		filteredBatch = append(filteredBatch, batch[i])
	}
	return filteredBatch, nil
}

// requireOffsetToken interpolates offset_token for message i and rejects an
// empty result (the "nothing committed" sentinel) or "null" (how Bloblang
// renders a missing metadata field), either of which would otherwise let
// rows share a bogus token.
func requireOffsetToken(exec *service.MessageBatchInterpolationExecutor, index int) (string, error) {
	token, err := exec.TryString(index)
	if err != nil {
		return "", err
	}
	if token == "" || token == "null" {
		return "", fmt.Errorf("%s interpolated to %q for message %d: exactly-once delivery requires a non-empty offset token for every message", ssopFieldOffsetToken, token, index)
	}
	return token, nil
}
