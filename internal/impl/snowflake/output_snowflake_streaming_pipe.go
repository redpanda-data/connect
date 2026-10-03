// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package snowflake

import (
	"bytes"
	"context"
	"crypto/rsa"
	"fmt"
	neturl "net/url"
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
		Version("4.110.0").
		Summary("Ingest data into Snowflake using Snowpipe Streaming, writing through a Snowflake pipe.").
		Description(`
Ingest data into Snowflake using Snowpipe Streaming, writing through a Snowflake pipe rather than directly into a table.

Unlike `+"`snowflake_streaming`"+`, which opens channels on a table, this output opens them on a pipe, so any transformation defined in the pipe's `+"`COPY INTO`"+` (stream-static join enrichment, `+"`WHERE`"+` filtering) runs before rows land. If you don't need those, `+"`snowflake_streaming`"+` is the simpler choice: it creates its table for you and needs no pipe.

This is an Enterprise feature and requires a valid Redpanda Enterprise Edition license that includes Connect (https://docs.redpanda.com/redpanda-connect/get-started/licensing/[licensing docs^]).

The `+"`"+ssopFieldPipe+"`"+` (and its table) must already exist; this output never issues `+"`CREATE PIPE`"+` or `+"`CREATE TABLE`"+`. A missing pipe is reported when the output connects, before any message is written.

Moving an existing `+"`snowflake_streaming`"+` pipeline across is not a rename: several fields are removed and `+"`"+ssopFieldCommitTimeout+"`"+` changes meaning. See the xref:guides:migrating-to-snowflake-streaming-pipe.adoc[migration guide].

[%header,format=dsv]
|===
Snowflake column type:Allowed format in Redpanda Connect
CHAR, VARCHAR:string
BINARY:[]byte
NUMBER:any numeric type, string
FLOAT:any numeric type
BOOLEAN:bool,any numeric type,string parsable according to `+"`strconv.ParseBool`"+`
TIME,DATE,TIMESTAMP:unix or RFC 3339 with nanoseconds timestamps
VARIANT,ARRAY,OBJECT:any data type is converted into JSON
GEOGRAPHY,GEOMETRY: Not supported
|===

Authentication can be configured using a https://docs.snowflake.com/en/user-guide/key-pair-auth[RSA Key Pair^].

There are https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#limitations[limitations^] of what data types can be loaded into Snowflake using this method.

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

*If the requirement is violated anyway*, batches this process can see are out of order fail with an error naming the channel and tokens, and `+"`snowflake_out_of_order_submissions`"+` is incremented; the error recurs on every retry until `+"`"+ssopFieldChannelName+"`"+` or `+"`max_in_flight`"+` is fixed. That detection is process-local: it misses overlapping batches, other replicas, and anything after a restart, when the refused rows would be dropped as duplicates with no error. Recover them from your dead-letter queue or the source; redelivery alone cannot re-insert them.

NOTE: There is a limit of 10,000 streams per table -- if using more than 10k streams please reach out to Snowflake support.
`+service.OutputPerformanceDocs(true, true)+`

*Despite the general advice above, don't set a `+"`batching`"+` policy on this output.* Batch size is governed by the input's `+"`max_yield_batch_bytes`"+` (default `+"`32KB`"+`, well below what this output can carry); raise that instead. An output batching policy pools every partition's yields into one request, serializing partitions and risking the 4MiB limit.

Requests are capped at 4MiB uncompressed. An oversized batch is rejected rather than split, and identically on every retry, so exceeding the limit wedges that channel; keep `+"`max_yield_batch_bytes`"+` under it and under the input's `+"`partition_buffer_bytes`"+`.
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
				Description(`Override the control-plane host (default `+"`<account>.snowflakecomputing.com`"+`) while keeping ingest-hostname discovery. Use this for https://docs.snowflake.com/en/user-guide/admin-privatelink[PrivateLink^] accounts. Ignored when `+"`"+ssopFieldURL+"`"+` is set.
`).Optional().Example("my-account.privatelink.snowflakecomputing.com").Advanced(),
			service.NewStringField(ssopFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user.").
				ShortDescription("The user to run the Snowpipe Stream as."),
			service.NewStringField(ssopFieldRole).Description("The role to ingest as. Every streaming call is authorized against a session token scoped to this role (not the user's default role), so it needs the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] on the pipe and table.").
				ShortDescription("The role to ingest as; sent as a session-scoped token, so it does constrain ingestion privileges.").Example("SNOWPIPE_STREAMING_ROLE"),
			service.NewStringField(ssopFieldDB).Description("The Snowflake database to ingest data into.").Example("MY_DATABASE"),
			service.NewStringField(ssopFieldSchema).Description("The Snowflake schema to ingest data into.").Example("PUBLIC"),
			service.NewStringField(ssopFieldPipe).Description("The Snowflake pipe to ingest data into. It must already exist; a missing pipe is reported when the output connects. Unlike `database` and `schema` this value is not upper-cased, so match the stored identifier exactly (`MY_PIPE` for a pipe created as `CREATE PIPE my_pipe`).").Example("MY_PIPE"),
			service.NewStringField(ssopFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").
				ShortDescription("PEM encoded private RSA key for authenticating with Snowflake. Either this or private_key_file is required.").Optional().Secret(),
			service.NewStringField(ssopFieldKeyFile).Description("The file to load the private RSA key from. This should be a `.p8` PEM encoded file. Either this or `private_key` must be specified.").
				ShortDescription("File to load the private RSA key from, as a .p8 PEM file. Either this or private_key is required.").Optional(),
			service.NewStringField(ssopFieldKeyPass).Description("The RSA key passphrase if the RSA key is encrypted.").Optional().Secret(),
			service.NewBatchPolicyField(ssopFieldBatching),
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
				Description(`Whether to tolerate rows Snowflake rejects after a batch is appended (malformed or uncastable values, reported asynchronously in the channel status). `+"`false`"+` (the default, matching the Kafka Connector's `+"`errors.tolerance=none`"+`) fails the batch with an error naming the channel and Snowflake's last error; `+"`true`"+` only logs a warning. Either way `+"`snowflake_rows_error_count`"+` is incremented and the rejected rows never reach a dead-letter queue. Failing the batch does not retry the rejected rows (Snowflake has already refused them and the redelivery dedupes to nothing); it exists to alert an operator. When `+"`"+ssopFieldOffsetToken+"`"+` is unset a rejection therefore never fails the batch even with `+"`false`"+`, since without dedup a retry would only re-insert the good rows as duplicates.`).
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
    # Large batches perform best, but count is not byte-aware: size it so a
    # batch stays under this output's 4MiB per-request limit.
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
        # one request and risk the 4MiB limit.
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
    # Keep each flush well under this output's 4MiB per-request limit: an
    # oversized batch is rejected identically on every retry and wedges
    # the pipeline. 2MiB leaves headroom.
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
		logger:            mgr.Logger(),
		metrics:           newSnowpipePipeMetrics(mgr.Metrics()),
		openChannelFn: func(ctx context.Context, name string) (v2.IngestChannel, error) {
			return client.OpenChannel(ctx, name)
		},
	}
	indexed.channelPool = pool.NewIndexed(func(ctx context.Context, name string) (v2.IngestChannel, error) {
		return indexed.openChannelFn(ctx, name)
	})
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

	// checkedTokenSpaces holds channels already vetted by
	// checkSyntheticTokenSpace, so its status call runs once per channel.
	checkedTokenSpaces   map[string]struct{}
	checkedTokenSpacesMu sync.Mutex

	// submittedWatermarks is the highest end token this process has
	// submitted per channel; see checkSubmissionOrder.
	submittedWatermarks   map[string]string
	submittedWatermarksMu sync.Mutex
}

// checkSubmissionOrder turns an out-of-order submission (a batch whose end
// token is below what this process already submitted on the channel, i.e.
// another writer got ahead of it) into an error, instead of letting the
// dedup filter silently drop the rows as duplicates. It only sees this
// process's own history, so it cannot make concurrent unordered writers
// safe. The error is permanent for that batch: the watermark only advances.
func (o *snowpipeIndexedOutputPipe) checkSubmissionOrder(channelName, start, end string) error {
	o.submittedWatermarksMu.Lock()
	defer o.submittedWatermarksMu.Unlock()
	if o.submittedWatermarks == nil {
		o.submittedWatermarks = map[string]string{}
	}
	if prev, ok := o.submittedWatermarks[channelName]; ok && v2.CompareOffsetTokens(end, prev) < 0 {
		o.metrics.ReportOutOfOrderSubmission()
		return fmt.Errorf(
			"channel %q received offset tokens %s..%s after this process had already submitted a batch up to %s -- "+
				"messages are arriving out of order for this channel, which breaks offset_token's exactly-once guarantee "+
				"(see the channel_name field's docs: a channel must be written to by only one ordered source, "+
				"with at most one batch in flight to it at a time). This will not resolve itself on retry: fix "+
				"channel_name (give each independent source its own channel) or max_in_flight (cap it at 1 for a "+
				"shared channel) rather than relying on redelivery",
			channelName, start, end, prev)
	}
	if prev, ok := o.submittedWatermarks[channelName]; !ok || v2.CompareOffsetTokens(end, prev) > 0 {
		o.submittedWatermarks[channelName] = end
	}
	return nil
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

// checkSyntheticTokenSpace runs once per channel in at-least-once mode. It
// bumps the counter past any synthetic token already committed on the
// channel, and refuses a channel whose committed real token would compare at
// or above our synthetic ones (every commit wait would then succeed before
// its append was confirmed). A failed status fetch leaves the channel
// unchecked so the next batch retries.
func (o *snowpipeIndexedOutputPipe) checkSyntheticTokenSpace(ctx context.Context, channel v2.IngestChannel) error {
	name := channel.Name()
	o.checkedTokenSpacesMu.Lock()
	_, done := o.checkedTokenSpaces[name]
	o.checkedTokenSpacesMu.Unlock()
	if done {
		return nil
	}
	latest, err := channel.LatestOffsetToken(ctx)
	if err != nil {
		return err
	}
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
				name, ssopFieldOffsetToken, latest, ssopFieldOffsetToken, ssopFieldChannelName)
		}
	}
	o.checkedTokenSpacesMu.Lock()
	if o.checkedTokenSpaces == nil {
		o.checkedTokenSpaces = map[string]struct{}{}
	}
	o.checkedTokenSpaces[name] = struct{}{}
	o.checkedTokenSpacesMu.Unlock()
	return nil
}

func (o *snowpipeIndexedOutputPipe) openChannel(ctx context.Context, name string) (v2.IngestChannel, error) {
	o.logger.Debugf("opening snowflake streaming channel for `%s.%s`: %s", o.db, o.schema, name)
	return o.openChannelFn(ctx, name)
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

// reopenAfterFailure replaces a channel in the pool after an append or
// commit-wait error, keeping the old one if the reopen fails. Rejected rows
// are reported first: a reopen resets the channel's error baseline, and on
// redelivery an already-committed batch dedupes to nothing before the
// post-commit check, so this is the only place they would be counted.
func (o *snowpipeIndexedOutputPipe) reopenAfterFailure(ctx context.Context, channel v2.IngestChannel) {
	if rejected := channel.RowsRejected(); rejected > 0 {
		o.metrics.ReportRowsRejected(rejected)
		o.logger.Warnf("channel %q has %d new row error(s) (total: %d) recorded before this batch failed. Last error message: %s",
			channel.Name(), rejected, channel.RowsErrorCount(), channel.LastErrorMessage())
	}
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

func (o *snowpipeIndexedOutputPipe) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	exec := batch.InterpolationExecutor(o.channelName)
	groups, err := groupMessagesByChannel(exec, batch)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := o.writeChannelGroup(ctx, group); err != nil {
			// An error nacks the whole batch; on retry, groups that already
			// committed dedupe to nothing (exactly-once) or are re-sent
			// (at-least-once), so there is nothing to gain from continuing.
			return err
		}
	}
	return nil
}

// writeChannelGroup runs the acquire, dedup, append, commit-wait, report,
// release cycle for one channel's share of a batch.
func (o *snowpipeIndexedOutputPipe) writeChannelGroup(ctx context.Context, group channelGroup) error {
	// Acquire keys by group.name and Release by channel.Name(); these agree
	// because OpenChannel always names the channel as asked.
	channel, err := o.channelPool.Acquire(ctx, group.name)
	if err != nil {
		return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
	}
	batch := group.batch
	var startOffsetToken, endOffsetToken string
	if o.offsetToken != nil {
		// Ordering is checked on the unfiltered token range, so a violation
		// is caught even when the filter would treat the group as a
		// duplicate.
		var origFirst, origLast string
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
		batch, startOffsetToken, endOffsetToken, err = preprocessForExactlyOncePipe(ctx, channel, o.offsetToken, batch)
		if err != nil || len(batch) == 0 {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
	} else {
		// At-least-once: no dedup, but the commit wait still needs a
		// call-unique token (see noDedupCommitToken).
		if err = o.checkSyntheticTokenSpace(ctx, channel); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		token := o.nextSyntheticCommitToken()
		startOffsetToken, endOffsetToken = token, token
	}
	o.logger.Debugf("inserting rows using channel %s at offset tokens: start=%s end=%s", channel.Name(), startOffsetToken, endOffsetToken)
	rows := make([][]byte, len(batch))
	// Empty/whitespace-only rows (tombstones) are dropped by AppendRows;
	// count them here so the drop is visible.
	emptyRows := 0
	for i, msg := range batch {
		row, err := msg.AsBytes()
		if err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return fmt.Errorf("unable to serialize message: %w", err)
		}
		if len(bytes.TrimSpace(row)) == 0 {
			emptyRows++
		}
		rows[i] = row
	}
	if emptyRows > 0 {
		o.metrics.ReportEmptyRowsSkipped(int64(emptyRows))
		o.logger.Debugf("channel %s: skipping %d of %d rows with an empty or whitespace-only body (e.g. tombstones); Snowflake has no row representation for them", channel.Name(), emptyRows, len(batch))
	}
	appendStart := time.Now()
	sent, err := channel.AppendRows(ctx, rows, startOffsetToken, endOffsetToken)
	if err != nil {
		if v2.IsBackpressure(err) {
			// Backpressure leaves the channel usable; reopening would only
			// add load to a saturated control plane.
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		o.reopenAfterFailure(ctx, channel)
		return err
	}
	appendDuration := time.Since(appendStart)
	if !sent {
		// Nothing was sent (every row was empty), so there is no token to
		// wait for; waiting would time out on every redelivery.
		o.logger.Debugf("channel %s: every row in this batch of %d was empty, skipping the commit wait", channel.Name(), len(batch))
		o.channelPool.Release(channel.Name(), channel)
		return nil
	}
	o.logger.Debugf("done inserting %d rows using channel %s in %s", len(batch), channel.Name(), appendDuration)
	commitStart := time.Now()
	if err := channel.WaitUntilCommitted(ctx, endOffsetToken, o.commitTimeout); err != nil {
		o.reopenAfterFailure(ctx, channel)
		return err
	}
	commitDuration := time.Since(commitStart)
	o.logger.Debugf("batch of %d rows committed using channel %s in %s", len(batch), channel.Name(), commitDuration)
	// RowsRejected is the delta since last reported; RowsErrorCount is the
	// channel's lifetime total. The metric is incremented in every mode.
	if rejected := channel.RowsRejected(); rejected > 0 {
		total := channel.RowsErrorCount()
		initial := total - rejected
		o.metrics.ReportRowsRejected(rejected)
		msg := fmt.Sprintf("channel %q has %d new errors (total: %d, initial: %d). Last error message: %s",
			channel.Name(), rejected, total, initial, channel.LastErrorMessage())
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
	o.metrics.Report(appendDuration, commitDuration)
	o.channelPool.Release(channel.Name(), channel)
	return nil
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
) (service.MessageBatch, string, string, error) {
	latest, err := channel.LatestOffsetToken(ctx)
	if err != nil {
		return nil, "", "", err
	}
	if v2.IsSyntheticToken(latest) {
		return nil, "", "", fmt.Errorf(
			"channel %q was last written with %s unset (its last committed offset token %q is a synthetic one this "+
				"output minted for at-least-once delivery), so no real %s value can ever compare as newer than it -- "+
				"every row would be dropped as an already-committed duplicate. This will not resolve itself on retry: "+
				"give this pipeline a fresh %s so it starts from an empty channel",
			channel.Name(), ssopFieldOffsetToken, latest, ssopFieldOffsetToken, ssopFieldChannelName)
	}
	exec := batch.InterpolationExecutor(offsetTokenMapping)
	firstRawToken, err := requireOffsetToken(exec, 0)
	if err != nil {
		return nil, "", "", err
	}
	lastRawToken, err := requireOffsetToken(exec, len(batch)-1)
	if err != nil {
		return nil, "", "", err
	}
	if latest != "" && v2.MixedNumericTokens(firstRawToken, latest) {
		return nil, "", "", fmt.Errorf(
			"channel %q's last committed offset token %q and this batch's %s value %q cannot be compared (one is a "+
				"bare integer, the other is not), so exactly-once dedup would be arbitrary -- this usually means %s's "+
				"expression was changed after the channel was already written to. This will not resolve itself on retry: "+
				"give this pipeline a fresh %s so it starts from an empty channel",
			channel.Name(), latest, ssopFieldOffsetToken, firstRawToken, ssopFieldOffsetToken, ssopFieldChannelName)
	}
	// fast path: nothing committed yet, or the whole batch is ahead -> no filtering needed
	if latest == "" || v2.CompareOffsetTokens(firstRawToken, latest) > 0 {
		return batch, firstRawToken, lastRawToken, nil
	}
	// We need to filter out data that is too old.
	filteredBatch := make(service.MessageBatch, 0, len(batch))
	for i := range batch {
		rawToken, err := requireOffsetToken(exec, i)
		if err != nil {
			return nil, "", "", err
		}
		// per-row filter: at or below the committed token -> already landed, drop it
		if v2.CompareOffsetTokens(rawToken, latest) <= 0 {
			continue
		}
		filteredBatch = append(filteredBatch, batch[i])
	}
	if len(filteredBatch) == 0 {
		return filteredBatch, "", "", nil
	}
	// This is a lazy way to compute the bounds, but filtering should be a rare operation.
	return preprocessForExactlyOncePipe(ctx, channel, offsetTokenMapping, filteredBatch)
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
