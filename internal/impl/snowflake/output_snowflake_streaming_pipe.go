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
	// ssopFieldCommitTimeout collapses the v1 snowflake_streaming output's
	// commit_backoff (initial_interval, max_interval, max_elapsed_time,
	// multiplier) plus its separately-named deprecated commit_timeout
	// override into a single wait budget: this output's WaitUntilCommitted takes one
	// context.Context and one time.Duration timeout, not a backoff
	// schedule.
	ssopFieldCommitTimeout = "commit_timeout"
	// ssopFieldTolerateRowErrors has no equivalent in the v1
	// snowflake_streaming output. It maps onto the Kafka Connector's
	// errors.tolerance (`none`/`all`): rows Snowflake rejects
	// asynchronously after append are always logged and metered, and this
	// field controls whether they also fail the batch. Default false
	// matches the Kafka Connector's own default (errors.tolerance=none):
	// fail loud on any newly-rejected row.
	ssopFieldTolerateRowErrors = "tolerate_row_errors"
)

// defaultChannelName builds the channel_name default used when channel_name
// is not configured: a single, constant, unpartitioned channel scoped to the
// fully-qualified pipe name. This output has no pooled/unordered mode to
// fall back to, so a single default channel -- rather than a per-partition
// one keyed off Kafka-specific metadata -- is the only default that works
// regardless of what kind of input is feeding this output. An operator who
// wants partition-aware parallelism opts in explicitly via channel_name; see
// its own doc comment for the pattern.
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

The `+"`snowflake_streaming`"+` output opens its channels on a table, which puts Snowflake's Streaming Transformations -- enrichment via a stream-static join, and server-side `+"`WHERE`"+`-clause filtering -- out of reach, since those only exist on a pipe's `+"`COPY INTO`"+` and a table has no such thing. This output opens its channels on a pipe instead, so any transformation defined on that pipe runs before rows land; the connector itself still sends rows untouched. If you don't need those transformations, `+"`snowflake_streaming`"+` remains the simpler choice -- it creates its destination table for you and needs no pipe set up first; this output never does either (see below).

This is an Enterprise feature: running it requires a valid Redpanda Enterprise Edition license that includes the Connect product. See https://docs.redpanda.com/redpanda-connect/get-started/licensing/[the licensing docs^] if you haven't already got one -- a config that lints clean will still fail at startup without it.

This output never issues `+"`CREATE PIPE`"+` or `+"`CREATE TABLE`"+` (the `+"`snowflake_streaming`"+` output can create its target table): the `+"`"+ssopFieldPipe+"`"+` field names an object that must already exist. Create the table and pipe yourself first -- the pipe's `+"`SELECT`"+` is where any casting, projection, renaming or filtering happens -- before pointing this output at it; a missing pipe fails at connector startup rather than at the first message.

Moving an existing `+"`snowflake_streaming`"+` pipeline across involves more than adding `+"`"+ssopFieldPipe+"`"+`: several fields are removed outright, and `+"`"+ssopFieldCommitTimeout+"`"+` keeps its name but changes what it means. See the xref:guides:migrating-to-snowflake-streaming-pipe.adoc[migration guide] before carrying a config forward unchanged.

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

Messages whose body is empty or whitespace-only -- most commonly Kafka/Redpanda tombstones, whose null value arrives as an empty body -- are not sent to Snowflake at all, since a row has no representation for "no value". This matches the Snowflake Kafka Connector's default (`+"`behavior.on.null.values=IGNORE`"+`). Such messages are still acknowledged upstream as delivered, are counted in the `+"`snowflake_empty_rows_skipped`"+` metric, and are logged at debug level; a batch consisting entirely of them is acknowledged without appending anything. The one caveat is `+"`"+ssopFieldOffsetToken+"`"+`: when set, it is still evaluated for these messages, so it must resolve for an empty body -- a metadata-based expression such as `+"`${! @kafka_offset }`"+` does, one derived from the message content does not and fails the batch. If tombstones should instead produce a row (a soft-delete marker, say), map them to one with a processor before this output.

[[choosing-a-channel-name]]
== Choosing a `+"`"+ssopFieldChannelName+"`"+`

By default, every message shares a single, constant, unpartitioned channel scoped to this output's pipe: `+"`<database>.<schema>.<pipe>`"+`. Set `+"`"+ssopFieldChannelName+"`"+` explicitly to split that into more than one channel -- most commonly, one per Kafka/Redpanda partition.

*The requirement this exists to satisfy:* a channel must only ever be written to by one ordered source, with at most one batch in flight to it at a time. `+"`"+ssopFieldOffsetToken+"`"+`'s dedup check assumes tokens arrive in increasing order, and has no way to tell "this was already committed by me" apart from "a different, unrelated writer just leapfrogged me" -- two unrelated sources sharing one channel breaks that assumption regardless of how carefully each one is itself ordered.

*For a partitioned Kafka/Redpanda input*, scope `+"`"+ssopFieldChannelName+"`"+` per partition so each partition gets its own channel and its own independent offset-token sequence:

[source,yaml]
----
channel_name: "<pipe>-p${! @kafka_partition }"
----

This pattern is safe at any `+"`max_in_flight`"+` *only if the input dispatches at most one batch per partition at a time*, so that the concurrency plays out _across_ partitions and never within one. Check your input, because they differ: the `+"`redpanda`"+` input does this by default (unless `+"`unordered.enabled`"+` is set), whereas `+"`kafka_franz`"+` and `+"`kafka`"+` allow several batches from one partition to be in flight at once (their `+"`checkpoint_limit`"+` defaults to 1024). With those inputs, either set `+"`checkpoint_limit: 1`"+` on the input or `+"`max_in_flight: 1`"+` on this output; otherwise two batches from the same partition can reach the same channel out of token order, which this output detects and fails permanently (see below) rather than silently mis-dedups.

If the input consumes more than one topic, include the topic in the name as well -- `+"`\"<pipe>-${! @kafka_topic }-p${! @kafka_partition }\"`"+` -- since partition 0 of two topics carry independent offset sequences and must not share a channel.

The prefix must be stable for a given partition across restarts and across hosts. *Never derive it from the host* (a `+"`channel-${HOST}`"+`-style value): a consumer-group rebalance would then hand the partition to a channel with no committed history, and every row the previous host already landed gets re-sent as new.

*If you're not sure*, or the input has no natural partition key at all: the always-safe fallback is this field's own default (one shared channel) with `+"`max_in_flight: 1`"+` set explicitly -- slower, but there is then only ever one writer, period.

*If the one-writer-per-channel requirement is violated anyway*, this output does not silently drop the affected rows: it fails the batch with an error naming the channel and the offset tokens involved, and increments the `+"`snowflake_out_of_order_submissions`"+` metric. That error is not transient -- it will keep recurring on retry for as long as the same concurrent writers keep contending for the channel. Fix `+"`"+ssopFieldChannelName+"`"+` or `+"`max_in_flight`"+`, don't wait it out.

NOTE: There is a limit of 10,000 streams per table -- if using more than 10k streams please reach out to Snowflake support.
`+service.OutputPerformanceDocs(true, true)+`

*Despite the general batching advice above, don't set a `+"`batching`"+` policy on this specific output.* Throughput here is governed by batch size, and batch size is capped by the *input's* `+"`max_yield_batch_bytes`"+` field, not by this output's `+"`batching`"+` policy -- nothing set here raises it. Its default, `+"`32KB`"+`, is far below what this connector can carry, so raising it is the primary throughput lever. Setting a `+"`batching`"+` policy on this output does not merely fail to help: with a multi-partition input it pools every partition's yields into one request, serializing throughput across partitions and risking the 4MiB limit below -- so leaving it unset is the recommendation here, not merely the default.

Whatever value is chosen there must stay under this output's 4MiB uncompressed per-request limit, and must not exceed the input's own `+"`partition_buffer_bytes`"+`. A batch over the 4MiB limit is rejected outright rather than split, and because a batch rejected for size is rejected identically on every retry, exceeding the limit wedges that partition indefinitely rather than merely running slowly.
`).
		Fields(
			service.NewStringField(ssopFieldAccount).
				Description(`The Snowflake https://docs.snowflake.com/en/user-guide/admin-account-identifier.html#using-an-account-locator-as-an-identifier[Account name^]. Which should be formatted as `+"`<orgname>-<account_name>`"+` where `+"`<orgname>`"+` is the name of your Snowflake organization and `+"`<account_name>`"+` is the unique name of your account within your organization.
`).
				ShortDescription("The Snowflake account name, formatted as orgname-account_name.").Example("ORG-ACCOUNT"),
			service.NewStringField(ssopFieldURL).
				Description(`Override the URL used to connect to Snowflake for BOTH the control-plane and ingest-plane connections, which is `+"`https://ORG-ACCOUNT.snowflakecomputing.com`"+` by default. Setting this skips Snowflake's normal ingest-hostname discovery entirely, so it is intended for local and test deployments where discovery would resolve to something unreachable, not for production accounts.

This is NOT the field for `+"`PrivateLink`"+` accounts or other non-standard hostnames: use `+"`"+ssopFieldAccountHost+"`"+` for that, which keeps ingest-hostname discovery working. If both `+"`"+ssopFieldURL+"`"+` and `+"`"+ssopFieldAccountHost+"`"+` are set, `+"`"+ssopFieldURL+"`"+` wins.
`).Optional().Example("http://localhost:8080").Advanced(),
			service.NewStringField(ssopFieldAccountHost).
				Description(`Override the control-plane host used to reach Snowflake, without changing how the ingest (data-plane) host is discovered: Snowflake's `+"`/v2/streaming/hostname`"+` discovery still runs normally against this host. Use this for accounts whose hostname doesn't follow the default `+"`<account>.snowflakecomputing.com`"+` pattern derived from `+"`"+ssopFieldAccount+"`"+` -- most commonly https://docs.snowflake.com/en/user-guide/admin-privatelink[PrivateLink^] accounts (`+"`<account>.privatelink.snowflakecomputing.com`"+`), and non-standard deployments such as a QA account.

If `+"`"+ssopFieldURL+"`"+` is also set, `+"`"+ssopFieldURL+"`"+` takes precedence and this field has no effect: `+"`"+ssopFieldURL+"`"+` overrides both planes and skips discovery, which its own description explains is for local/test deployments, not PrivateLink. Leaving this unset preserves the previous behaviour, deriving the control-plane host as `+"`<account>.snowflakecomputing.com`"+`.
`).Optional().Example("my-account.privatelink.snowflakecomputing.com").Advanced(),
			service.NewStringField(ssopFieldUser).Description("The user to run the Snowpipe Stream as. See https://docs.snowflake.com/en/user-guide/admin-user-management[Snowflake Documentation^] on how to create a user.").
				ShortDescription("The user to run the Snowpipe Stream as."),
			service.NewStringField(ssopFieldRole).Description("The role to ingest as. The connector requests a session-scoped token for this role, so Snowflake authorizes every streaming call against it rather than against the `user`'s default role -- setting a least-privilege role here does restrict ingestion. It needs the https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview#required-access-privileges[required privileges^] on the target pipe and table; a role that cannot reach the pipe fails at startup, naming the role. Note that the effective privileges are this role's whole inherited hierarchy.").
				ShortDescription("The role to ingest as; sent as a session-scoped token, so it does constrain ingestion privileges.").Example("SNOWPIPE_STREAMING_ROLE"),
			service.NewStringField(ssopFieldDB).Description("The Snowflake database to ingest data into.").Example("MY_DATABASE"),
			service.NewStringField(ssopFieldSchema).Description("The Snowflake schema to ingest data into.").Example("PUBLIC"),
			service.NewStringField(ssopFieldPipe).Description("The Snowflake pipe to ingest data into. The pipe must already exist -- this output never issues `CREATE PIPE`, so a missing pipe fails at startup rather than at the first message. Unlike `database` and `schema`, which are upper-cased automatically, this value is used exactly as written (it becomes part of the case-sensitive REST path, and a pipe created with a quoted identifier keeps its case), so match the stored identifier: `MY_PIPE` for a pipe created as `CREATE PIPE my_pipe`.").Example("MY_PIPE"),
			service.NewStringField(ssopFieldKey).Description("The PEM encoded private RSA key to use for authenticating with Snowflake. Either this or `private_key_file` must be specified.").
				ShortDescription("PEM encoded private RSA key for authenticating with Snowflake. Either this or private_key_file is required.").Optional().Secret(),
			service.NewStringField(ssopFieldKeyFile).Description("The file to load the private RSA key from. This should be a `.p8` PEM encoded file. Either this or `private_key` must be specified.").
				ShortDescription("File to load the private RSA key from, as a .p8 PEM file. Either this or private_key is required.").Optional(),
			service.NewStringField(ssopFieldKeyPass).Description("The RSA key passphrase if the RSA key is encrypted.").Optional().Secret(),
			service.NewBatchPolicyField(ssopFieldBatching),
			service.NewOutputMaxInFlightField().Default(4),
			service.NewInterpolatedStringField(ssopFieldChannelName).
				Description(`The channel to write through. Duplicate channel names will result in errors and prevent multiple instances of Redpanda Connect from writing at the same time. This interpolation is executed independently for every message in a batch, not just the first: messages are grouped by their resolved channel name (preserving each message's relative order within its group) and each group is appended, deduplicated (when `+"`"+ssopFieldOffsetToken+"`"+` is set) and committed on its own. It's still recommended to batch at the input level so that a batch contains messages for as few channels as possible, since a batch spanning several channels is handled correctly but round-trips to Snowflake once per channel instead of once for the whole batch.

If not set, defaults to a single, constant, unpartitioned channel scoped to this output's pipe: `+"`<database>.<schema>.<pipe>`"+`.

Getting this right matters for exactly-once delivery -- see <<choosing-a-channel-name,Choosing a `+"`"+ssopFieldChannelName+"`"+`>> above for the requirement this field exists to satisfy, the per-partition pattern for Kafka/Redpanda inputs, the always-safe fallback, and what happens if it's violated.`).
				Optional().
				Advanced().
				Examples(`MYDB.PUBLIC.BETS_PIPE-p${!@kafka_partition}`),
			service.NewInterpolatedStringField(ssopFieldOffsetToken).
				Description(`The offset token to use for exactly once delivery of data in the pipeline. When data is sent on a channel, each message in a batch's offset token
is compared numerically to the latest token for a channel when both parse as integers (as Kafka offsets do), and lexicographically otherwise. If the offset token is
not newer than the latest in the channel, it's assumed the message is a duplicate and is dropped. This means it is *very important* to have ordered delivery to the
output, any out of order messages to the output will be seen as duplicates and dropped. Specifically this means that retried messages could be seen as duplicates if
later messages have succeeded in the meantime, so in most circumstances a dead letter queue output should be employed for failed messages.

NOTE: It's assumed that messages within a batch are in increasing order by offset token.

If not set, this output does not deduplicate at all: every row is appended and committed as received. That's genuine
at-least-once delivery, not an error condition -- omit this field entirely for any input that has no natural per-partition
offset to give it. For a Kafka/Redpanda-shaped input that wants exactly-once, `+"`${! @kafka_offset }`"+` (the same value the Kafka
Connector uses) is a common choice, shown as an example below rather than assumed by default; any other input can supply its
own monotonic per-partition value instead, such as a Postgres LSN. Note that leaving this unset scales the usual
at-least-once duplicate risk to every channel a single batch happens to touch, not just one -- a batch spanning several
channels (see `+"`"+ssopFieldChannelName+"`"+`) that partially succeeds before a retry can duplicate rows on each channel that already
landed, since there is no offset token to filter any of them back out on redelivery.

*Don't add or remove this field on a pipeline that has already written to its channels.* A channel's offset tokens have to form one comparable sequence for as long as the channel exists, and the two modes use different sequences: with this field set, the channel carries your tokens; with it unset, this output tags each append with a synthetic token of the form `+"`~rpcn-at-least-once-<digits>`"+` (visible as the channel's `+"`last_committed_offset_token`"+` in Snowflake) purely so it can confirm the commit. A synthetic token compares as newer than any real one, so a channel last written without this field can never accept a real token as new -- every row would be classed as an already-committed duplicate. Rather than silently dropping them, this output refuses the batch with an error that says so, and keeps refusing until the pipeline is given a fresh `+"`"+ssopFieldChannelName+"`"+`. Going the other way (removing this field from a channel that carried real tokens) is checked too and is normally fine; use a fresh `+"`"+ssopFieldChannelName+"`"+` if it isn't.

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
			service.NewBoolField(ssopFieldTolerateRowErrors).
				Description(`Whether to tolerate rows Snowflake rejects after a batch has been appended (a malformed or uncastable field, visible only in the channel's asynchronous status as `+"`rows_error_count > 0`"+` alongside a `+"`channel_status_code`"+` that still reads `+"`SUCCESS`"+`). This is this output's equivalent of the Kafka Connector's `+"`errors.tolerance`"+` setting: `+"`false`"+` (the default) matches the Kafka Connector's own default of `+"`errors.tolerance=none`"+` -- any newly-rejected row fails the batch with an error naming the channel, the new/total/initial error counts, and Snowflake's last error message. `+"`true`"+` matches `+"`errors.tolerance=all`"+`: the same rejection instead only logs a warning and continues. In both settings the `+"`snowflake_rows_error_count`"+` metric is incremented by the newly-rejected count. Rejected rows never reach a dead-letter queue either way -- neither this output nor the Kafka Connector routes server-side (post-append) rejections to one; the Kafka Connector's DLQ is wired only to client-side pre-append validation failures. Note that failing the batch (the `+"`false`"+` default) does not cause the rejected rows themselves to be retried: Snowflake has already permanently rejected them, and a redelivered batch dedupes against the channel's already-advanced offset token to nothing. The failure exists to alert an operator, via the error and the metric, not to drive a retry that couldn't help. For that reason, when `+"`"+ssopFieldOffsetToken+"`"+` is unset (at-least-once mode, no dedup) a rejection never fails the batch even with this set to `+"`false`"+`: a retry would re-insert every good row in the batch as a duplicate and hit the same rejection again, forever. The rejection is warned about and counted in the metric exactly as with `+"`true`"+`.`).
				ShortDescription("Whether to tolerate rows Snowflake rejects (equivalent to the Kafka Connector's `errors.tolerance`); false (default) fails the batch, true only warns.").
				Default(false).
				Advanced(),
		).
		// A second, separate LintRule call here would silently discard this
		// one instead of adding to it -- ConfigSpec.LintRule assigns the
		// mapping string outright (FieldSpec.LinterBlobl), it doesn't
		// compose across calls. All rules for this component must live in
		// this single match block; a mistake here previously left the
		// private_key/private_key_file exclusion dead (overwritten by a
		// second .LintRule() call below it), passing silently on a config
		// that set both.
		LintRule(`root = match {
  this.exists("private_key") && this.exists("private_key_file") => [ "both `+"`private_key`"+` and `+"`private_key_file`"+` can't be set simultaneously" ],
  !this.exists("private_key") && !this.exists("private_key_file") => [ "exactly one of `+"`private_key`"+` or `+"`private_key_file`"+` must be set" ],
}`).
		Example(
			"Simple at-least-once ingestion",
			`The minimal config: no `+"`offset_token`"+` or `+"`channel_name`"+`. Neither is required, and
neither implies anything about the input -- this works the same way whether the input is Kafka-shaped, a
plain file, or (as here) a synthetic generator. Rows land at-least-once, with no dedup on retry, through a
single channel named after the pipe. That's a fine starting point for getting something running; add
`+"`offset_token`"+` later if you need exactly-once, or `+"`channel_name`"+` if you need more than one
channel writing in parallel.`,
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

NOTE: This uses one unpartitioned channel (no `+"`"+ssopFieldChannelName+"`"+` set), which is only safe for exactly-once because `+"`"+"max_in_flight: 1"+"`"+` guarantees at most one writer to it -- see `+"`"+ssopFieldChannelName+"`"+`'s own documentation for why that pairing matters. Removing `+"`"+ssopFieldOffsetToken+"`"+` is a safer option than getting that pairing wrong: it switches Redpanda Connect to at-least-once delivery instead (on a fresh `+"`"+ssopFieldChannelName+"`"+` if the pipeline has already run -- see that field's docs on not switching modes on an existing channel).`,
			`
input:
  postgres_cdc:
    dsn: postgres://foouser:foopass@localhost:5432/foodb
    schema: "public"
    slot_name: "my_repl_slot"
    tables: ["my_pg_table"]
    # We want very large batches - each batch will be sent to Snowflake individually
    # so to optimize query performance we want as big of files as we have memory for.
    # count has no byte-size awareness: 50000 rows can exceed this output's 4MiB
    # uncompressed per-request limit depending on row size, and a batch rejected for
    # size fails identically on every retry. Size count against your actual row size.
    # This example has no fallback (see the Redpanda example below for one) -- with a
    # single channel and no fallback, an oversized batch wedges the whole pipeline, not
    # just one partition among many.
    batching:
      count: 50000
      period: 45s
    # Prevent multiple batches from being in flight at once, so that we never send
    # a batch while another batch is being retried, this is important to ensure that
    # the Snowflake Snowpipe Streaming channel does not see older data - as it will
    # assume that the older data is already committed.
    checkpoint_limit: 1
output:
  snowflake_streaming_pipe:
    # We use the log sequence number in the WAL from Postgres to ensure we
    # only upload data exactly once, these are already lexicographically
    # ordered.
    offset_token: "${!@lsn}"
    # This whole pipeline is one ordered stream feeding one channel (no
    # channel_name set below, so every message shares the single default
    # channel). Setting max_in_flight above 1 would let more than one
    # WriteBatch call race for that same channel concurrently, which can
    # silently drop rows out of order -- see channel_name's own docs for
    # why. 1 is the always-safe choice whenever a single channel has no
    # per-partition split to lean on.
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
			`How to ingest data from Redpanda with consumer groups, decode the schema using the schema registry, then write the corresponding data into Snowflake exactly once, through a pipe (rather than directly into a table) so that any transformation or filtering defined on the pipe's COPY INTO runs before the rows land.

NOTE: This is the pattern to copy for any partitioned Kafka/Redpanda input: `+"`"+ssopFieldChannelName+"`"+` is scoped per partition below, which is what makes exactly-once hold here -- see that field's own documentation for exactly what it protects and why. Removing `+"`"+ssopFieldOffsetToken+"`"+` is a safer option than getting that wrong: it switches Redpanda Connect to at-least-once delivery instead (on a fresh `+"`"+ssopFieldChannelName+"`"+` if the pipeline has already run -- see `+"`"+ssopFieldOffsetToken+"`"+`'s docs on not switching modes on an existing channel).`,
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
        # Each Kafka/Redpanda partition gets its own channel, so a batch
        # from one partition never shares a channel with a batch from
        # another -- see channel_name's own documentation for why that's
        # the requirement this is satisfying, and why a real per-partition-
        # ordered consumer group makes it safe to run this at any
        # max_in_flight (unlike the Postgres example above, which is a
        # single unpartitioned channel and needs max_in_flight: 1 instead).
        channel_name: "MYDB.PUBLIC.BETS_PIPE-p${!@kafka_partition}"
        # Deliberately no batching policy on this output. With a multi-partition input, an
        # output batching policy pools every partition's yields into one HTTP request, and
        # its byte_size trigger is a threshold rather than a hard cap: the shipped batch can
        # exceed it by roughly one more partition's yield, enough to blow past this output's
        # 4MiB limit and wedge a partition permanently. Leaving batching unset here removes
        # that failure class -- batch size is governed solely by the input's own
        # max_yield_batch_bytes (default 32KB). Raise that instead of adding batching here.
        # Tokens that parse as int64 are compared numerically, so no
        # zero-padding is needed for correct ordering. offset_token has no
        # default -- omitting it switches this output to at-least-once
        # delivery instead of exactly-once, so set it explicitly here.
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
    # Batching policy: keep each flush well under this connector's 4MiB
    # per-request limit (streamingv2.Client's request size limit). That cap
    # is on the uncompressed row body, is checked by rejecting the batch
    # outright rather than splitting it, and a request that is rejected for
    # being oversized will be rejected identically on every retry -- with
    # max_in_flight left at its default of 1, that wedges the pipeline
    # indefinitely rather than merely failing once. 2MiB gives real headroom.
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
	// v2.Config splits the old single `url` override into BaseURL (used
	// verbatim for both planes, skipping ingest-host discovery) and
	// AccountHost (control-plane host only; ingest-host discovery still
	// runs normally). Both are populated independently below: accountBase()
	// prefers BaseURL when set, so if the user sets both `url` and
	// `account_host`, `url` wins without any special-casing here.
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
	// offsetToken stays nil when offset_token isn't configured, exactly like
	// the v1 snowflake_streaming output's own offsetToken field. WriteBatch
	// treats a nil offsetToken as "skip dedup/ordering, deliver
	// at-least-once" rather than an error -- see the offset_token field's
	// Description above. Never substitute a synthetic/counter-derived token
	// here: a counter resets on restart and would make the dedup guard
	// silently drop rows already committed under a higher token.

	commitTimeout, err := conf.FieldDuration(ssopFieldCommitTimeout)
	if err != nil {
		return nil, err
	}

	tolerateRowErrors, err := conf.FieldBool(ssopFieldTolerateRowErrors)
	if err != nil {
		return nil, err
	}

	// Normalize role, db and schema as they are case-sensitive in the API calls.
	// Maybe we should use the golang SQL driver for SQL statements so we don't have
	// to handle this, instead of the REST API directly.
	role = strings.ToUpper(role)
	db = strings.ToUpper(db)
	schema = strings.ToUpper(schema)

	if channelName == nil {
		// No channel_name configured: fall back to a single, constant,
		// unpartitioned channel for the whole output. This output has no
		// pooled/unordered mode to fall back to, so this is the only default
		// that works regardless of what's feeding this output.
		// Partition-aware parallelism is opt-in via channel_name; see its
		// own doc comment for the pattern.
		expr := defaultChannelName(db, schema, pipe)
		channelName, err = service.NewInterpolatedString(expr)
		if err != nil {
			return nil, fmt.Errorf("internal error building default %s template: %w", ssopFieldChannelName, err)
		}
	}

	client, err := v2.NewClient(v2.Config{
		Account:     account,
		AccountHost: accountHost,
		BaseURL:     baseURL,
		User:        user,
		Role:        role,
		PrivateKey:  rsaKey,
		Database:    db,
		Schema:      schema,
		Pipe:        pipe,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create streaming client: %w", err)
	}
	if err := client.Connect(context.Background()); err != nil {
		return nil, fmt.Errorf("unable to connect streaming client: %w", err)
	}

	indexed := &snowpipeIndexedOutputPipe{
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

	openChannelFn func(ctx context.Context, name string) (v2.IngestChannel, error)

	db, schema string
	// offsetToken is nil when offset_token isn't configured. writeChannelGroup
	// treats that as "skip dedup/ordering, deliver at-least-once" rather than
	// an error -- see the offset_token field's Description.
	offsetToken, channelName *service.InterpolatedString
	commitTimeout            time.Duration
	tolerateRowErrors        bool
	logger                   *service.Logger
	metrics                  *snowpipePipeMetrics

	// noDedupCommitToken hands WaitUntilCommitted something to poll for when
	// offsetToken is nil, so it can still tell "this specific append landed"
	// from "some earlier append on this channel already landed". Reusing a
	// fixed value (or the empty string) on every call can't make that
	// distinction: once any commit carrying that value lands, every later
	// call would see it immediately and return before its own append ever
	// committed. This counter is process-local and shared across every
	// channel this output writes to (global uniqueness is simplest to
	// reason about, and no less correct than scoping it per channel).
	//
	// Seeded lazily from time.Now().UnixNano() on first use (see
	// nextSyntheticCommitToken), not zero: the channel name is stable across
	// restarts by default, so Snowflake's own committed token for it carries
	// over from whatever a previous process run left it at, and
	// WaitUntilCommitted's success check is "committed >= mine". A counter
	// restarting at 1 would have every batch in a fresh run whose token
	// falls at or below the previous run's last committed value succeed
	// immediately, without that batch's own append having been confirmed at
	// all -- silently downgrading "at least once" to "hope it landed" for
	// however many batches it takes the new sequence to climb back past the
	// old one. Wall-clock nanoseconds are large enough that a fresh run's
	// seed is essentially guaranteed to already be past anything a previous
	// run could have counted up to.
	//
	// The value is rendered via v2.FormatSyntheticToken rather than as a
	// bare integer so the token is recognisable as synthetic when it later
	// shows up as a channel's last committed token: a channel written this
	// way has a token space that is incompatible with any real offset_token
	// expression (every real token would compare as already committed), and
	// both checkSyntheticTokenSpace and preprocessForExactlyOncePipe rely on
	// being able to tell the two apart to refuse that switch loudly instead
	// of silently dropping every row.
	noDedupCommitToken atomic.Int64
	syntheticSeedOnce  sync.Once

	// checkedTokenSpaces records the channel names checkSyntheticTokenSpace
	// has already vetted in this process, so the one control-plane status
	// call it costs happens once per channel per process rather than once
	// per batch. Guarded by checkedTokenSpacesMu.
	checkedTokenSpaces   map[string]struct{}
	checkedTokenSpacesMu sync.Mutex

	// submittedWatermarks tracks, per channel name, the highest
	// endOffsetToken any writeChannelGroup call in this process has itself
	// already attempted to append -- see checkSubmissionOrder's doc comment
	// for the class of silent data loss this catches that the channel's own
	// committed token (queried fresh from Snowflake on every call) can't:
	// that value reflects whoever committed most recently, with no way to
	// tell "this is my own batch landing a second time" apart from "a
	// different, logically later batch already landed before mine got its
	// turn". This map answers that question locally instead, since only
	// this process's own submission history can.
	submittedWatermarks   map[string]string
	submittedWatermarksMu sync.Mutex
}

// checkSubmissionOrder guards the invariant every offset_token-bearing
// channel depends on -- see channel_name's own Description -- that at most
// one source, in increasing-token order, is ever writing to a given channel
// at a time. It is not a fairness mechanism and cannot make a genuinely
// disordered writer (misconfigured max_in_flight sharing one channel across
// partitions, or any other source that violates that invariant) safe: two
// truly independent, unordered sources have no shared token order for this
// check to enforce in the first place. What it does do is turn the
// resulting failure from silent -- rows dropped by the dedup filter below
// as if they were an ordinary already-committed retry, with no error, no
// warning, no metric -- into an immediate, attributable error the first
// time it happens, rather than passing a real bug or misconfiguration off
// as successful at-least-once delivery.
//
// That error is not transient: the watermark only ever advances, so a batch
// that lost the race can never win a later one by retrying with the same
// content -- it will fail this check again every time, for as long as the
// same concurrent writers keep advancing the watermark past it. An input
// that retries nacked batches forever with no dead-letter path will retry
// this one forever too, uselessly. See the error text below, which says so
// explicitly rather than reading like an ordinary retryable failure.
//
// end must be a token this call is *about* to submit (called once per
// writeChannelGroup invocation, before AppendRows), not one already
// committed elsewhere -- Acquire/Release's mutual exclusion on the channel
// object means no two calls for the same name run this concurrently, so
// there is no separate locking concern beyond protecting the map itself.
func (o *snowpipeIndexedOutputPipe) checkSubmissionOrder(channelName, start, end string) error {
	o.submittedWatermarksMu.Lock()
	defer o.submittedWatermarksMu.Unlock()
	if o.submittedWatermarks == nil {
		// Constructed directly (as several unit tests do) rather than via
		// newSnowflakeStreamerPipe, which would otherwise have initialized
		// this.
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

// seedSyntheticTokens performs noDedupCommitToken's one-time wall-clock
// seeding (see its doc comment). Lazy rather than done in
// newSnowflakeStreamerPipe so that an output constructed directly -- as the
// unit tests do -- gets exactly the same restart-safe behaviour as one built
// from config, instead of silently starting its sequence at zero.
func (o *snowpipeIndexedOutputPipe) seedSyntheticTokens() {
	o.syntheticSeedOnce.Do(func() {
		o.noDedupCommitToken.Store(time.Now().UnixNano())
	})
}

// nextSyntheticCommitToken mints the per-call commit token writeChannelGroup
// uses when offset_token is unset. See noDedupCommitToken.
func (o *snowpipeIndexedOutputPipe) nextSyntheticCommitToken() string {
	o.seedSyntheticTokens()
	return v2.FormatSyntheticToken(o.noDedupCommitToken.Add(1))
}

// checkSyntheticTokenSpace guards the at-least-once (offset_token unset) side
// of the same invariant preprocessForExactlyOncePipe guards on the
// exactly-once side: a channel's offset tokens must form one comparable
// sequence for the life of the channel, and this output's two modes use
// different sequences (the operator's own tokens vs. synthetic ones from
// nextSyntheticCommitToken). Switching an existing channel from real tokens
// to synthetic ones is normally safe -- a synthetic token is built to compare
// above any realistic real token (see v2.FormatSyntheticToken), so the first
// synthetic commit wait genuinely waits for its own append rather than
// returning early on the older committed value. This check exists for the
// exotic remainder: a last committed real token that nonetheless compares at
// or above the synthetic token about to be used, which would let every
// commit wait on this channel succeed before its append was confirmed. In
// that case it fails the batch with an error naming the token and the fix
// (a fresh channel_name), rather than acking unconfirmed writes.
//
// Runs once per channel name per process: the pool's Acquire/Release hands
// out a channel exclusively, so after the first synthetic commit lands on a
// channel every later committed token on it is one of ours, and the question
// is settled. A transient failure fetching the token is returned as-is
// without marking the channel checked, so the next batch re-asks.
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
		// A previous run of this output left a synthetic token committed.
		// Don't rely on this host's wall clock alone to have moved past it
		// (a restored VM snapshot or a badly unsynced failover node can be
		// behind): bump the counter to at least that value so this run's
		// first token is strictly newer and the commit wait genuinely waits.
		// A plain CAS loop: another channel's first check may be racing to
		// do the same with a different value, and the maximum must win.
		for {
			cur := o.noDedupCommitToken.Load()
			if cur >= prev || o.noDedupCommitToken.CompareAndSwap(cur, prev) {
				break
			}
		}
	} else if latest != "" {
		// Peek at the next token without consuming it: Add(0) reads the
		// current value, and the token actually minted for this batch will
		// be strictly greater.
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

// logReopenFailure warns that reopening name failed after an AppendRows or
// WaitUntilCommitted error, distinguishing the one reopenErr worth calling
// out specifically: ERR_CHANNEL_HAS_UNCOMMITTED_DATA means the channel still
// has appended-but-uncommitted data from before the failure, which is why
// OpenChannel refuses to force past it rather than risk racing the pending
// commit (see IsUncommittedDataConflict's doc comment) -- an operator
// reading this log needs to know that's the specific reason reopening
// didn't happen, not just that it didn't.
func (o *snowpipeIndexedOutputPipe) logReopenFailure(name string, reopenErr error) {
	if v2.IsUncommittedDataConflict(reopenErr) {
		o.logger.Warnf("unable to reopen channel %q: it still has uncommitted data from before the failure, so Snowflake refused the reopen rather than risk racing the pending commit: %v", name, reopenErr)
		return
	}
	o.logger.Warnf("unable to reopen channel %q after failure: %v", name, reopenErr)
}

// reopenAfterFailure is the shared recovery path for an AppendRows or
// WaitUntilCommitted error: report any rows Snowflake rejected on the way to
// the failure, then replace the (possibly poisoned) channel in the pool with
// a freshly opened one, falling back to putting the old one back if the
// reopen itself fails.
//
// The rejected-row report has to happen here, before the reopen, or it
// never happens at all: OpenChannel snapshots Snowflake's cumulative
// rows_error_count as the new channel's baseline (see
// streamingv2.Channel.RowsRejected), which folds any rejection from the
// batch that just failed into "pre-existing", and on redelivery an
// exactly-once batch that had in fact committed dedupes to nothing and
// returns before the post-commit RowsRejected check runs. Without this, a
// malformed row whose batch also hit a commit-poll error would be neither
// metered nor logged above debug level. The batch is failing regardless, so
// this always logs and counts rather than branching on tolerate_row_errors.
func (o *snowpipeIndexedOutputPipe) reopenAfterFailure(ctx context.Context, channel v2.IngestChannel) {
	if rejected := channel.RowsRejected(); rejected > 0 {
		o.metrics.ReportRowsRejected(rejected)
		o.logger.Warnf("channel %q has %d new row error(s) (total: %d) recorded before this batch failed. Last error message: %s",
			channel.Name(), rejected, channel.RowsErrorCount(), channel.LastErrorMessage())
	}
	reopened, reopenErr := o.openChannel(ctx, channel.Name())
	if reopenErr == nil {
		// Releasing reopened under channel.Name() (not reopened.Name())
		// relies on the same pool-key invariant noted at Acquire in
		// writeChannelGroup.
		o.channelPool.Release(channel.Name(), reopened)
		return
	}
	o.logReopenFailure(channel.Name(), reopenErr)
	// Keep the same channel around so a later batch retries opening.
	o.channelPool.Release(channel.Name(), channel)
}

func (*snowpipeIndexedOutputPipe) Connect(context.Context) error {
	return nil
}

// channelGroup is one channel's share of a WriteBatch call's messages: the
// resolved channel name, plus those messages in their original relative
// order.
type channelGroup struct {
	name  string
	batch service.MessageBatch
}

// groupMessagesByChannel resolves exec (built from the output's channel_name
// mapping) against every message in batch and buckets them into
// channelGroups keyed by resolved channel name -- rather than resolving the
// channel name from message 0 only and applying it to the whole batch, which
// silently mishandles a batch spanning more than one channel. Groups are
// returned in first-appearance order; each group preserves the relative
// order of its own messages from batch, since preprocessForExactlyOncePipe's
// dedup/ordering pass assumes increasing offset-token order within a
// channel.
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
			// Returning on the first group's error, rather than continuing
			// on to the rest, is correct here and not just simpler: Benthos
			// treats a WriteBatch error as a whole-batch nack/retry, so on
			// retry every group -- including ones that already succeeded --
			// flows through writeChannelGroup again. A group that already
			// committed either dedupes back down to nothing (offset_token
			// set: the offset-token filter drops rows at or below the
			// channel's already-advanced committed token) or is safely
			// re-sent (offset_token unset: the same at-least-once tradeoff a
			// single-channel batch already had, now correctly scoped
			// per-channel instead of accidentally applied to the whole
			// batch). No new failure mode is introduced by grouping, so
			// aggregating the remaining groups' errors here would not
			// change what the retry does with them.
			return err
		}
	}
	return nil
}

// writeChannelGroup runs the append/commit cycle for one channel's share of
// a WriteBatch call: acquire the channel, optionally dedup/order via
// preprocessForExactlyOncePipe, append the rows, wait for commit, report
// rejected-row metrics, then release the channel back to the pool.
func (o *snowpipeIndexedOutputPipe) writeChannelGroup(ctx context.Context, group channelGroup) error {
	// Acquire keys the pool by group.name; every Release below keys by
	// channel.Name() instead. These only agree because v2.Client.OpenChannel
	// always constructs Channel.name == the name it was given, so keep that
	// invariant in mind before changing either side of this pairing.
	channel, err := o.channelPool.Acquire(ctx, group.name)
	if err != nil {
		return fmt.Errorf("unable to open snowflake streaming channel: %w", err)
	}
	batch := group.batch
	var startOffsetToken, endOffsetToken string
	if o.offsetToken != nil {
		// origFirst/origLast are this call's own token range, before
		// preprocessForExactlyOncePipe's dedup filter runs -- checked and
		// recorded against this process's own submission history first, so
		// an ordering violation is caught even when the filter below would
		// otherwise treat the whole group as an unremarkable already-landed
		// duplicate. See checkSubmissionOrder's own doc comment.
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
		// No offsetToken means no dedup pass, but WaitUntilCommitted below
		// still needs a non-empty, call-unique token to poll for -- see
		// noDedupCommitToken's doc comment on why a fixed value (including
		// empty) can't drive that wait correctly -- and the channel's
		// existing token space has to be one that token can be waited on
		// against (see checkSyntheticTokenSpace).
		if err = o.checkSyntheticTokenSpace(ctx, channel); err != nil {
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		token := o.nextSyntheticCommitToken()
		startOffsetToken, endOffsetToken = token, token
	}
	// When o.offsetToken is nil (offset_token isn't configured), dedup and
	// ordering are skipped entirely: every row in the group is appended as
	// received, tagged only with the synthetic per-call token generated
	// above. That's genuine at-least-once delivery, not an error -- see the
	// offset_token field's Description.
	o.logger.Debugf("inserting rows using channel %s at offset tokens: start=%s end=%s", channel.Name(), startOffsetToken, endOffsetToken)
	rows := make([][]byte, len(batch))
	// emptyRows counts messages whose body is empty or whitespace-only after
	// trimming -- Kafka/Redpanda tombstones (null values) being the usual
	// source. A Snowflake row has no representation for "no value", so
	// AppendRows (via EncodeRows) drops them from the request, the same way
	// the Kafka Connector's default behavior.on.null.values=IGNORE does.
	// Counted here, before the append, so the drop is at least visible in a
	// metric and a debug line rather than happening silently inside the
	// client.
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
			// Backpressure (HTTP 429/503, or one of the Kafka Connector's
			// four retryable SFException codes) does not invalidate the
			// channel -- unlike other append failures, the channel remains
			// open and usable, so it goes back to the pool unchanged rather
			// than being reopened. Reopening here would be pure overhead on
			// the control plane precisely when it's already saturated.
			o.channelPool.Release(channel.Name(), channel)
			return err
		}
		o.reopenAfterFailure(ctx, channel)
		return err
	}
	appendDuration := time.Since(appendStart)
	if !sent {
		// Every row in this group encoded to nothing (empty or
		// whitespace-only after trimming -- see EncodeRows), most commonly a
		// batch of Kafka/Redpanda tombstones. No request was made, so
		// endOffsetToken was never sent either: waiting for Snowflake to
		// commit a token it was never given would block for the full
		// commit_timeout on every attempt, forever, since the same rows
		// re-encode to nothing on every redelivery too. There is nothing
		// left to do for this group -- release the channel and report
		// success, matching how preprocessForExactlyOncePipe filtering a
		// group down to nothing already does above. (The rows themselves
		// were already counted and logged as skipped above.)
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
	// rejected is the delta since the last time this channel reported a
	// positive delta (see streamingv2.IngestChannel.RowsRejected), i.e. rows
	// Snowflake counted as rejected because of (or since) this batch.
	// RowsErrorCount is the channel's absolute lifetime total, read after
	// RowsRejected so "total" and "initial" (total minus this delta) line
	// up with the delta just reported -- mirroring the Kafka Connector's
	// SnowpipeStreamingPartitionChannel, which snapshots initialErrorCount
	// at open and advances it the same way, before branching on
	// errors.tolerance.
	if rejected := channel.RowsRejected(); rejected > 0 {
		total := channel.RowsErrorCount()
		initial := total - rejected
		// o.metrics.ReportRowsRejected fires unconditionally here, before the
		// tolerate/fail branch below, so the snowflake_rows_error_count
		// metric is incremented by the same delta in both modes.
		o.metrics.ReportRowsRejected(rejected)
		msg := fmt.Sprintf("channel %q has %d new errors (total: %d, initial: %d). Last error message: %s",
			channel.Name(), rejected, total, initial, channel.LastErrorMessage())
		switch {
		case o.tolerateRowErrors:
			o.logger.Warnf("%s", msg)
		case o.offsetToken == nil:
			// tolerate_row_errors is false, but with offset_token unset there
			// is no dedup pass, so failing the batch could not do what the
			// failure exists for. Snowflake has already permanently rejected
			// the bad rows; the redelivered batch would re-append every GOOD
			// row as a duplicate under a fresh synthetic token, the same bad
			// rows would be rejected again, and the batch would fail again
			// -- an unbounded loop inserting duplicates on every iteration.
			// Warn (and count, above) instead, and say why the batch is not
			// being failed so the tolerate_row_errors: false setting doesn't
			// look ignored.
			o.logger.Warnf("%s -- not failing the batch despite %s: false, because with %s unset a retry would only re-insert the batch's good rows as duplicates and be rejected again",
				msg, ssopFieldTolerateRowErrors, ssopFieldOffsetToken)
		default:
			// The channel itself is still healthy (no append/commit error),
			// so release it for reuse rather than forcing a reopen -- only
			// this batch fails.
			o.channelPool.Release(channel.Name(), channel)
			return fmt.Errorf("%s", msg)
		}
	} else if total := channel.RowsErrorCount(); total > 0 {
		// Pre-existing errors this batch didn't add to: the Kafka
		// Connector's equivalent branch (currentErrorCount > 0 but no new
		// errors) logs at debug only, never warn/error, since a channel
		// reopened after an earlier rejection would otherwise refuse to
		// start (or every later batch would otherwise re-warn on) a
		// historical error nothing in this batch caused.
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

// preprocessForExactlyOncePipe filters batch down to messages whose
// interpolated offset token is newer than the channel's latest committed
// offset token, and returns the raw offset tokens of the first and last
// message in the (possibly filtered) batch, for the caller to pass to
// AppendRows / WaitUntilCommitted.
//
// Tokens are compared with v2.CompareOffsetTokens, not a plain string
// comparison: Kafka offsets are decimal integers, and "10" < "9"
// lexicographically, so a string comparison silently reintroduces
// duplicates the moment a partition's offset reaches double digits.
//
// It's assumed the batch is already in increasing offset-token order (see
// the offset_token field's ordering note); an out-of-order batch will have
// its later, lower-offset rows dropped as if they were duplicates.
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
		// This channel was last written by this output running WITHOUT
		// offset_token (see nextSyntheticCommitToken), and a synthetic token
		// compares above every real one (v2.FormatSyntheticToken). Left
		// unchecked, the per-row filter below would classify every message
		// in every batch as an already-committed duplicate and drop it --
		// with the batch acked as a success. Refuse instead. Checked on
		// every call rather than once per channel because it's free: latest
		// is already in hand.
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

// requireOffsetToken interpolates the offset_token expression for message i
// and fails loudly if it resolves to an empty string, rather than letting an
// empty token flow into the comparisons above. An empty token is exactly
// the sentinel channel.LatestOffsetToken uses for "nothing committed yet",
// so silently accepting one would make a real row indistinguishable from
// that sentinel. Only called when offset_token is explicitly configured
// (see writeChannelGroup): there's no default expression to collide with
// here anymore, but the check stays unconditional regardless, since the
// collision is the same problem for any offset_token value the operator
// writes. Never substitute a synthetic or counter-derived token here: that
// would silently disable exactly-once instead of failing loudly -- contrast
// noDedupCommitToken, which only ever runs when the operator has not
// configured offset_token at all, and so is never a substitute for a token
// this function was expecting to validate.
//
// A missing metadata reference (e.g. @kafka_offset on a non-Kafka input,
// still a common expression to reach for since it's shown in this field's
// docs as one example) does not interpolate to "": Bloblang stringifies a
// null query result as the literal string "null" (value.IToBytes/IToString
// in the benthos SDK), so that case is checked for explicitly too -- an
// unconditional "" check alone would let every row silently share the token
// "null" instead of failing.
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
