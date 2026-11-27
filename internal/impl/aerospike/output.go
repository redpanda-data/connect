// Copyright 2026 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aerospike

import (
	"fmt"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	aoFieldOperation            = "operation"
	aoFieldTombstoneAsDelete    = "tombstone_as_delete"
	aoFieldCoerceIntegralFloats = "coerce_integral_floats"
	aoFieldTTL                  = "ttl"
	aoFieldGeneration           = "generation"
	aoFieldSendKey              = "send_key"
	aoFieldDurableDelete        = "durable_delete"
	aoFieldCoalesceBatchKeys    = "coalesce_batch_keys"
	aoFieldMaxRecordBytes       = "max_record_bytes"
	aoFieldIgnoreErrorCodes     = "ignore_error_codes"
	aoFieldFencing              = "fencing"
	aoFieldFencingEnabled       = "enabled"
	aoFieldFencingBin           = "bin"
	aoFieldFencingValue         = "value"
	aoFieldFencingTombstoneBin  = "tombstone_bin"
	aoFieldFencingTombstoneTTL  = "tombstone_ttl"
	aoFieldBatching             = "batching"
)

// retryableIgnoreLint rejects ignore_error_codes entries that a retry could
// still write. The same set is enforced in parseOutputConfig.
func retryableIgnoreLint() string {
	codes := make([]string, 0, len(retryableResultCodes))
	for code := range retryableResultCodes {
		codes = append(codes, strconv.Itoa(int(code)))
	}
	// The linter runs this mapping on the list and again on each element.
	return `root = if this.type() == "number" {
  if [` + strings.Join(codes, ", ") + `].contains(this) {
    [ "code %v is retryable or not an error and must not be listed".format(this) ]
  } else { [] }
} else { [] }`
}

func outputSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Version("4.120.0").
		Categories("Services").
		Summary("Writes messages to an Aerospike cluster as records, batched and keyed by an interpolated primary key or digest.").
		Description(`
Each message is mapped to a single Aerospike record. The record is addressed by
` + "`namespace`" + `, ` + "`set`" + `, and either ` + "`key`" + ` or ` + "`digest`" + `.
` + "`key`" + ` is a user key, which Aerospike hashes. ` + "`digest`" + ` is the 20-byte
digest itself and the stored user key is null. The bins are produced by the
` + "`bins`" + ` Bloblang mapping.

### Shaping data

This output deliberately has no transform language of its own — use Bloblang, either in a
` + "`mapping`" + ` processor upstream or in the ` + "`bins`" + ` field. Aerospike bin names are
capped at 15 characters, so any source field with a longer name must be renamed in the
mapping rather than passed through.

### Record model

This output writes whole bins (` + "`PutOp`" + `). It will not append to a list, increment a
counter, or patch a map in place — those need a modeled operate in application code.
Treat ` + "`bins`" + ` as a schema contract the server will not enforce: keep bin names and types
stable, and cap nested lists and maps in Bloblang (` + "`this.events.slice(0, 100)`" + `). Aerospike
rewrites the entire record on every update, so the bulk of records should stay in
single-digit KiB. Sets are a name, not a shard key. Do not interpolate an unbounded
value. A small fixed list of topic names is fine. Readers, including ` + "`aerospike_lookup`" + `, address
records by primary key, so do not rely on the data being queryable as a join.

### Batching and hot keys

Writes are dispatched with a single batch command per output batch. Configure ` + "`batching`" + `,
otherwise every message is its own command. Aerospike enforces
concurrency limits per record and does not apply the entries of one batch command in order,
so messages for the same key inside one batch are coalesced into one write where that gives
the same result as writing them in order (see ` + "`coalesce_batch_keys`" + `). A message that
cannot be folded is sent in a later batch command, after the earlier one for its key completes.
When the earlier one fails, the later messages for that key fail without being sent, so they
cannot land before its retry.

Coalescing and that ordering apply **within a single batch only**. ` + "`max_in_flight`" + `
defaults to ` + "`1`" + `, so batches are written one at a time and writes to one key land in stream
order. A higher value dispatches batches concurrently, so two batches in flight can carry the
same record key, which reorders those two writes and can leave an older value in the record.
Raise it only when keys do not repeat across batches, and prefer larger batches to recover
throughput.

### Delivery guarantees

Redpanda Connect delivers at least once, so a message may be written more than once after a
retry. Full-record writes are naturally idempotent. If your mapping is not — or if you want
redelivery of stale data to be rejected outright — enable ` + "`fencing`" + `, which stores a
monotonic value alongside the record and guards each write with a filter expression
evaluated on the server.

A resolved namespace that is not declared on the cluster is rejected with code 20
(` + "`INVALID_NAMESPACE`" + `) for that message only. The other messages in the batch are still
written. The rejection does not go away on retry, so with ` + "`auto_replay_nacks`" + ` left on,
that message is replayed forever and holds the source partition. Wrap this output in
` + "`output.fallback`" + ` so the rejected message is sent somewhere else instead of being retried.

### What this output does not do

Writes are whole-bin puts. Atomic increments, list append, and map operations are not
available — shape the record in Bloblang and write the full bin values. There is no input;
Aerospike has no client-side change feed.
` + service.OutputPerformanceDocs(true, true))

	spec = spec.Fields(clientFields()...)
	spec = spec.Fields(outputKeyFields(`${! json("user_id") }`, `${! meta("kafka_key") }`)...)

	spec = spec.Fields(
		service.NewInterpolatedStringField(aoFieldOperation).
			Description(`The write operation to perform. One of:

- `+"`write`"+` — create or update, merging these bins with any existing bins.
- `+"`replace`"+` — create or replace, deleting existing bins not named here. A null bin is left out of the write. Without fencing, a replace whose every bin is null is rejected on that message.
- `+"`replace_only`"+` — fail if the record does not exist, otherwise replace, deleting existing bins not named here. A null bin is left out of the write. Without fencing, a replace_only whose every bin is null is rejected on that message.
- `+"`create_only`"+` — fail if the record already exists.
- `+"`update_only`"+` — fail if the record does not exist.
- `+"`delete`"+` — delete the record.

Interpolation allows routing per message, for example from a CDC operation field. For a compact topic or CDC stream that sends the full record each time, use `+"`replace`"+` so leftover bins cannot accumulate. `+"`replace_only`"+` on a missing key returns `+"`KEY_NOT_FOUND`"+` (code `+"`2`"+`). That rejection does not go away on retry, so with `+"`auto_replay_nacks`"+` left on the message is replayed forever and holds the source partition until some other write creates the record. List `+"`2`"+` in `+"`ignore_error_codes`"+` to drop it, or wrap this output in `+"`output.fallback`"+` to send it elsewhere. Listing `+"`2`"+` acknowledges the message, so fallback does not see it.

A `+"`create_only`"+` or `+"`generation`"+` write whose result was in doubt may have been applied on the server. Its retry then fails with code `+"`5`"+` (`+"`KEY_EXISTS`"+`) or `+"`3`"+` (`+"`GENERATION_ERROR`"+`) on every attempt, which holds the source partition the same way. Wrap this output in `+"`output.fallback`"+` so such a message is sent elsewhere for inspection.`).
			Default("write").
			Example("replace").
			Example(`${! if json("op") == "d" { "delete" } else { "write" } }`),

		service.NewBoolField(aoFieldTombstoneAsDelete).
			Description("Treat a message with an empty or `null` payload as a delete, matching the Kafka tombstone convention. Applied before `operation` is evaluated. Disable this for HTTP, file, or other sources where an empty body is not a delete.").
			Default(true),

		service.NewBloblangField(aoFieldBins).
			Description("A Bloblang mapping producing an object whose keys are bin names and whose values are bin values. Bin names must be 1-15 characters. Nested objects and arrays become Aerospike maps and lists. Bound collection size here — this output cannot append or trim on the server, and an unbounded list makes every subsequent write rewrite a larger record.\n\n"+
				"A `null` value depends on the operation:\n\n"+
				"- `write` and `update_only` delete that bin from the record.\n"+
				"- `replace` and `replace_only` leave that bin out of the write, because those operations already delete bins they do not name.\n"+
				"- A `replace` or `replace_only` whose every value is null is rejected on that message while the batch is planned, before coalescing, so an earlier write of the same key still lands. That rejection does not clear on retry, and `ignore_error_codes` does not cover it, so it holds the source partition until the mapping changes.\n"+
				"- With fencing on, that message is not rejected, because the fence bin is still written. The record is replaced by one holding only the fence bin, with no tombstone bin, so readers see an existing record with no data bins. Map a delete instead if the record should be removed.\n\n"+
				"When coalescing folds a later message that nulls every bin into an earlier message for the same key, the folded command is a `delete`:\n\n"+
				"- After a `replace`, a `write` or `update_only` produces the record those messages would leave, and a missing key is already that end state.\n"+
				"- After a `replace_only`, an `update_only` keeps the existence check, so the delete fails if the record is missing.").
			Default("root = this").
			Example(`root = this.without("user_id")`).
			Example(`root.name = this.full_name
root.email = this.email_address
root.updated = this.updated_at_epoch_seconds
root.events = this.events.slice(0, 50)`),

		service.NewBoolField(aoFieldCoerceIntegralFloats).
			Description("Store JSON numbers with no fractional part as Aerospike integers rather than doubles. JSON has a single number type, so without this an identifier or counter arrives as a double, which breaks integer comparisons, `add` operations and integer secondary indexes.\n\nThe trade-off is that the stored type then depends on the value: a `price` bin holding `10.0` is written as an integer while `10.5` is written as a double. Aerospike types bins per record rather than per set, so this raises no error, but an integer secondary index over such a bin only indexes the records that happened to be integral. Disable this for bins that are genuinely floating point, or coerce the type explicitly in the `bins` mapping.").
			Default(true).
			Advanced(),

		service.NewInterpolatedStringField(aoFieldTTL).
			Description(`Record time-to-live. Accepts a duration such as `+"`24h`"+`, unit spellings (`+"`24H`"+`, `+"`1D`"+`, `+"`5M`"+`, `+"`60S`"+`, or a bare number of seconds), or:

- `+"`0s`"+` / `+"`0`"+` — use the namespace `+"`default-ttl`"+`. Every write with this value re-bases void-time to that default.
- `+"`never`"+` / `+"`-1`"+` — never expire.
- `+"`keep`"+` / `+"`-2`"+` — leave the existing expiration untouched on update. On create, the namespace default applies. An interpolation that resolves to an empty string also means `+"`keep`"+`.

Units S/M/H/D are case-insensitive, so a payload such as `+"`\"ttl\": \"24H\"`"+` can be interpolated with `+"`${! json(\"ttl\") }`"+` without rewriting producers. Go durations (`+"`24h`"+`, `+"`90s`"+`) still work.

The default is `+"`keep`"+` so a stream of updates does not reset or shorten void-time. Shortening TTL on an existing record can contribute to resurrection after a cold start. A positive TTL requires `+"`nsup-period`"+` greater than 0 on the target namespace, otherwise the server rejects the write and nothing ever expires.

With `+"`fencing.enabled`"+`, `+"`keep`"+` is rejected, at startup when the value is static and on the message when it is interpolated. A write after a fenced delete would keep the expiration of the tombstone (`+"`fencing.tombstone_ttl`"+`), so the record would expire with it. Set `+"`0`"+` for the namespace default, `+"`never`"+`, or a duration.`).
			Default("keep").
			Example("24h").
			Example("24H").
			Example("never").
			Example("keep"),

		service.NewInterpolatedStringField(aoFieldGeneration).
			Description("Expected record generation for a compare-and-set write. When set, the write fails if the stored generation does not match. Empty disables the check. The value is the generation the producer read the record at. Pair with `aerospike_lookup` `emit_metadata`, which sets `aerospike_generation` on the looked-up message.\n\nA compare-and-set message is never coalesced, even with `coalesce_batch_keys` enabled: the check refers to the specific record that was read, and one command can carry only one expected generation. If several messages in a batch target the same key and any of them carries a generation, each is sent as its own command, one after another in message order, and a failed one fails the messages after it for that key. On `GENERATION_ERROR` the correct recovery is to re-read the record and redo the change, not to retry the same write.").
			Default("").
			Example(`${! meta("aerospike_generation") }`).
			Advanced(),

		service.NewBoolField(aoFieldSendKey).
			Description("Store the primary key alongside the record, so reads can return it. Aerospike stores only a digest by default, meaning the original key is not recoverable from the record.").
			Default(false),

		service.NewBoolField(aoFieldDurableDelete).
			Description("Write a tombstone on delete so deleted records cannot reappear after a cold restart. Enterprise Edition only.").
			Default(false),

		service.NewBoolField(aoFieldCoalesceBatchKeys).
			Description("Collapse multiple messages for the same record key within a batch into a single write. Strongly recommended: sending them separately contends on one record, which surfaces as `KEY_BUSY` or as latency inflation with no error at all.\n\n"+
				"Two messages fold only when one command leaves the same record and fails the same messages as writing them in order, whether the key exists or not. Otherwise the later message is sent in a later batch command, after the earlier one completes. Disabling this sends every message on its own, in that order.\n\n"+
				"- A `write`, `replace`, or `delete` folds into an earlier `write`, `replace`, or `delete`. After a `delete`, a `write` becomes a `replace`, so bins the delete removed do not come back.\n"+
				"- An `update_only` or `replace_only` folds into an earlier `write` or `replace`, which leaves the record in place, so the existence check is dropped. It also folds into an earlier `update_only` or `replace_only`, and the folded command keeps the check.\n"+
				"- A `create_only` folds only into an earlier `delete`, and becomes a `replace`.\n"+
				"- Nothing folds into a `create_only`, and a `write`, `replace`, or `delete` does not fold into an `update_only` or `replace_only`, because one command cannot fail one message and apply the other.\n"+
				"- A later `ttl` of `keep` leaves the expiration of the earlier message, as it would on its own. After a `delete`, the later `ttl` applies, and `keep` means the namespace default, as for a new record.\n"+
				"- A later `write` or `update_only` that nulls every bin of an earlier `replace` becomes a `delete`, which is the record those messages would leave. After a `replace_only`, the delete keeps the existence check.\n"+
				"- Nothing folds into a `write` or `update_only` whose every bin is null, and a later `write` or `update_only` does not fold into an earlier `write`, `update_only`, or `delete` when the folded command would set no bin. On a missing key such a message fails with `KEY_NOT_FOUND`, and one command cannot fail it and apply the other.\n\n"+
				"With `fencing.enabled`, only an earlier `write`, `replace`, or `delete` takes later messages. A later message whose fence value is not greater is folded and acknowledged, as the server would filter it out after the earlier message. A later message with a greater fence value folds only when it is a `replace`, `replace_only`, or `delete`, because that replaces the whole record whether or not the earlier message passed the fence; a `replace_only` becomes a `replace`. A `create_only` never folds: the record exists after a fenced message, and the server checks that before the fence, so it fails with `KEY_EXISTS`. Any other message is sent in a later batch command.\n\n"+
				"Messages carrying a `generation` are never coalesced. Coalescing only ever applies within one batch — see `max_in_flight` for what happens across concurrent batches.").
			Default(true).
			Advanced(),

		commitLevelField(),

		service.NewIntField(aoFieldMaxRecordBytes).
			Description("Reject a mapped record whose bins exceed this many bytes (approximate). `0` disables the check. Aerospike rewrites the whole record on every update, so treat anything above about 50 KiB as a modeling decision to justify — set a budget here rather than waiting for `RECORD_TOO_BIG`.").
			Default(0).
			LintRule(nonNegativeLint).
			Advanced(),

		service.NewIntListField(aoFieldIgnoreErrorCodes).
			Description(`Aerospike result codes to acknowledge instead of failing the message. Empty by default, so a rejection is still a failure and Connect retries it.

A listed code is logged at warning, counted in `+"`aerospike_ignored_errors`"+`, and then acknowledged. That drops the message: the record is not written, the payload is not kept, and `+"`output.fallback`"+` does not see it, because this output reported success. Use this for a permanent reject such as `+"`13`"+` (`+"`RECORD_TOO_BIG`"+`), `+"`20`"+` (`+"`INVALID_NAMESPACE`"+`), `+"`21`"+` (`+"`BIN_NAME_TOO_LONG`"+`), or `+"`22`"+` (`+"`FAIL_FORBIDDEN`"+`), which would otherwise retry forever and hold the source partition.

Code `+"`20`"+` is returned when the resolved namespace is not declared on the cluster. The client marks that record and still writes the other records in the batch. Namespaces cannot be created at runtime. A resolved value of `+"`null`"+` (a missing interpolated field) is rejected while the batch is planned and is not code 20. When the namespace comes from the message, prefer `+"`output.fallback`"+` over listing 20: a typo in that field is published instead of dropped.

Code `+"`21`"+` also covers a top-level bin name longer than 15 bytes. That name is rejected while the batch is planned, so the record is never sent and the server does not return 21. A key inside a map or list is not a bin name and is not code 21.

Code `+"`22`"+` is returned when a positive TTL is written to a namespace with `+"`nsup-period 0`"+`. TTL values `+"`0`"+`, `+"`-1`"+` (`+"`never`"+`), and `+"`-2`"+` (`+"`keep`"+`) do not return 22.

Code `+"`2`"+` (`+"`KEY_NOT_FOUND`"+`) is returned by `+"`update_only`"+` and `+"`replace_only`"+` when the record does not exist, by a `+"`write`"+` or `+"`update_only`"+` whose every bin is null when the record does not exist, and by a delete that coalescing produced from `+"`replace_only`"+` after every bin was nulled. A plain delete of a missing record does not return it. Listing `+"`2`"+` drops those messages. Prefer `+"`output.fallback`"+` when a missing key should be sent somewhere else rather than dropped.

Codes `+"`5`"+` (`+"`KEY_EXISTS`"+`) and `+"`3`"+` (`+"`GENERATION_ERROR`"+`) are returned on the retry of a `+"`create_only`"+` or `+"`generation`"+` write that was in doubt but was applied. They do not clear on retry. Prefer `+"`output.fallback`"+` over listing them, because they also report a real conflict with another writer, which listing would drop.

With `+"`coalesce_batch_keys`"+` left on, several messages for one key become one record. A listed code on that record acknowledges every message that folded into it, including messages that would have fit on their own.

A connection failure of the batch is still a failure. So is any mapping error other than that 15-byte name, a result that is in doubt, and any result code left off this list. `+"`max_record_bytes`"+` is a separate local limit and is not code 13. Codes `+"`0`"+` (`+"`OK`"+`), `+"`9`"+` (`+"`TIMEOUT`"+`), `+"`14`"+` (`+"`KEY_BUSY`"+`), and `+"`18`"+` (`+"`DEVICE_OVERLOAD`"+`) are rejected, as are other retryable and connection codes. Listing one would drop a record a retry could still write.`).
			Default([]int{}).
			Example([]int{13, 20, 21, 22}).
			LintRule(retryableIgnoreLint()).
			Advanced(),
	)
	spec = spec.Fields(batchPolicyFieldsWithRetries(0)...)
	return spec.Fields(
		service.NewObjectField(aoFieldFencing,
			service.NewBoolField(aoFieldFencingEnabled).
				Description("Enable fencing.").
				Default(false),
			service.NewStringField(aoFieldFencingBin).
				Description("Bin holding the fence value. Counts toward the 15 character bin name limit. Readers of these records should ignore it, since it is bookkeeping rather than data. A message whose `bins` mapping produces this bin is rejected. Must match `fence_bin` on `aerospike_lookup`, which strips it from the records it emits.").
				Default(defaultFenceBin),
			service.NewInterpolatedStringField(aoFieldFencingValue).
				Description("An integer that strictly increases with every update of a given record key, so no two updates of a key share a value. A write whose value is not greater than the stored one is filtered out and acknowledged, so a second update carrying the same value is dropped. The Kafka offset satisfies this when keys map to partitions consistently, which is the default partitioner's behaviour. A timestamp does not, because two updates can share one.").
				Default(`${! meta("kafka_offset") }`).
				Example(`${! json("version") }`),
			service.NewStringField(aoFieldFencingTombstoneBin).
				Description("Bin written on a fenced delete to mark the record as a tombstone. Readers should treat a record carrying this bin as missing, and `aerospike_lookup` does. Counts toward the 15 character bin name limit. A message whose `bins` mapping produces this bin is rejected.").
				Default(defaultTombstoneBin),
			service.NewStringField(aoFieldFencingTombstoneTTL).
				Description(`Time-to-live for fenced tombstone records. Accepts a duration, or `+"`never`"+`. Defaults to `+"`never`"+` so the fence outlives the data TTL; if the tombstone expires, a stale replay can recreate the record. While a tombstone exists, `+"`create_only`"+` on that key fails with `+"`KEY_EXISTS`"+`, so with `+"`never`"+` it fails permanently.

Note that `+"`never`"+` means fenced deletes accumulate without bound: the records stay small, but Aerospike does not evict records that have no void-time, so a delete-heavy workload grows the namespace monotonically. Set a duration comfortably longer than the longest replay or redelivery window you need to defend against — beyond that point a replay is no longer a realistic risk — and budget the primary index at roughly 64 bytes per record per replica in the meantime.`).
				Default("never").
				Example("168h"),
		).Description(`Guards each write with a server-side filter expression so that a record is only
overwritten by a strictly newer message. Redelivered or out-of-order messages are discarded by the
server and acknowledged as successful, giving effectively-once writes without a transaction.
Those discards increment the `+"`aerospike_filtered_out`"+` counter and are logged at debug.

When fencing is enabled, deletes do not remove the record: they replace it with the fence plus a
tombstone bin, so a later replay cannot resurrect stale data. Readers should treat that tombstone
as not found, and `+"`aerospike_lookup`"+` does. `+"`create_only`"+` after a fenced delete fails because the tombstone still exists. `+"`replace_only`"+` succeeds and removes the tombstone, because the record is still there. `+"`update_only`"+` also succeeds and removes the tombstone, leaving a record with only the bins in that update.

Fencing requires `+"`ttl`"+` to be something other than `+"`keep`"+`, which is its default, so set it explicitly. See `+"`coalesce_batch_keys`"+` for how fenced messages for one key in a batch are combined.`).
			Version("4.121.0").
			Advanced(),

		service.NewBatchPolicyField(aoFieldBatching).
			Description("Allows you to configure a batching policy. Without one, every message is written by its own command, one network round trip per message. A good starting point is `count: 256` and `period: 1s`."),

		service.NewOutputMaxInFlightField().
			Description("Maximum number of batches to have in flight concurrently. The default of `1` writes batches in order, so writes to one key land in stream order. Coalescing and ordering only apply within a batch, so a value above `1` lets two concurrent batches write the same record key out of order and can leave an older value in the record. Raise it only when keys do not repeat across batches.").
			Default(1),
	).
		Example(
			"Stream a topic into Aerospike",
			"Consumes a Redpanda topic and writes each message as a record. A JSON body is keyed by user_id. A tombstone has no body, so the key is the Kafka message key and the record is deleted.",
			`
input:
  redpanda:
    seed_brokers: [ "localhost:19092" ]
    topics: [ "users" ]
    consumer_group: "aerospike-sink"

output:
  aerospike:
    hosts: [ "localhost:3000" ]
    namespace: test
    set: users
    key: '${! json("user_id").catch(meta("kafka_key")) }'
    bins: 'root = this.without("user_id")'
    operation: replace
    ttl: keep
    batching:
      count: 500
      period: 1s
`,
		).
		Example(
			"Write a record by digest",
			"The producer already has the 20-byte Aerospike digest, carried as a standard Base64 JSON field. The stored user key is null. For a raw 20-byte digest, encode it in the mapping, for example `digest: '${! meta(\"kafka_key\").encode(\"base64\") }'`.",
			`
output:
  aerospike:
    hosts: [ "localhost:3000" ]
    namespace: test
    set: users
    digest: '${! json("digest") }'
    bins: 'root = this.without("digest")'
    operation: replace
    batching:
      count: 256
      period: 1s
`,
		).
		Example(
			"Effectively-once with offset fencing",
			"Rejects redelivered or out-of-order messages on the server, so at-least-once delivery cannot regress a record to an older state.",
			`
output:
  aerospike:
    hosts: [ "localhost:3000" ]
    namespace: profiles
    set: user
    key: '${! meta("kafka_key") }'
    operation: replace
    bins: 'root = this'
    ttl: 0
    fencing:
      enabled: true
      bin: _off
      value: '${! meta("kafka_offset") }'
    batching:
      count: 256
      period: 1s
`,
		).
		Example(
			"Route topics to namespaces and sets",
			"One cluster, many namespace and set combinations. The topic name can be the namespace, or a switch can send each topic to a fixed namespace and set. A JSON field can choose the namespace. The set is limited to a known list. The test and payments namespaces must already be declared on the server. Each branch is wrapped in output.fallback so a code 20 (INVALID_NAMESPACE) rejection is not replayed forever.",
			`
input:
  redpanda:
    seed_brokers: [ "localhost:19092" ]
    topics: [ "test", "payments", "events" ]
    consumer_group: "aerospike-sink"

output:
  switch:
    cases:
      - check: meta("kafka_topic") == "events"
        output:
          fallback:
            - aerospike:
                hosts: [ "localhost:3000" ]
                namespace: '${! json("namespace_name") }'
                set: '${! if ["clicks", "views"].contains(json("set_name")) { json("set_name") } else { throw("set_name is not allowed") } }'
                key: '${! json("id") }'
                bins: 'root = this.without("id", "namespace_name", "set_name")'
                operation: replace
                batching:
                  count: 256
                  period: 1s
            - redpanda:
                seed_brokers: [ "localhost:19092" ]
                topic: aerospike-rejects
      - check: meta("kafka_topic") == "test" || meta("kafka_topic") == "payments"
        output:
          fallback:
            - aerospike:
                hosts: [ "localhost:3000" ]
                namespace: '${! meta("kafka_topic") }'
                set: users
                key: '${! meta("kafka_key") }'
                bins: 'root = this'
                operation: replace
                batching:
                  count: 256
                  period: 1s
            - redpanda:
                seed_brokers: [ "localhost:19092" ]
                topic: aerospike-rejects
`,
		)
}

// errFencedKeepTTL rejects a ttl of keep with fencing. A fenced delete leaves a
// tombstone that expires after tombstone_ttl, and a later write with keep
// would keep that expiration, so the record would expire with the tombstone.
var errFencedKeepTTL = fmt.Errorf("field '%v' must not be keep when '%v.%v' is true, because a write after a fenced delete would keep the expiration of the tombstone ('%v.%v'); set it to 0 for the namespace default, never, or a duration", aoFieldTTL, aoFieldFencing, aoFieldFencingEnabled, aoFieldFencing, aoFieldFencingTombstoneTTL)

// aerospikeConfig is the parsed, validated form of the output configuration.
type aerospikeConfig struct {
	client *clientConfig
	keys   *keyConfig

	operation         *service.InterpolatedString
	staticOperation   opKind
	operationIsStatic bool
	tombstoneAsDelete bool

	bins       *bloblang.Executor
	coerceInts bool

	ttl         *service.InterpolatedString
	staticTTL   uint32
	ttlIsStatic bool

	generation *service.InterpolatedString

	coalesce bool

	maxRecordBytes int

	// ignoreCodes are per-record result codes acknowledged as success. Empty
	// means every rejection fails the message.
	ignoreCodes map[int]struct{}

	fenceEnabled bool
	fenceBin     string
	fenceValue   *service.InterpolatedString
	tombstoneBin string
	tombstoneTTL uint32

	batchPolicy  *as.BatchPolicy
	writePolicy  *as.BatchWritePolicy
	deletePolicy *as.BatchDeletePolicy
}

func parseOutputConfig(conf *service.ParsedConfig) (*aerospikeConfig, error) {
	c := new(aerospikeConfig)

	var err error
	if c.client, err = parseClientConfig(conf); err != nil {
		return nil, err
	}
	if c.keys, err = parseKeyConfig(conf); err != nil {
		return nil, err
	}

	if c.operation, err = conf.FieldInterpolatedString(aoFieldOperation); err != nil {
		return nil, err
	}
	// Resolve and validate a non-dynamic operation up front, so a typo fails at
	// startup rather than on the first message.
	if lit, ok := c.operation.Static(); ok {
		if c.staticOperation, err = parseOpKind(lit); err != nil {
			return nil, fmt.Errorf("field '%v': %w", aoFieldOperation, err)
		}
		c.operationIsStatic = true
	}
	if c.tombstoneAsDelete, err = conf.FieldBool(aoFieldTombstoneAsDelete); err != nil {
		return nil, err
	}

	if c.bins, err = conf.FieldBloblang(aoFieldBins); err != nil {
		return nil, err
	}
	if c.coerceInts, err = conf.FieldBool(aoFieldCoerceIntegralFloats); err != nil {
		return nil, err
	}

	if c.ttl, err = conf.FieldInterpolatedString(aoFieldTTL); err != nil {
		return nil, err
	}
	if lit, ok := c.ttl.Static(); ok {
		if c.staticTTL, err = parseTTL(lit); err != nil {
			return nil, fmt.Errorf("field '%v': %w", aoFieldTTL, err)
		}
		c.ttlIsStatic = true
	}

	if c.generation, err = conf.FieldInterpolatedString(aoFieldGeneration); err != nil {
		return nil, err
	}

	if c.coalesce, err = conf.FieldBool(aoFieldCoalesceBatchKeys); err != nil {
		return nil, err
	}
	if c.maxRecordBytes, err = conf.FieldInt(aoFieldMaxRecordBytes); err != nil {
		return nil, err
	}
	if c.maxRecordBytes < 0 {
		return nil, fmt.Errorf("field '%v' must not be negative", aoFieldMaxRecordBytes)
	}

	codes, err := conf.FieldIntList(aoFieldIgnoreErrorCodes)
	if err != nil {
		return nil, err
	}
	if len(codes) > 0 {
		c.ignoreCodes = make(map[int]struct{}, len(codes))
		for _, code := range codes {
			if retryableIgnore(code) {
				return nil, fmt.Errorf("field '%v': code %d (%s) is retryable or not an error and must not be listed",
					aoFieldIgnoreErrorCodes, code, types.ResultCodeToString(types.ResultCode(code)))
			}
			c.ignoreCodes[code] = struct{}{}
		}
	}

	if conf.Contains(aoFieldFencing) {
		fc := conf.Namespace(aoFieldFencing)
		if c.fenceEnabled, err = fc.FieldBool(aoFieldFencingEnabled); err != nil {
			return nil, err
		}
		if c.fenceBin, err = fc.FieldString(aoFieldFencingBin); err != nil {
			return nil, err
		}
		if c.fenceValue, err = fc.FieldInterpolatedString(aoFieldFencingValue); err != nil {
			return nil, err
		}
		if c.tombstoneBin, err = fc.FieldString(aoFieldFencingTombstoneBin); err != nil {
			return nil, err
		}
		ttlStr, err := fc.FieldString(aoFieldFencingTombstoneTTL)
		if err != nil {
			return nil, err
		}
		if c.tombstoneTTL, err = parseTTL(ttlStr); err != nil {
			return nil, fmt.Errorf("field '%v.%v': %w", aoFieldFencing, aoFieldFencingTombstoneTTL, err)
		}
		if c.tombstoneTTL == as.TTLDontUpdate {
			return nil, fmt.Errorf("field '%v.%v' must be a duration or never, not keep", aoFieldFencing, aoFieldFencingTombstoneTTL)
		}
	}
	if c.fenceEnabled {
		if err := validateBinName(c.fenceBin); err != nil {
			return nil, fmt.Errorf("field '%v.%v': %w", aoFieldFencing, aoFieldFencingBin, err)
		}
		if err := validateBinName(c.tombstoneBin); err != nil {
			return nil, fmt.Errorf("field '%v.%v': %w", aoFieldFencing, aoFieldFencingTombstoneBin, err)
		}
		if c.tombstoneBin == c.fenceBin {
			return nil, fmt.Errorf("field '%v.%v' must not be the same as '%v.%v'", aoFieldFencing, aoFieldFencingTombstoneBin, aoFieldFencing, aoFieldFencingBin)
		}
		if c.ttlIsStatic && c.staticTTL == as.TTLDontUpdate {
			return nil, errFencedKeepTTL
		}
	}

	sendKey, err := conf.FieldBool(aoFieldSendKey)
	if err != nil {
		return nil, err
	}
	durableDelete, err := conf.FieldBool(aoFieldDurableDelete)
	if err != nil {
		return nil, err
	}

	if c.batchPolicy, err = parseBatchPolicy(conf); err != nil {
		return nil, err
	}

	c.writePolicy = as.NewBatchWritePolicy()
	c.writePolicy.SendKey = sendKey
	c.writePolicy.DurableDelete = durableDelete

	c.deletePolicy = as.NewBatchDeletePolicy()
	c.deletePolicy.SendKey = sendKey
	c.deletePolicy.DurableDelete = durableDelete

	commitStr, err := conf.FieldString(aoFieldCommitLevel)
	if err != nil {
		return nil, err
	}
	commit, err := parseCommitLevel(commitStr)
	if err != nil {
		return nil, fmt.Errorf("field '%v': %w", aoFieldCommitLevel, err)
	}
	c.writePolicy.CommitLevel = commit
	c.deletePolicy.CommitLevel = commit

	return c, nil
}

// ignores reports whether a per-record result code is configured to be
// acknowledged. An empty list ignores nothing.
func (c *aerospikeConfig) ignores(code types.ResultCode) bool {
	if c == nil || len(c.ignoreCodes) == 0 {
		return false
	}
	_, exists := c.ignoreCodes[int(code)]
	return exists
}
