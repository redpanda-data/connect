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
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	aoFieldNotFound     = "not_found"
	aoFieldEmitMetadata = "emit_metadata"
	aoFieldTombstoneBin = "tombstone_bin"
	aoFieldFenceBin     = "fence_bin"

	// "null" would be parsed as a YAML null rather than a string, so the option
	// that emits a null result has to be named something YAML leaves alone.
	notFoundNull  = "emit_null"
	notFoundError = "error"
	notFoundDrop  = "drop"

	metaGeneration = "aerospike_generation"
	metaTTL        = "aerospike_ttl"

	// Matches the framework's default pipeline thread count closely enough to
	// keep the connection pool ahead of demand without holding descriptors the
	// pipeline will never use.
	defaultLookupConcurrency = 16
)

func init() {
	service.MustRegisterBatchProcessor("aerospike_lookup", lookupSpec(), newLookupProcessor)
}

func lookupSpec() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Version("4.121.0").
		Categories("Services").
		Summary("Looks up Aerospike records by primary key and replaces each message with the record's bins.").
		Description(`
Reads one record per message, keyed by an interpolated primary key, and replaces the message
content with a JSON object of the record's bins. Intended for enrichment, so it is normally
wrapped in a ` + "xref:components:processors/branch.adoc[`branch`]" + ` processor which grafts
the result back onto the original message.

== One batch read, deduplicated

The whole batch is served by a single Aerospike batch command, and repeated keys within a
batch are looked up once and fanned back out to every message that asked for them. A stream
of events referencing a few hundred entities therefore costs a few hundred reads, not one
per event.

== Failure handling

Each key's result code is applied to its own message via a message-level error, so a missing
or rejected record does not fail the rest of the batch. Use the standard
xref:configuration:error_handling.adoc[error handling] patterns to route them.

== Selecting bins

Naming bins in ` + "`bins`" + ` reduces what crosses the network, but not what the server reads
from storage — Aerospike stores a record contiguously and reads all of it regardless. Select
bins to save bandwidth and client-side work, not device I/O.

This is a primary-key lookup, not a query. Secondary indexes, scans, and joins are out of
scope: denormalise into the record the write path stores, or issue another keyed lookup.
`)

	spec = spec.Fields(clientFields()...)
	spec = spec.Fields(keyFields(`${! json("user_id") }`, `${! meta("kafka_key") }`)...)

	spec = spec.Fields(
		service.NewStringListField(aoFieldBins).
			Description("Bin names to read. Leave empty to read every bin.").
			Default([]string{}).
			Example([]string{"tier", "ltv", "country"}),

		service.NewStringEnumField(aoFieldNotFound, notFoundNull, notFoundError, notFoundDrop).
			Description(`What to do when the record does not exist:

- `+"`emit_null`"+` — set the message to `+"`null`"+`, letting a `+"`branch`"+` result map decide.
- `+"`error`"+` — mark the message with an error for the standard error handling patterns.
- `+"`drop`"+` — remove the message from the batch.`).
			Default(notFoundNull),

		service.NewBoolField(aoFieldEmitMetadata).
			Description("Attach `aerospike_generation` and `aerospike_ttl` metadata from the record. Both are written in the form the Aerospike output accepts, so `generation` reads `aerospike_generation` for a compare-and-set write and `ttl` reads `aerospike_ttl` to carry an expiration across a read-modify-write. A record that never expires reports its TTL as `never`.").
			Default(false).
			Advanced(),

		service.NewStringField(aoFieldTombstoneBin).
			Description("Treat a record that carries this bin as missing, and strip it from the emitted document. Must match `fencing.tombstone_bin` on the Aerospike output when fencing is enabled. Empty disables the check.").
			Default(defaultTombstoneBin).
			Advanced(),

		service.NewStringField(aoFieldFenceBin).
			Description("Strip this bin from the emitted document. Fencing bookkeeping is not part of the record's data, so leaving it in place would graft an internal field onto every enriched message. Must match `fencing.bin` on the Aerospike output when fencing is enabled. Empty disables the check.").
			Default(defaultFenceBin).
			Advanced(),
	)
	spec = spec.Fields(batchPolicyFields()...)
	spec = spec.Fields(readPolicyFields()...)
	return spec.
		Example(
			"Enrich a stream with a user profile",
			"Looks up a profile keyed by a field on the event and attaches it under `profile`, leaving the original message otherwise untouched.",
			`
pipeline:
  processors:
    - branch:
        processors:
          - aerospike_lookup:
              hosts: [ "localhost:3000" ]
              namespace: profiles
              set: user
              key: '${! json("user_id") }'
              bins: [ tier, ltv, country ]
        result_map: 'root.profile = this'
`,
		).
		Example(
			"Drop events for unknown entities",
			"Uses the lookup as a filter as well as an enrichment: events whose key has no record are removed.",
			`
pipeline:
  processors:
    - branch:
        processors:
          - aerospike_lookup:
              hosts: [ "localhost:3000" ]
              namespace: profiles
              set: user
              key: '${! json("user_id") }'
              not_found: drop
        result_map: 'root.profile = this'
`,
		)
}

type lookupConfig struct {
	client *clientConfig
	keys   *keyConfig

	// binNames is what the client is asked for, which includes the tombstone
	// bin when it is not already named: a record can only be recognised as a
	// fenced delete if that bin was actually read.
	binNames     []string
	readAllBins  bool
	notFound     string
	emitMetadata bool
	tombstoneBin string
	fenceBin     string

	batchPolicy *as.BatchPolicy
	readPolicy  *as.BatchReadPolicy
}

func parseLookupConfig(conf *service.ParsedConfig) (*lookupConfig, error) {
	c := &lookupConfig{}

	var err error
	if c.client, err = parseClientConfig(conf); err != nil {
		return nil, err
	}
	if c.keys, err = parseKeyConfig(conf); err != nil {
		return nil, err
	}

	if c.binNames, err = conf.FieldStringList(aoFieldBins); err != nil {
		return nil, err
	}
	for _, name := range c.binNames {
		if err := validateBinName(name); err != nil {
			return nil, fmt.Errorf("field '%v': %w", aoFieldBins, err)
		}
	}
	// With no bin names the client reads only the record header, so reading
	// every bin has to be requested explicitly.
	c.readAllBins = len(c.binNames) == 0

	if c.notFound, err = conf.FieldString(aoFieldNotFound); err != nil {
		return nil, err
	}
	if c.emitMetadata, err = conf.FieldBool(aoFieldEmitMetadata); err != nil {
		return nil, err
	}
	if c.tombstoneBin, err = conf.FieldString(aoFieldTombstoneBin); err != nil {
		return nil, err
	}
	if c.tombstoneBin != "" {
		if err := validateBinName(c.tombstoneBin); err != nil {
			return nil, fmt.Errorf("field '%v': %w", aoFieldTombstoneBin, err)
		}
	}
	if c.fenceBin, err = conf.FieldString(aoFieldFenceBin); err != nil {
		return nil, err
	}
	if c.fenceBin != "" {
		if err := validateBinName(c.fenceBin); err != nil {
			return nil, fmt.Errorf("field '%v': %w", aoFieldFenceBin, err)
		}
	}

	// The tombstone check needs the bin to have been read, so request it
	// alongside the named bins. Doing this once here keeps it off the per-key
	// path in planReads.
	if !c.readAllBins && c.tombstoneBin != "" && !slices.Contains(c.binNames, c.tombstoneBin) {
		c.binNames = append(slices.Clone(c.binNames), c.tombstoneBin)
	}

	c.readPolicy = as.NewBatchReadPolicy()

	if c.batchPolicy, err = parseBatchPolicy(conf); err != nil {
		return nil, err
	}
	if err := applyReadPolicy(conf, c.batchPolicy); err != nil {
		return nil, err
	}

	return c, nil
}

func newLookupProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
	parsed, err := parseLookupConfig(conf)
	if err != nil {
		return nil, err
	}
	// A processor has no max_in_flight to read: its concurrency is whatever the
	// pipeline runs it at. Size for a typical thread count so lookups are not
	// the first thing to find an empty pool.
	parsed.client.sizePoolForConcurrency(defaultLookupConcurrency)
	parsed.client.warnStartup(mgr.Logger())

	return &lookupProcessor{
		conf: parsed,
		conn: newConnection(parsed.client, mgr.Logger()),
		log:  mgr.Logger(),
	}, nil
}

type lookupProcessor struct {
	conf *lookupConfig
	conn *connection
	log  *service.Logger

	// Processors have no Connect callback, so the cluster connection is
	// established on first use and re-established after a loss.
	connectMut sync.Mutex
}

func (p *lookupProcessor) Close(ctx context.Context) error {
	return p.conn.close(ctx)
}

// client returns the live client, connecting first when needed. The caller
// must call release when it is done with the client.
func (p *lookupProcessor) client(ctx context.Context) (client *as.Client, release func(), err error) {
	if client, release := p.conn.acquire(); client != nil {
		return client, release, nil
	}

	p.connectMut.Lock()
	defer p.connectMut.Unlock()

	// Another goroutine may have connected while we waited for the lock.
	if client, release := p.conn.acquire(); client != nil {
		return client, release, nil
	}
	if err := p.conn.connect(ctx); err != nil {
		if p.log != nil {
			p.log.Errorf("Connecting to Aerospike: %v", err)
		}
		return nil, nil, err
	}
	if client, release := p.conn.acquire(); client != nil {
		return client, release, nil
	}
	return nil, nil, service.ErrNotConnected
}

// pendingRead is one deduplicated record read, plus the batch indexes waiting
// on it.
type pendingRead struct {
	read    *as.BatchRead
	indexes []int
}

func (p *lookupProcessor) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	client, release, err := p.client(ctx)
	if err != nil {
		// Returning an error marks every message in the batch, which is the
		// right outcome when the cluster is unreachable.
		return nil, err
	}
	defer release()

	// The incoming batch must not be mutated.
	out := batch.Copy()

	reads := p.planReads(out)
	if len(reads) == 0 {
		return assemble(out, nil), nil
	}

	records := make([]as.BatchRecordIfc, len(reads))
	for i, r := range reads {
		records[i] = r.read
	}

	policy, err := batchPolicyForContext(ctx, p.conf.batchPolicy)
	if err != nil {
		return nil, err
	}

	if batchErr := client.BatchOperate(policy, records); batchErr != nil {
		if isConnectionError(batchErr) {
			if p.log != nil {
				p.log.Errorf("Aerospike lookup batch failed: %v", batchErr)
			}
			// Drop the client so the next batch reconnects, and fail this one.
			p.conn.drop()
			return nil, batchErr
		}
		// Otherwise fall through: per-key result codes say which keys failed.
	}

	dropped := map[int]bool{}
	for _, r := range reads {
		rec := r.read.BatchRec()
		for _, idx := range r.indexes {
			if p.applyResult(out[idx], rec) == resultDrop {
				dropped[idx] = true
			}
		}
	}

	return assemble(out, dropped), nil
}

// planReads resolves a key per message and deduplicates them, so a batch that
// references the same entity many times issues one read for it. Messages whose
// key cannot be resolved are marked in place and contribute no read.
func (p *lookupProcessor) planReads(batch service.MessageBatch) []*pendingRead {
	resolver := p.conf.keys.resolver(batch)

	reads := make([]*pendingRead, 0, len(batch))
	byKey := make(map[string]*pendingRead, len(batch))

	for i := range batch {
		key, err := resolver.resolve(i)
		if err != nil {
			batch[i].SetError(err)
			continue
		}

		id := keyID(key)
		if prev, exists := byKey[id]; exists {
			prev.indexes = append(prev.indexes, i)
			continue
		}

		// NewBatchRead derives ReadAllBins from an empty bin list, which is
		// what readAllBins already means, so no override is needed here.
		read := as.NewBatchRead(p.conf.readPolicy, key, p.conf.binNames)

		pending := &pendingRead{read: read, indexes: []int{i}}
		byKey[id] = pending
		reads = append(reads, pending)
	}

	return reads
}

type resultAction int

const (
	resultKeep resultAction = iota
	resultDrop
)

func (p *lookupProcessor) applyResult(msg *service.Message, rec *as.BatchRecord) resultAction {
	switch rec.ResultCode {
	case types.OK:
		if rec.Record == nil {
			return p.applyNotFound(msg)
		}
		if isFencedTombstone(map[string]any(rec.Record.Bins), p.conf.tombstoneBin) {
			return p.applyNotFound(msg)
		}
		out := fromAerospike(map[string]any(rec.Record.Bins))
		if m, ok := out.(map[string]any); ok {
			// Fencing bookkeeping is not part of the record's data; leaving it
			// in place grafts an internal field onto every enriched message.
			for _, bin := range [...]string{p.conf.tombstoneBin, p.conf.fenceBin} {
				if bin != "" {
					delete(m, bin)
				}
			}
			out = m
		}
		msg.SetStructuredMut(out)
		if p.conf.emitMetadata {
			msg.MetaSetMut(metaGeneration, int(rec.Record.Generation))
			msg.MetaSetMut(metaTTL, formatTTL(rec.Record.Expiration))
		}
		return resultKeep

	case types.KEY_NOT_FOUND_ERROR:
		return p.applyNotFound(msg)

	default:
		msg.SetError(fmt.Errorf("aerospike lookup failed for key %v: %s (code %d: %s)",
			rec.Key, explainResultCode(rec.ResultCode), rec.ResultCode,
			types.ResultCodeToString(rec.ResultCode)))
		return resultKeep
	}
}

func (p *lookupProcessor) applyNotFound(msg *service.Message) resultAction {
	switch p.conf.notFound {
	case notFoundError:
		msg.SetError(errors.New("aerospike lookup found no record for this key"))
	case notFoundDrop:
		return resultDrop
	default:
		msg.SetStructuredMut(nil)
	}
	return resultKeep
}

// assemble builds the outgoing batch, omitting dropped messages. Messages whose
// key could not be resolved never enter the drop set, so they keep their
// original content and carry an error.
func assemble(batch service.MessageBatch, dropped map[int]bool) []service.MessageBatch {
	if len(dropped) == 0 {
		return []service.MessageBatch{batch}
	}

	kept := make(service.MessageBatch, 0, len(batch)-len(dropped))
	for i, msg := range batch {
		if dropped[i] {
			continue
		}
		kept = append(kept, msg)
	}
	if len(kept) == 0 {
		return nil
	}
	return []service.MessageBatch{kept}
}
