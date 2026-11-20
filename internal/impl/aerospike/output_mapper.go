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
	"slices"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"

	"github.com/redpanda-data/benthos/v4/public/service"
)

type opKind int

const (
	opWrite opKind = iota
	opReplace
	opReplaceOnly
	opCreateOnly
	opUpdateOnly
	opDelete
)

func (o opKind) String() string {
	switch o {
	case opWrite:
		return "write"
	case opReplace:
		return "replace"
	case opReplaceOnly:
		return "replace_only"
	case opCreateOnly:
		return "create_only"
	case opUpdateOnly:
		return "update_only"
	case opDelete:
		return "delete"
	}
	return "unknown"
}

func (o opKind) recordExistsAction() as.RecordExistsAction {
	switch o {
	case opReplace:
		return as.REPLACE
	case opReplaceOnly:
		return as.REPLACE_ONLY
	case opCreateOnly:
		return as.CREATE_ONLY
	case opUpdateOnly:
		return as.UPDATE_ONLY
	default:
		return as.UPDATE
	}
}

func parseOpKind(s string) (opKind, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "write", "update", "":
		return opWrite, nil
	case "replace":
		return opReplace, nil
	case "replace_only":
		return opReplaceOnly, nil
	case "create_only", "create":
		return opCreateOnly, nil
	case "update_only":
		return opUpdateOnly, nil
	case "delete":
		return opDelete, nil
	}
	return 0, fmt.Errorf("unknown operation %q: expected one of write, replace, replace_only, create_only, update_only, delete", s)
}

// pendingOp is one record write, possibly folded from several source messages.
type pendingOp struct {
	key *as.Key
	// indexes lists every source batch index folded into this op, so a failure
	// can be reported against all of them.
	indexes []int

	kind opKind

	// bins is the accumulated bin set. binOrder preserves insertion order so
	// generated operations are deterministic, which keeps tests meaningful.
	bins     map[string]any
	binOrder []string

	ttl uint32

	generation    uint32
	hasGeneration bool

	// deleteRequiresRecord marks a delete that stands in for a replace_only
	// whose bins were all nulled by a later update_only. A plain delete of a
	// missing key succeeds. This one fails, so the existence check survives.
	deleteRequiresRecord bool

	// round orders commands for one key that cannot share a command. A batch
	// sends round 0, then round 1 once round 0 has completed, and so on.
	round int
}

func newPendingOp(index int, key *as.Key, kind opKind, ttl uint32) *pendingOp {
	return &pendingOp{
		key:     key,
		indexes: []int{index},
		kind:    kind,
		bins:    map[string]any{},
		ttl:     ttl,
	}
}

func (p *pendingOp) setBin(name string, value any) {
	if _, exists := p.bins[name]; !exists {
		p.binOrder = append(p.binOrder, name)
	}
	p.bins[name] = value
}

func (p *pendingOp) resetBins() {
	p.bins = map[string]any{}
	p.binOrder = nil
}

func (p *pendingOp) copyBinsFrom(next *pendingOp) {
	for _, name := range next.binOrder {
		p.setBin(name, next.bins[name])
	}
}

func (p *pendingOp) take(next *pendingOp) {
	p.kind = next.kind
	p.deleteRequiresRecord = next.deleteRequiresRecord
	p.resetBins()
	p.copyBinsFrom(next)
}

// canFold reports whether next can be folded into this op. One command must
// leave the same record, and fail the same messages, as the two commands run
// in order, whether the key exists or not.
//
//   - write, replace, and delete leave a known record: the merged bins, the
//     replaced bins, or no record. A later write, replace, or delete applies
//     to that record without an existence check, so it folds.
//   - update_only and replace_only require the record. After write or replace
//     the record exists, so the check passes and is dropped. After another
//     update_only or replace_only the folded command keeps the check, and a
//     missing key fails every message as it would have on its own.
//   - create_only folds only after a delete. The delete removes the record,
//     so the create succeeds and the result is a replace.
//
// Every other pair waits for a later round. For example, create_only{a:1}
// then create_only{a:2} on a new key stores a=1 and fails the second message
// with KEY_EXISTS, which one command cannot do. A delete that coalescing made
// from replace_only keeps its existence check, so nothing folds into it.
func (p *pendingOp) canFold(next *pendingOp) bool {
	if p.kind == opDelete && p.deleteRequiresRecord {
		return false
	}
	switch next.kind {
	case opWrite, opReplace, opDelete:
		return p.kind == opWrite || p.kind == opReplace || p.kind == opDelete
	case opUpdateOnly, opReplaceOnly:
		return p.kind == opWrite || p.kind == opReplace || p.kind == opUpdateOnly || p.kind == opReplaceOnly
	case opCreateOnly:
		return p.kind == opDelete
	}
	return false
}

// fold merges a later operation for the same key into this one. The caller
// checks canFold first.
//
// A later ttl of keep leaves the expiration the earlier command set, so the
// earlier ttl stays. After a delete the later ttl applies, because the record
// is created again.
//
// Generation is deliberately not merged here: coalesceOps never folds an
// operation carrying a compare-and-set check, because one command can only
// carry one expected generation.
func (p *pendingOp) fold(next *pendingOp) {
	p.indexes = append(p.indexes, next.indexes...)
	if p.kind == opDelete || next.ttl != as.TTLDontUpdate {
		p.ttl = next.ttl
	}

	switch next.kind {
	case opDelete, opReplace:
		p.take(next)
	case opReplaceOnly:
		prior := p.kind
		p.take(next)
		if prior == opWrite || prior == opReplace {
			// The record exists, so the check always passes.
			p.kind = opReplace
		}
	case opCreateOnly:
		// Only after a delete, so the record is gone and the create succeeds.
		p.take(next)
		p.kind = opReplace
	case opWrite:
		if p.kind == opDelete {
			// Bins the delete removed must not survive.
			p.kind = opReplace
			p.resetBins()
		}
		p.copyBinsFrom(next)
	case opUpdateOnly:
		p.copyBinsFrom(next)
	}

	// A later write or update_only can null every bin of an earlier replace.
	// That command would send no operations, and a record with no bins does
	// not exist, so the fold is a delete. An all-null replace or replace_only
	// never reaches here: mapMessage rejects that message on its own.
	//
	// replace_only keeps its existence check. The delete fails when the key
	// is missing, which is what [replace_only{a:1}, update_only{a:null}]
	// would do on its own. A replace would have created the record, so a
	// missing key is already the end state and the delete succeeds.
	if p.kind != opDelete {
		p.deleteRequiresRecord = false
	}
	if p.kind == opDelete {
		return
	}
	if len(p.putOps(p.kind.recordExistsAction())) == 0 {
		p.deleteRequiresRecord = p.kind == opReplaceOnly
		p.kind = opDelete
		p.resetBins()
	}
}

// coalesceOps orders the commands for each record key and, when fold is set,
// folds a message into the previous command for its key where canFold allows.
// A message that is not folded goes in the round after that command, so the
// commands for one key run in message order and never in the same batch
// command.
//
// A key touched by any compare-and-set message is never folded. A generation
// check is a statement about the specific record the caller read, and every
// write increments the generation, so folding two writes would change the
// generation a later check expects.
func coalesceOps(ops []*pendingOp, fold bool) []*pendingOp {
	ids := make([]string, len(ops))
	exempt := map[string]bool{}
	for i, op := range ops {
		ids[i] = keyID(op.key)
		if op.hasGeneration {
			exempt[ids[i]] = true
		}
	}

	out := make([]*pendingOp, 0, len(ops))
	last := make(map[string]*pendingOp, len(ops))
	for i, op := range ops {
		id := ids[i]
		prev, seen := last[id]
		if seen && fold && !exempt[id] && prev.canFold(op) {
			prev.fold(op)
			continue
		}
		if seen {
			op.round = prev.round + 1
		}
		last[id] = op
		out = append(out, op)
	}
	return out
}

// batchMapper maps one batch of messages into pending writes.
//
// Every interpolation and mapping is bound to the batch rather than to
// individual messages, so batch-aware Bloblang — `batch_index()`, windowed
// queries — behaves as it does elsewhere in Redpanda Connect.
type batchMapper struct {
	conf  *aerospikeConfig
	batch service.MessageBatch

	keys       *keyResolver
	bins       *service.MessageBatchBloblangExecutor
	op         *service.MessageBatchInterpolationExecutor
	ttl        *service.MessageBatchInterpolationExecutor
	generation *service.MessageBatchInterpolationExecutor
}

func newBatchMapper(conf *aerospikeConfig, batch service.MessageBatch) *batchMapper {
	m := &batchMapper{
		conf:  conf,
		batch: batch,
		keys:  conf.keys.resolver(batch),
		bins:  batch.BloblangExecutor(conf.bins),
	}
	if !conf.operationIsStatic {
		m.op = batch.InterpolationExecutor(conf.operation)
	}
	if !conf.ttlIsStatic {
		m.ttl = batch.InterpolationExecutor(conf.ttl)
	}
	if lit, ok := conf.generation.Static(); !ok || strings.TrimSpace(lit) != "" {
		m.generation = batch.InterpolationExecutor(conf.generation)
	}
	return m
}

// mapMessage converts the message at the given batch index into a pending write.
func (m *batchMapper) mapMessage(index int) (*pendingOp, error) {
	key, err := m.keys.resolve(index)
	if err != nil {
		return nil, err
	}

	kind, err := m.resolveOperation(index)
	if err != nil {
		return nil, err
	}

	ttl, err := m.resolveTTL(index)
	if err != nil {
		return nil, err
	}

	op := newPendingOp(index, key, kind, ttl)
	if gen, ok, err := m.resolveGeneration(index); err != nil {
		return nil, err
	} else if ok {
		op.generation, op.hasGeneration = gen, true
	}
	if kind == opDelete {
		return op, nil
	}

	if err := m.applyBins(op, index); err != nil {
		return nil, err
	}

	if len(op.binOrder) == 0 {
		return nil, fmt.Errorf("mapping in field '%v' produced no bins; a record must have at least one bin", aoFieldBins)
	}

	// Reject this message before coalescing. Folding an all-null replace into
	// an earlier write would delete the record that write just created.
	// write and update_only keep a null: that put deletes the bin.
	if kind == opReplace || kind == opReplaceOnly {
		if len(op.putOps(kind.recordExistsAction())) == 0 {
			return nil, nullReplaceError(kind, false)
		}
	}

	return op, nil
}

func (m *batchMapper) resolveOperation(index int) (opKind, error) {
	if m.conf.tombstoneAsDelete && isTombstone(m.batch[index]) {
		return opDelete, nil
	}
	if m.conf.operationIsStatic {
		return m.conf.staticOperation, nil
	}
	opStr, err := m.op.TryString(index)
	if err != nil {
		return 0, fmt.Errorf("interpolating '%v': %w", aoFieldOperation, err)
	}
	kind, err := parseOpKind(opStr)
	if err != nil {
		return 0, fmt.Errorf("field '%v': %w", aoFieldOperation, err)
	}
	return kind, nil
}

func (m *batchMapper) resolveTTL(index int) (uint32, error) {
	if m.conf.ttlIsStatic {
		return m.conf.staticTTL, nil
	}
	ttlStr, err := m.ttl.TryString(index)
	if err != nil {
		return 0, fmt.Errorf("interpolating '%v': %w", aoFieldTTL, err)
	}
	ttl, err := parseTTL(ttlStr)
	if err != nil {
		return 0, fmt.Errorf("field '%v': %w", aoFieldTTL, err)
	}
	return ttl, nil
}

func (m *batchMapper) resolveGeneration(index int) (uint32, bool, error) {
	if m.generation == nil {
		return 0, false, nil
	}
	raw, err := m.generation.TryString(index)
	if err != nil {
		return 0, false, fmt.Errorf("interpolating '%v': %w", aoFieldGeneration, err)
	}
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "null" {
		return 0, false, nil
	}
	v, err := strconv.ParseUint(raw, 10, 32)
	if err != nil {
		return 0, false, fmt.Errorf("field '%v' resolved to %q, which is not an unsigned integer: %w", aoFieldGeneration, raw, err)
	}
	return uint32(v), true, nil
}

// applyBins runs the bins mapping and converts the resulting object into bins.
func (m *batchMapper) applyBins(op *pendingOp, index int) error {
	mapped, err := m.bins.Query(index)
	if err != nil {
		return fmt.Errorf("executing mapping in field '%v': %w", aoFieldBins, err)
	}
	if mapped == nil {
		// The mapping deleted the root. Dropping the message here would look
		// like a successful write, so make the user filter upstream instead.
		return fmt.Errorf("mapping in field '%v' deleted the root; filter unwanted messages with a processor before this output", aoFieldBins)
	}

	res, err := mapped.AsStructured()
	if err != nil {
		return fmt.Errorf("mapping in field '%v' did not produce structured data: %w", aoFieldBins, err)
	}

	obj, ok := res.(map[string]any)
	if !ok {
		return fmt.Errorf("mapping in field '%v' must produce an object, got %T", aoFieldBins, res)
	}

	// Sort for deterministic bin ordering; Go map iteration is randomised and
	// unstable ordering makes failures hard to reproduce.
	names := make([]string, 0, len(obj))
	for name := range obj {
		names = append(names, name)
	}
	slices.Sort(names)

	for _, name := range names {
		if err := validateBinName(name); err != nil {
			if len(name) > maxBinNameLen {
				return &longBinNameError{key: op.key, name: name, n: len(name)}
			}
			return fmt.Errorf("%w; rename it in the '%v' mapping", err, aoFieldBins)
		}
		value, err := toAerospike(obj[name], m.conf.coerceInts)
		if err != nil {
			return fmt.Errorf("bin %q: %w", name, err)
		}
		op.setBin(name, value)
	}

	if m.conf.maxRecordBytes > 0 {
		size := 0
		for _, name := range op.binOrder {
			size += len(name) + estimateSize(op.bins[name])
		}
		if size > m.conf.maxRecordBytes {
			return fmt.Errorf("mapped record is about %d bytes, which exceeds max_record_bytes (%d); Aerospike rewrites the whole record on every update, so cap collections in the '%v' mapping or split the data across keys", size, m.conf.maxRecordBytes, aoFieldBins)
		}
	}
	return nil
}

// isTombstone reports whether a message carries no payload, which is the Kafka
// convention for "this key is deleted".
func isTombstone(msg *service.Message) bool {
	if v, err := msg.AsStructured(); err == nil {
		if v == nil {
			return true
		}
		if s, ok := v.(string); ok {
			trimmed := strings.TrimSpace(s)
			return trimmed == "" || trimmed == "null"
		}
		return false
	}
	b, err := msg.AsBytes()
	if err != nil {
		return false
	}
	trimmed := strings.TrimSpace(string(b))
	return trimmed == "" || trimmed == "null"
}
