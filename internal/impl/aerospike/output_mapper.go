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

	fence    int64
	hasFence bool

	// deleteRequiresRecord marks a delete that stands in for a replace_only
	// whose bins were all nulled by a later update_only. A plain delete of a
	// missing key succeeds. This one fails, so the existence check survives.
	deleteRequiresRecord bool
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

// fold merges a later operation for the same key into this one, so that a batch
// containing several messages for one record issues a single write.
//
// The merge rules preserve the outcome of applying the operations in sequence
// when a single Aerospike command can do that. Existence-conditional ops
// (create_only / update_only / replace_only) cannot always keep their existence
// check without nacking a preceding write that would have succeeded, so the
// check is dropped in favour of the sequential final record:
//
//   - delete and replace each discard whatever was accumulated, because they
//     define the record's full state.
//   - replace_only also defines the full record. It keeps the "must exist"
//     check when folded with update_only or another replace_only, since every
//     one of those ops fails when the key is missing. After write, replace, or
//     create_only it is promoted to replace, so the op that would have created
//     the record is not nacked as KEY_NOT_FOUND. After an unfenced delete it
//     is dropped, so a successful delete is not nacked as KEY_NOT_FOUND.
//     After a fenced delete it is promoted to replace carrying its own bins:
//     the tombstone record is still there, and REPLACE_ONLY would fail when
//     that delete is what created it. update_only after a fenced delete is
//     promoted the same way, so bins the delete removed do not survive.
//   - write merges its bins over the accumulation, and inherits the accumulated
//     kind — so [replace{a}, write{b}] stays a replace carrying {a,b}. After
//     replace_only it is promoted to replace: the existence check is dropped
//     and bins already on the server must not survive.
//   - write after delete is promoted to replace, because the intervening delete
//     means bins already on the server must not survive.
//   - an unfenced replace whose every remaining bin is null becomes a delete.
//     A later write or update_only has nulled the bins an earlier replace or
//     delete set up, and a record with no bins does not exist. An unfenced
//     all-null replace or replace_only is rejected in mapMessage and never
//     reaches here, so it cannot delete a preceding write.
//   - an unfenced replace_only left with only null bins becomes a delete that
//     still fails when the record is missing. [replace_only{a:1},
//     update_only{a:null}] keeps the existence check. A later message then
//     folds with the unfenced-delete rules.
//   - update_only after write/replace merges bins and keeps the write/replace,
//     so [write{a}, update_only{b}] is not folded into a lone update_only that
//     would KEY_NOT_FOUND and nack both messages. After replace_only it merges
//     and stays replace_only, because both ops require the record. After an
//     unfenced delete it is dropped. After a fenced delete it becomes a
//     replace, as described above.
//   - write after create_only/update_only is promoted to write, so the write
//     is not nacked by the preceding existence check.
//   - create_only after write/replace is dropped: it cannot succeed against
//     the record those ops leave, and folding into create_only would nack the
//     write as KEY_EXISTS. After replace_only both checks are dropped and the
//     result is a replace carrying both payloads. That is a compromise: the
//     sequential outcomes disagree, and one command cannot fail either message.
//
// When both sides are fenced, a lower-fence payload is ignored — the server
// would FILTERED_OUT it. A higher-fence payload is merged with the rules above
// and carries that fence, so [write{a}@3, write{b}@10] stays write{a,b}@10
// rather than replacing the payload with only {b}.
//
// Generation is deliberately not merged here: coalesceOps never folds an
// operation carrying a compare-and-set check, because one command can only
// carry one expected generation.
func (p *pendingOp) fold(next *pendingOp) {
	p.indexes = append(p.indexes, next.indexes...)

	if p.hasFence && next.hasFence && next.fence < p.fence {
		return
	}

	p.ttl = next.ttl
	// The delete's own fence decides whether a tombstone was left behind.
	// next's fence is copied just below, so read this first.
	priorFenced := p.hasFence
	if next.hasFence {
		p.fence, p.hasFence = next.fence, true
	}

	switch next.kind {
	case opDelete, opReplace:
		p.take(next)
	case opReplaceOnly:
		switch p.kind {
		case opDelete:
			if priorFenced {
				// A fenced delete leaves the tombstone record, so replace_only
				// succeeds and removes it. Promote to replace: REPLACE_ONLY
				// would KEY_NOT_FOUND when this delete created the tombstone.
				p.take(next)
				p.kind = opReplace
			}
			// Otherwise keep the delete. Sequential replace_only fails after an
			// unfenced delete, and folding it in would nack that delete as
			// KEY_NOT_FOUND.
		case opUpdateOnly, opReplaceOnly:
			// Both require the record to exist, and replace_only is the full
			// record. A missing key fails the folded command, which is what
			// each message would have done on its own.
			p.take(next)
		default:
			// write, replace, and create_only can create the record, so a
			// folded replace_only would KEY_NOT_FOUND and nack that write.
			// Drop the check and keep the full-record payload.
			p.kind = opReplace
			p.resetBins()
			p.copyBinsFrom(next)
		}
	case opCreateOnly:
		switch p.kind {
		case opWrite, opReplace:
			// create_only cannot succeed against the record those ops leave.
			// Absorb it so the preceding write is not nacked as KEY_EXISTS.
		case opReplaceOnly:
			// One command cannot keep both existence checks, and the
			// sequential outcomes disagree: replace_only's bins if the key
			// exists, create_only's bins if it does not. Promote to replace
			// carrying both payloads so neither message is nacked. This does
			// not replay either outcome.
			p.kind = opReplace
			p.copyBinsFrom(next)
		case opUpdateOnly:
			// One command cannot keep both existence checks. Promote to write
			// so neither message is poisoned by the other's constraint.
			p.kind = opWrite
			p.copyBinsFrom(next)
		case opDelete:
			// Sequential delete then create is a full-record rewrite.
			p.kind = opReplace
			p.resetBins()
			p.copyBinsFrom(next)
		default:
			p.take(next)
		}
	case opUpdateOnly:
		switch p.kind {
		case opDelete:
			if priorFenced {
				// A fenced delete leaves the tombstone record, so update_only
				// lands on it. Promote to replace so bins the delete removed
				// do not survive, and so a tombstone this delete created is
				// not a KEY_NOT_FOUND.
				p.take(next)
				p.kind = opReplace
			}
			// Otherwise keep the delete. Sequential update_only fails after an
			// unfenced delete, and folding it in would nack that delete as
			// KEY_NOT_FOUND.
		case opCreateOnly:
			// One command cannot keep both existence checks. Promote to write
			// so neither message is poisoned by the other's constraint.
			p.kind = opWrite
			p.copyBinsFrom(next)
		default:
			p.copyBinsFrom(next)
		}
	case opWrite:
		switch p.kind {
		case opDelete:
			p.kind = opReplace
			p.resetBins()
		case opCreateOnly, opUpdateOnly:
			p.kind = opWrite
		case opReplaceOnly:
			// Drop the existence check so the write is not nacked, and stay a
			// full-record replace so bins already on the server do not survive.
			p.kind = opReplace
		}
		p.copyBinsFrom(next)
	}

	// A later write or update_only can null every bin of an earlier replace.
	// That command would send no operations, and a record with no bins does
	// not exist, so the fold is a delete. An unfenced all-null replace or
	// replace_only never reaches here: mapMessage rejects that message on its own.
	//
	// replace_only keeps its existence check. The delete fails when the key
	// is missing, which is what [replace_only{a:1}, update_only{a:null}]
	// would do on its own. A replace would have created the record, so a
	// missing key is already the end state and the delete succeeds.
	// Fencing always carries the fence bin, so a fenced result is not empty.
	if p.kind != opDelete {
		p.deleteRequiresRecord = false
	}
	if p.hasFence || p.kind == opDelete {
		return
	}
	if len(p.putOps(p.kind.recordExistsAction())) == 0 {
		p.deleteRequiresRecord = p.kind == opReplaceOnly
		p.kind = opDelete
		p.resetBins()
	}
}

// coalesceOps folds repeated writes to one record key into a single command,
// preserving the outcome of applying them in sequence.
//
// A key touched by any compare-and-set message is exempt and keeps one command
// per message. A generation check is a statement about the specific record the
// caller read, and a folded command can carry only one expected generation:
// folding would either drop the check (letting a write land on a record that
// changed underneath it) or apply a generation the server never reached
// (failing both messages). Neither is acceptable silently, so CAS wins over
// coalescing and the server arbitrates.
func coalesceOps(ops []*pendingOp) []*pendingOp {
	ids := make([]string, len(ops))
	exempt := map[string]bool{}
	for i, op := range ops {
		ids[i] = keyID(op.key)
		if op.hasGeneration {
			exempt[ids[i]] = true
		}
	}

	out := make([]*pendingOp, 0, len(ops))
	byKey := make(map[string]*pendingOp, len(ops))
	for i, op := range ops {
		id := ids[i]
		if exempt[id] {
			out = append(out, op)
			continue
		}
		if prev, exists := byKey[id]; exists {
			prev.fold(op)
			continue
		}
		byKey[id] = op
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
	fence      *service.MessageBatchInterpolationExecutor
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
	if conf.fenceEnabled {
		m.fence = batch.InterpolationExecutor(conf.fenceValue)
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
	if m.conf.fenceEnabled {
		fence, err := m.resolveFence(index)
		if err != nil {
			return nil, err
		}
		op.fence, op.hasFence = fence, true
		op.setBin(m.conf.fenceBin, fence)
		if kind == opDelete {
			op.setBin(m.conf.tombstoneBin, true)
			return op, nil
		}
	} else if kind == opDelete {
		return op, nil
	}

	if err := m.applyBins(op, index); err != nil {
		return nil, err
	}

	if m.conf.fenceEnabled && kind != opReplace && kind != opReplaceOnly && kind != opCreateOnly {
		// UPDATE merges. A prior fenced delete left a tombstone bin that must
		// be removed or readers will still treat the record as missing.
		// replace and replace_only delete bins they do not name, and
		// create_only fails while the tombstone record exists, so those three
		// do not clear it here.
		op.setBin(m.conf.tombstoneBin, nil)
	}

	if len(op.binOrder) == 0 {
		return nil, fmt.Errorf("mapping in field '%v' produced no bins; a record must have at least one bin", aoFieldBins)
	}

	// Reject this message before coalescing. Folding an all-null replace into
	// an earlier write would delete the record that write just created.
	// Fencing has already stored the fence bin, so a fenced payload still has
	// a value to send. write and update_only keep a null: that put deletes the bin.
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

func (m *batchMapper) resolveFence(index int) (int64, error) {
	raw, err := m.fence.TryString(index)
	if err != nil {
		return 0, fmt.Errorf("interpolating '%v.%v': %w", aoFieldFencing, aoFieldFencingValue, err)
	}
	fence, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("field '%v.%v' resolved to %q, which is not an integer: %w", aoFieldFencing, aoFieldFencingValue, raw, err)
	}
	return fence, nil
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
