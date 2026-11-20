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
	"sync/atomic"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func init() {
	service.MustRegisterBatchOutput("aerospike", outputSpec(), newAerospikeOutput)
}

func newAerospikeOutput(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
	batchPolicy, err := conf.FieldBatchPolicy(aoFieldBatching)
	if err != nil {
		return nil, batchPolicy, 0, err
	}
	maxInFlight, err := conf.FieldMaxInFlight()
	if err != nil {
		return nil, batchPolicy, 0, err
	}

	parsed, err := parseOutputConfig(conf)
	if err != nil {
		return nil, batchPolicy, 0, err
	}
	parsed.client.sizePoolForConcurrency(maxInFlight)

	return &aerospikeWriter{
		conf:    parsed,
		conn:    newConnection(parsed.client, mgr.Logger()),
		log:     mgr.Logger(),
		ignored: mgr.Metrics().NewCounter("aerospike_ignored_errors"),
	}, batchPolicy, maxInFlight, nil
}

type aerospikeWriter struct {
	conf    *aerospikeConfig
	conn    *connection
	log     *service.Logger
	ignored *service.MetricCounter
	// ignoredTotal is the same total passed to aerospike_ignored_errors.
	// Unit tests read it because they have no metrics registry.
	ignoredTotal atomic.Int64

	// operate, when set, replaces the live client's BatchOperate. Tests use
	// this to exercise WriteBatch without a cluster.
	operate func(*as.BatchPolicy, []as.BatchRecordIfc) error
}

func (w *aerospikeWriter) Connect(ctx context.Context) error {
	return w.conn.connect(ctx)
}

func (w *aerospikeWriter) Close(ctx context.Context) error {
	return w.conn.close(ctx)
}

func (w *aerospikeWriter) ConnectionTest(ctx context.Context) service.ConnectionTestResults {
	// A connection test should not warm a pool it is about to throw away.
	probe := *w.conf.client
	probe.WarmUp = false
	tmp := newConnection(&probe, w.log)
	if err := tmp.connect(ctx); err != nil {
		return service.ConnectionTestFailed(err).AsList()
	}
	_ = tmp.close(ctx)
	return service.ConnectionTestSucceeded().AsList()
}

func (w *aerospikeWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	operate := w.operate
	if operate == nil {
		client := w.conn.asClient()
		if client == nil {
			if w.log != nil {
				w.log.Error("Not connected to Aerospike")
			}
			return service.ErrNotConnected
		}
		operate = func(p *as.BatchPolicy, recs []as.BatchRecordIfc) error {
			if err := client.BatchOperate(p, recs); err != nil {
				return err
			}
			return nil
		}
	}

	ops, failures := w.planBatch(batch)
	rounds := splitRounds(ops)
	for r, round := range rounds {
		err := w.writeRound(ctx, operate, round, failures)
		if err == nil {
			continue
		}
		if r == 0 {
			return err
		}
		// Earlier rounds are written. Retrying the whole batch would write
		// them again, so only this round and the later ones fail. The error
		// must not wrap ErrNotConnected, which retries the whole batch.
		if errors.Is(err, service.ErrNotConnected) {
			err = errors.New("lost the connection to aerospike after earlier commands in this batch were written")
		}
		for _, later := range rounds[r:] {
			for _, op := range later {
				for _, idx := range op.indexes {
					failures[idx] = err
				}
			}
		}
		break
	}

	if len(failures) == 0 {
		return nil
	}

	summary := fmt.Errorf("%v of %v messages did not write to aerospike", len(failures), len(batch))
	// A batch of one is reported by its summary alone, for example in the
	// fallback_error metadata, so the summary has to carry the cause.
	if len(failures) == 1 {
		for _, msgErr := range failures {
			summary = fmt.Errorf("%w: %w", summary, msgErr)
		}
	}
	err := service.NewBatchError(batch, summary)
	for idx, msgErr := range failures {
		err = err.Failed(idx, msgErr)
	}
	return err
}

// writeRound sends one batch command and records each failed message in
// failures. It returns an error when the command failed as a whole.
func (w *aerospikeWriter) writeRound(ctx context.Context, operate func(*as.BatchPolicy, []as.BatchRecordIfc) error, ops []*pendingOp, failures map[int]error) error {
	records := make([]as.BatchRecordIfc, len(ops))
	for i, op := range ops {
		records[i] = w.buildRecord(op)
	}

	policy, err := batchPolicyForContext(ctx, w.conf.batchPolicy)
	if err != nil {
		return err
	}

	batchErr := operate(policy, records)
	// A connection error can end a batch after some nodes have answered.
	// Those records are applied and must not be written again, so the batch
	// is only treated as unsent when no record got a response.
	connErr := batchErr != nil && isConnectionError(batchErr)
	if connErr {
		if !anyResponse(records) {
			if w.log != nil {
				w.log.Errorf("Aerospike batch command failed: %v", batchErr)
			}
			return service.ErrNotConnected
		}
		if w.log != nil {
			w.log.Warnf("Aerospike batch command failed after some records were written, failing the rest: %v", batchErr)
		}
	}

	// A one-record rejection comes back as the command error and as the
	// record code. handleRecord counts that record. When every record was
	// accepted or ignored, a listed command error must not fail the batch.
	// A record still at NO_RESPONSE keeps its own result. After a write
	// error the client marks those records in doubt, and errors.As reports
	// only the first node error, so that code can belong to another node.
	var asErr *as.AerospikeError
	commandListed := batchErr != nil && errors.As(batchErr, &asErr) && !asErr.InDoubt && w.conf.ignores(asErr.ResultCode)
	failed := false
	for i, op := range ops {
		rec := records[i].BatchRec()
		if err := w.handleRecord(rec, op); err != nil {
			failed = true
			for _, idx := range op.indexes {
				failures[idx] = err
			}
		}
	}

	if batchErr != nil && !failed && !commandListed && !connErr {
		return fmt.Errorf("aerospike batch command failed: %w", batchErr)
	}
	return nil
}

// splitRounds groups ops by round, keeping message order within a round.
func splitRounds(ops []*pendingOp) [][]*pendingOp {
	var rounds [][]*pendingOp
	for _, op := range ops {
		for len(rounds) <= op.round {
			rounds = append(rounds, nil)
		}
		rounds[op.round] = append(rounds[op.round], op)
	}
	return rounds
}

// anyResponse reports whether a server answered for at least one record. The
// client leaves a record it never heard back about at NO_RESPONSE.
func anyResponse(records []as.BatchRecordIfc) bool {
	for _, r := range records {
		if r.BatchRec().ResultCode != types.NO_RESPONSE {
			return true
		}
	}
	return false
}

// planBatch maps every message to a pending write, folds repeats of the same
// record key unless coalescing is disabled, and puts the commands for one key
// that cannot be folded into successive rounds.
//
// Coalescing matters more than it looks: a topic partition routinely delivers
// several messages for one key inside a single batch, and Aerospike limits
// concurrent commands per record. Sending them as separate entries of one
// batch command contends on that one record — visible as KEY_BUSY, or as
// latency inflation with no error at all when it stays under the limit — and
// the server does not apply them in message order. Rounds keep that order.
func (w *aerospikeWriter) planBatch(batch service.MessageBatch) ([]*pendingOp, map[int]error) {
	failures := map[int]error{}
	ops := make([]*pendingOp, 0, len(batch))
	mapper := newBatchMapper(w.conf, batch)

	for i := range batch {
		op, err := mapper.mapMessage(i)
		if err != nil {
			if w.acknowledgeIgnoredMapping(err) {
				continue
			}
			failures[i] = err
			continue
		}
		ops = append(ops, op)
	}

	ops = coalesceOps(ops, w.conf.coalesce)
	return dropBinlessWrites(ops, failures), failures
}

// dropBinlessWrites rejects a command that would send no bin operations.
// mapMessage already rejects an all-null replace or replace_only, and fold
// turns a folded empty replace into a delete, so this is the backstop.
func dropBinlessWrites(ops []*pendingOp, failures map[int]error) []*pendingOp {
	kept := make([]*pendingOp, 0, len(ops))
	for _, op := range ops {
		if op.kind == opDelete || len(op.putOps(op.kind.recordExistsAction())) > 0 {
			kept = append(kept, op)
			continue
		}
		for _, idx := range op.indexes {
			failures[idx] = nullReplaceError(op.kind, len(op.indexes) > 1)
		}
	}
	return kept
}

func nullReplaceError(kind opKind, coalesced bool) error {
	err := fmt.Errorf("mapping in field '%v' produced only null bin values; %s drops bins it does not name and a null value is not sent, so this write would have no bins", aoFieldBins, kind)
	if coalesced {
		err = fmt.Errorf("%w; these messages were coalesced into one command", err)
	}
	return err
}

func (w *aerospikeWriter) buildRecord(op *pendingOp) as.BatchRecordIfc {
	if op.kind == opDelete {
		if !op.hasGeneration {
			return as.NewBatchDelete(w.conf.deletePolicy, op.key)
		}
		policy := *w.conf.deletePolicy
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = op.generation
		return as.NewBatchDelete(&policy, op.key)
	}

	// Copy the shared policy so per-message expiration and generation checks
	// do not race across concurrent batches.
	policy := *w.conf.writePolicy
	policy.RecordExistsAction = op.kind.recordExistsAction()
	policy.Expiration = op.ttl
	if op.hasGeneration {
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = op.generation
	}

	return as.NewBatchWrite(&policy, op.key, op.putOps(policy.RecordExistsAction)...)
}

// putOps is the bin puts buildRecord sends. REPLACE and REPLACE_ONLY already
// drop bins that are not named. A nil value is a bin delete, and the server
// rejects that put as a parameter error on those two actions. mapMessage
// rejects a replace or replace_only that this leaves empty. fold turns a
// folded empty one into a delete, and dropBinlessWrites is the backstop.
func (p *pendingOp) putOps(action as.RecordExistsAction) []*as.Operation {
	fullReplace := action == as.REPLACE || action == as.REPLACE_ONLY
	ops := make([]*as.Operation, 0, len(p.binOrder))
	for _, name := range p.binOrder {
		value := p.bins[name]
		if value == nil && fullReplace {
			continue
		}
		ops = append(ops, as.PutOp(as.NewBin(name, value)))
	}
	return ops
}

func ignoredMessages(op *pendingOp) int {
	n := len(op.indexes)
	if n == 0 {
		n = 1
	}
	return n
}

func (w *aerospikeWriter) handleRecord(rec *as.BatchRecord, op *pendingOp) error {
	err := classifyRecord(rec, op)
	// A result that is in doubt is not ignored. The write may already have been applied.
	if err != nil && !rec.InDoubt && w.conf.ignores(rec.ResultCode) {
		w.noteIgnored(rec.ResultCode, rec.Key, err, ignoredMessages(op))
		return nil
	}
	return err
}

// acknowledgeIgnoredMapping reports whether err is the local 15-byte bin-name
// rejection and code 21 is listed. The message is then acknowledged and is not
// sent. Any other mapping error stays a failure.
func (w *aerospikeWriter) acknowledgeIgnoredMapping(err error) bool {
	var long *longBinNameError
	if !errors.As(err, &long) || !w.conf.ignores(types.BIN_NAME_TOO_LONG) {
		return false
	}
	w.noteIgnored(types.BIN_NAME_TOO_LONG, long.key, err, 1)
	return true
}

// noteIgnored records a dropped message.
func (w *aerospikeWriter) noteIgnored(code types.ResultCode, key *as.Key, err error, n int) {
	w.ignoredTotal.Add(int64(n))
	if w.ignored != nil {
		w.ignored.Incr(int64(n))
	}
	if w.log == nil {
		return
	}
	ns, set, id := "", "", ""
	if key != nil {
		ns, set = key.Namespace(), key.SetName()
		if v := key.Value(); v != nil {
			id = v.String()
		}
	}
	w.log.Warnf("ignoring aerospike error code %d (%s) namespace=%s set=%s key=%s: %v",
		code, types.ResultCodeToString(code), ns, set, id, err)
}
