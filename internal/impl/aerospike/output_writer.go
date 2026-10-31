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
	"strings"
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
	// An unbatched database sink round-trips per message. If the user has not
	// expressed a preference, batch by default rather than shipping a pipeline
	// that is an order of magnitude slower than it should be. The framework
	// renders an unset policy as `count: 0` in the config reference, so say out
	// loud what actually got applied.
	if batchPolicy.IsNoop() {
		batchPolicy.Count = defaultBatchCount
		batchPolicy.Period = defaultBatchPeriod
		mgr.Logger().Infof(
			"No batching policy set, defaulting to %d messages or %s; set batching.count to 1 to write one message per command",
			defaultBatchCount, defaultBatchPeriod)
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
	if names := parsed.client.insecureCiphers; len(names) > 0 {
		mgr.Logger().Warnf("field '%s' includes insecure cipher suites: %s", aoFieldCiphers, strings.Join(names, ", "))
	}

	return &aerospikeWriter{
		conf:        parsed,
		conn:        newConnection(parsed.client, mgr.Logger()),
		log:         mgr.Logger(),
		filteredOut: mgr.Metrics().NewCounter("aerospike_filtered_out"),
		ignored:     mgr.Metrics().NewCounter("aerospike_ignored_errors"),
	}, batchPolicy, maxInFlight, nil
}

type aerospikeWriter struct {
	conf        *aerospikeConfig
	conn        *connection
	log         *service.Logger
	filteredOut *service.MetricCounter
	ignored     *service.MetricCounter
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

	if len(ops) > 0 {
		records := make([]as.BatchRecordIfc, len(ops))
		for i, op := range ops {
			records[i] = w.buildRecord(op)
		}

		policy, err := batchPolicyForContext(ctx, w.conf.batchPolicy)
		if err != nil {
			return err
		}

		batchErr := operate(policy, records)
		if batchErr != nil && isConnectionError(batchErr) {
			if w.log != nil {
				w.log.Errorf("Aerospike batch command failed: %v", batchErr)
			}
			return service.ErrNotConnected
		}

		// A one-record rejection comes back as the command error and as the
		// record code. handleRecord counts that record. When every record was
		// accepted or ignored, a listed command error must not fail the batch.
		// A record still at NO_RESPONSE keeps its own result. After a write
		// error the client marks those records in doubt, and errors.As reports
		// only the first node error, so that code can belong to another node.
		var asErr *as.AerospikeError
		commandListed := batchErr != nil && errors.As(batchErr, &asErr) && !asErr.InDoubt && w.conf.ignores(asErr.ResultCode)
		for i, op := range ops {
			rec := records[i].BatchRec()
			if err := w.handleRecord(rec, op); err != nil {
				for _, idx := range op.indexes {
					failures[idx] = err
				}
			}
		}

		if batchErr != nil && len(failures) == 0 && !commandListed {
			return fmt.Errorf("aerospike batch command failed: %w", batchErr)
		}
	}

	if len(failures) == 0 {
		return nil
	}

	err := service.NewBatchError(batch, fmt.Errorf(
		"%v of %v messages did not write to aerospike", len(failures), len(batch)))
	for idx, msgErr := range failures {
		err = err.Failed(idx, msgErr)
	}
	return err
}

// planBatch maps every message to a pending write and, unless disabled,
// coalesces repeats of the same record key.
//
// Coalescing matters more than it looks: a topic partition routinely delivers
// several messages for one key inside a single batch, and Aerospike limits
// concurrent commands per record. Sending them as separate batch entries
// contends on that one record — visible as KEY_BUSY, or as latency inflation
// with no error at all when it stays under the limit. Folding them into one
// write also removes any question of ordering within the batch.
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

	if w.conf.coalesce {
		ops = coalesceOps(ops)
	}
	return dropBinlessWrites(ops, failures), failures
}

// dropBinlessWrites rejects a command that would send no bin operations.
// mapMessage already rejects an all-null replace or replace_only, and fold
// turns a folded empty replace into a delete, so this is the backstop.
func dropBinlessWrites(ops []*pendingOp, failures map[int]error) []*pendingOp {
	kept := make([]*pendingOp, 0, len(ops))
	for _, op := range ops {
		if op.kind == opDelete && !op.hasFence {
			kept = append(kept, op)
			continue
		}
		action := op.kind.recordExistsAction()
		if op.kind == opDelete {
			action = as.REPLACE
		}
		if len(op.putOps(action)) > 0 {
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
	if op.kind == opDelete && !op.hasFence {
		if !op.hasGeneration {
			return as.NewBatchDelete(w.conf.deletePolicy, op.key)
		}
		policy := *w.conf.deletePolicy
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = op.generation
		return as.NewBatchDelete(&policy, op.key)
	}

	// Copy the shared policy so per-message expiration, fencing, and generation
	// checks do not race across concurrent batches.
	policy := *w.conf.writePolicy
	policy.RecordExistsAction = op.kind.recordExistsAction()
	policy.Expiration = op.ttl
	if op.kind == opDelete {
		// A fenced delete is a replace that leaves the fence on the key.
		policy.RecordExistsAction = as.REPLACE
		policy.Expiration = w.conf.tombstoneTTL
	}
	if op.hasFence {
		policy.FilterExpression = fenceExpression(w.conf.fenceBin, op.fence)
	}
	if op.hasGeneration {
		policy.GenerationPolicy = as.EXPECT_GEN_EQUAL
		policy.Generation = op.generation
	}

	return as.NewBatchWrite(&policy, op.key, op.putOps(policy.RecordExistsAction)...)
}

// putOps is the bin puts buildRecord sends. REPLACE and REPLACE_ONLY already
// drop bins that are not named. A nil value is a bin delete, and the server
// rejects that put as a parameter error on those two actions. update_only
// after a fenced delete is one such write: it clears the tombstone with a nil
// bin, then fold promotes it to replace. mapMessage rejects a replace or
// replace_only that this leaves empty. fold turns a folded empty one into a
// delete, and dropBinlessWrites is the backstop.
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

func filteredOutCount(rec *as.BatchRecord, op *pendingOp) int {
	if rec.ResultCode != types.FILTERED_OUT {
		return 0
	}
	return ignoredMessages(op)
}

func ignoredMessages(op *pendingOp) int {
	n := len(op.indexes)
	if n == 0 {
		n = 1
	}
	return n
}

func (w *aerospikeWriter) handleRecord(rec *as.BatchRecord, op *pendingOp) error {
	if n := filteredOutCount(rec, op); n > 0 {
		if w.filteredOut != nil {
			w.filteredOut.Incr(int64(n))
		}
		if w.log != nil {
			w.log.Debugf("aerospike fenced out %d message(s) for key %v", n, rec.Key)
		}
	}
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

// noteIgnored records a dropped message. The counter is separate from
// aerospike_filtered_out, which counts fencing discards that did reach the server.
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

// fenceExpression admits a write only when the record is new, or when the
// stored fence is strictly older than this message's. Evaluated on the server,
// so a redelivered or out-of-order message is discarded without a round trip
// and without a transaction.
func fenceExpression(bin string, fence int64) *as.Expression {
	return as.ExpOr(
		as.ExpNot(as.ExpBinExists(bin)),
		as.ExpLess(as.ExpIntBin(bin), as.ExpIntVal(fence)),
	)
}
