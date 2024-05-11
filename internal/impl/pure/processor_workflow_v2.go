package pure

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/gabs/v2"
	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	wflowProcFieldMetaPathV2        = "metapath"
	wflowProcFieldAdjacencyMatrixV2 = "adjacency_matrix"
	wflowProcFieldDependencyListV2  = "dependency_list"
	wflowProcFieldBranchesV2        = "branches"
)

func workflowProcSpecV2() *service.ConfigSpec {
	return service.NewConfigSpec().
		Fields(
			service.NewStringField(wflowProcFieldMetaPathV2).
				Description("A [dot path](/docs/configuration/field_paths) indicating where to store and reference [structured metadata](#structured-metadata) about the workflow execution.").
				Default("meta.workflow"),
			service.NewStringListOfListsField(wflowProcFieldAdjacencyMatrixV2).
				Description("An explicit declaration of branch ordered tiers, which describes the order in which parallel tiers of branches should be executed. Branches should be identified by the name as they are configured in the field `branches`. It's also possible to specify branch processors configured [as a resource](#resources).").
				Examples(
					[]any{[]any{"foo", "bar"}, []any{"baz"}},
					[]any{[]any{"foo"}, []any{"bar"}, []any{"baz"}},
				),
			service.NewObjectMapField(wflowProcFieldBranchesV2, branchSpecFieldsV2()...).
				Description("An object of named [`branch` processors](/docs/components/processors/branch) that make up the workflow. The order and parallelism in which branches are executed can either be made explicit with the field `order`, or if omitted an attempt is made to automatically resolve an ordering based on the mappings of each branch."))
}

func init() {
	err := service.RegisterBatchProcessor(
		"workflow_v2", workflowProcSpecV2(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			w, err := NewWorkflowV2(conf, interop.UnwrapManagement(mgr))
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(w), nil
		})
	if err != nil {
		panic(err)
	}

}

//------------------------------------------------------------------------------

// Workflow is a processor that applies a list of child processors to a new
// payload mapped from the original, and after processing attempts to overlay
// the results back onto the original payloads according to more mappings.
type WorkflowV2 struct {
	log    log.Modular
	tracer trace.TracerProvider

	children  *workflowBranchMapV2
	allStages map[string]struct{}
	metaPath  []string

	// Metrics
	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewWorkflow instanciates a new workflow processor.
func NewWorkflowV2(conf *service.ParsedConfig, mgr bundle.NewManagement) (*WorkflowV2, error) {

	stats := mgr.Metrics()
	w := &WorkflowV2{
		log:    mgr.Logger(),
		tracer: mgr.Tracer(),

		metaPath:  nil,
		allStages: map[string]struct{}{},

		mReceived:      stats.GetCounter("processor_received"),
		mBatchReceived: stats.GetCounter("processor_batch_received"),
		mSent:          stats.GetCounter("processor_sent"),
		mBatchSent:     stats.GetCounter("processor_batch_sent"),
		mError:         stats.GetCounter("processor_error"),
		mLatency:       stats.GetTimer("processor_latency_ns"),
	}

	metaStr, err := conf.FieldString(wflowProcFieldMetaPathV2)
	if err != nil {
		return nil, err
	}
	if metaStr != "" {
		w.metaPath = gabs.DotPathToSlice(metaStr)
	}

	if w.children, err = newWorkflowBranchMapV2(conf, mgr); err != nil {
		return nil, err
	}
	for k := range w.children.Branches {
		w.allStages[k] = struct{}{}
	}

	return w, nil

}

type resultTrackerV2 struct {
	notStarted map[string]struct{}
	running    map[string]struct{}
	succeeded  map[string]struct{}
	skipped    map[string]struct{}
	failed     map[string]string
	sync.Mutex
}

func trackerFromDagV2(dag [][]string) *resultTrackerV2 {
	r := &resultTrackerV2{
		notStarted: map[string]struct{}{},
		running:    map[string]struct{}{},
		succeeded:  map[string]struct{}{},
		skipped:    map[string]struct{}{},
		failed:     map[string]string{},
	}

	for i := range dag {
		node_name := string(byte('A' + i))
		r.notStarted[node_name] = struct{}{}
	}

	return r
}

func (r *resultTrackerV2) SkippedV2(k string) {
	r.Lock()
	delete(r.notStarted, k)

	r.skipped[k] = struct{}{}
	r.Unlock()
}

func (r *resultTrackerV2) Succeeded(k string) {
	r.Lock()
	delete(r.running, k)

	r.succeeded[k] = struct{}{}
	r.Unlock()
}

func (r *resultTrackerV2) Started(k string) {
	r.Lock()
	delete(r.notStarted, k)

	r.running[k] = struct{}{}
	r.Unlock()
}

func (r *resultTrackerV2) FailedV2(k, why string) {
	r.Lock()
	delete(r.running, k)
	delete(r.skipped, k)

	r.failed[k] = why
	r.Unlock()
}

func (r *resultTrackerV2) ToObjectV2() map[string]any {
	succeeded := make([]any, 0, len(r.succeeded))
	skipped := make([]any, 0, len(r.skipped))
	notStarted := make([]any, 0, len(r.notStarted))
	running := make([]any, 0, len(r.running))
	failed := make(map[string]any, len(r.failed))

	for k := range r.succeeded {
		succeeded = append(succeeded, k)
	}
	sort.Slice(succeeded, func(i, j int) bool {
		return succeeded[i].(string) < succeeded[j].(string)
	})
	for k := range r.skipped {
		skipped = append(skipped, k)
	}
	for k := range r.notStarted {
		notStarted = append(notStarted, k)
	}
	for k := range r.running {
		running = append(running, k)
	}
	sort.Slice(skipped, func(i, j int) bool {
		return skipped[i].(string) < skipped[j].(string)
	})
	for k, v := range r.failed {
		failed[k] = v
	}

	m := map[string]any{}
	if len(succeeded) > 0 {
		m["succeeded"] = succeeded
	}
	if len(skipped) > 0 {
		m["skipped"] = skipped
	}
	if len(notStarted) > 0 {
		m["notStarted"] = notStarted
	}
	if len(running) > 0 {
		m["running"] = running
	}
	if len(failed) > 0 {
		m["failed"] = failed
	}
	return m
}

// Returns a map of enrichment IDs that should be skipped for this payload.
func (w *WorkflowV2) skipFromMetaV2(root any) map[string]struct{} {
	skipList := map[string]struct{}{}
	if len(w.metaPath) == 0 {
		return skipList
	}

	gObj := gabs.Wrap(root)

	// If a whitelist is provided for this flow then skip stages that aren't
	// within it.
	if apply, ok := gObj.S(append(w.metaPath, "apply")...).Data().([]any); ok {
		if len(apply) > 0 {
			for k := range w.allStages {
				skipList[k] = struct{}{}
			}
			for _, id := range apply {
				if idStr, isString := id.(string); isString {
					delete(skipList, idStr)
				}
			}
		}
	}

	// Skip stages that already succeeded in a previous run of this workflow.
	if succeeded, ok := gObj.S(append(w.metaPath, "succeeded")...).Data().([]any); ok {
		for _, id := range succeeded {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	// Skip stages that were already skipped in a previous run of this workflow.
	if skipped, ok := gObj.S(append(w.metaPath, "skipped")...).Data().([]any); ok {
		for _, id := range skipped {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	return skipList
}

// ProcessBatch applies workflow stages to each part of a message type.
func (w *WorkflowV2) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	w.mReceived.Incr(int64(msg.Len()))
	w.mBatchReceived.Incr(1)
	startedAt := time.Now()

	// Prevent resourced branches from being updated mid-flow.
	dag, children, unlock, err := w.children.LockV2()
	if err != nil {
		w.mError.Incr(1)
		w.log.Error("Failed to establish workflow: %v\n", err)

		_ = msg.Iter(func(i int, p *message.Part) error {
			p.ErrorSet(err)
			return nil
		})
		w.mSent.Incr(int64(msg.Len()))
		w.mBatchSent.Incr(1)
		return []message.Batch{msg}, nil
	}
	defer unlock()

	skipOnMeta := make([]map[string]struct{}, msg.Len())
	_ = msg.Iter(func(i int, p *message.Part) error {
		// TODO: Do we want to evaluate bytes here? And metadata? (TODO from original workflow processor)
		if jObj, err := p.AsStructured(); err == nil {
			skipOnMeta[i] = w.skipFromMetaV2(jObj)
		} else {
			skipOnMeta[i] = map[string]struct{}{}
		}
		return nil
	})

	propMsg, _ := tracing.WithChildSpans(w.tracer, "workflow", msg)

	records := make([]*resultTrackerV2, msg.Len())
	for i := range records {
		records[i] = trackerFromDagV2(dag)
	}

	type collector struct {
		eid     string
		results [][]*message.Part
	}

	done := make(chan collector)

	go func() {
		mssge := <-done
		errors := make([]error, 1)
		var failed []branchMapError
		err := errors[0]
		if err == nil {
			failed, err = children[mssge.eid].overlayResult(msg, mssge.results[0])
		}
		if err != nil {
			w.mError.Incr(1)
			w.log.Error("Failed to perform enrichment '%v': %v\n", mssge.eid, err)
			for j := range records {
				records[j].FailedV2(mssge.eid, err.Error())
			}
		}
		for _, e := range failed {
			records[e.index].FailedV2(mssge.eid, e.err.Error())
		}
	}()

	numberOfBranches := len(records[0].notStarted)

	for len(records[0].succeeded) != numberOfBranches {
		for eid := range records[0].notStarted {

			results := make([][]*message.Part, 6) // TODO: remove literal int
			errors := make([]error, 6)            // TODO: remove literal int

			if isColumnAllZeros(dag, int(eid[0]-'A')) { // TODO: get rid of this branch name -> matrix column conversion
				records[0].Started(eid) // TODO: remove literal

				branchMsg, branchSpans := tracing.WithChildSpans(w.tracer, eid, propMsg.ShallowCopy())

				go func(id string, index int) {

					branchParts := make([]*message.Part, branchMsg.Len())
					_ = branchMsg.Iter(func(partIndex int, part *message.Part) error {
						// Remove errors so that they aren't propagated into the
						// branch.
						part.ErrorSet(nil)
						if _, exists := skipOnMeta[partIndex][id]; !exists {
							branchParts[partIndex] = part
						}
						return nil
					})

					var mapErrs []branchMapError
					results[index], mapErrs, errors[index] = children[id].createResult(ctx, branchParts, propMsg.ShallowCopy())
					for _, s := range branchSpans {
						s.Finish()
					}
					for j, p := range results[index] {
						if p == nil {
							records[j].SkippedV2(id)
						}
					}
					for _, e := range mapErrs {
						records[e.index].FailedV2(id, e.err.Error())
					}
					for j := range results[index] {
						records[j].Succeeded((id))
					}
					dag = zeroOutRow(dag, index)
					dag = updateColumnDone(dag, index)
					asdf := collector{
						eid:     id,
						results: results,
					}
					done <- asdf
				}(eid, int(eid[0]-'A'))
			}
		}
	}

	// Finally, set the meta records of each document.
	if len(w.metaPath) > 0 {
		_ = msg.Iter(func(i int, p *message.Part) error {
			pJSON, err := p.AsStructuredMut()
			if err != nil {
				w.mError.Incr(1)
				w.log.Error("Failed to parse message for meta update: %v\n", err)
				p.ErrorSet(err)
				return nil
			}

			gObj := gabs.Wrap(pJSON)
			previous := gObj.S(w.metaPath...).Data()
			current := records[i].ToObjectV2()
			if previous != nil {
				current["previous"] = previous
			}
			_, _ = gObj.Set(current, w.metaPath...)

			p.SetStructuredMut(gObj.Data())
			return nil
		})
	} else {
		_ = msg.Iter(func(i int, p *message.Part) error {
			if lf := len(records[i].failed); lf > 0 {
				failed := make([]string, 0, lf)
				for k := range records[i].failed {
					failed = append(failed, k)
				}
				sort.Strings(failed)
				p.ErrorSet(fmt.Errorf("workflow branches failed: %v", failed))
			}
			return nil
		})
	}

	tracing.FinishSpans(propMsg)

	w.mSent.Incr(int64(msg.Len()))
	w.mBatchSent.Incr(1)
	w.mLatency.Timing(time.Since(startedAt).Nanoseconds())

	return []message.Batch{msg}, nil
}

// Close shuts down the processor and stops processing requests.
func (w *WorkflowV2) Close(ctx context.Context) error {
	return w.children.Close(ctx)
}

func isColumnAllZeros(matrix [][]string, columnIdx int) bool {
	for i := 0; i < len(matrix); i++ {
		if matrix[i][columnIdx] != "0" {
			return false
		}
	}
	return true
}

func zeroOutRow(matrix [][]string, rowIdx int) [][]string {
	for i := 0; i < len(matrix); i++ {
		matrix[rowIdx][i] = "0"
	}
	return matrix
}

func updateColumnDone(matrix [][]string, rowIdx int) [][]string {
	matrix[rowIdx][rowIdx] = "1"
	return matrix
}

func branchSpecFieldsV2() []*service.ConfigField {
	return []*service.ConfigField{
		service.NewBloblangField(branchProcFieldReqMap).
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that describes how to create a request payload suitable for the child processors of this branch. If left empty then the branch will begin with an exact copy of the origin message (including metadata).").
			Examples(`root = {
	"id": this.doc.id,
	"content": this.doc.body.text
}`,
				`root = if this.type == "foo" {
	this.foo.request
} else {
	deleted()
}`).
			Default(""),
		service.NewStringListField(wflowProcFieldDependencyListV2).
			Description("TODO"),
		service.NewProcessorListField(branchProcFieldProcs).
			Description("A list of processors to apply to mapped requests. When processing message batches the resulting batch must match the size and ordering of the input batch, therefore filtering, grouping should not be performed within these processors."),
		service.NewBloblangField(branchProcFieldResMap).
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that describes how the resulting messages from branched processing should be mapped back into the original payload. If left empty the origin message will remain unchanged (including metadata).").
			Examples(`meta foo_code = meta("code")
root.foo_result = this`,
				`meta = meta()
root.bar.body = this.body
root.bar.id = this.user.id`,
				`root.raw_result = content().string()`,
				`root.enrichments.foo = if meta("request_failed") != null {
  throw(meta("request_failed"))
} else {
  this
}`).
			Default(""),
	}
}
