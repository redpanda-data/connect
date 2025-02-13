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
	wflowProcFieldMetaPathV2       = "metapath"
	wflowProcFieldDependencyListV2 = "dependency_list"
	wflowProcFieldBranchesV2       = "branches"
)

func workflowProcSpecV2() *service.ConfigSpec {
	return service.NewConfigSpec().
		Fields(
			service.NewStringField(wflowProcFieldMetaPathV2).
				Description("A [dot path](/docs/configuration/field_paths) indicating where to store and reference [structured metadata](#structured-metadata) about the workflow execution.").
				Default("meta.workflow"),
			service.NewObjectMapField(wflowProcFieldBranchesV2, branchSpecFieldsV2()...).
				Description("An object of named [`branch` processors](/docs/components/processors/branch) that make up the workflow. The order and parallelism in which branches are executed can either be made explicit with the field `order`, or if omitted an attempt is made to automatically resolve an ordering based on the mappings of each branch."))
}

// Copy from processor_branch.go
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
	notStarted        map[string]struct{}
	started           map[string]struct{}
	succeeded         map[string]struct{}
	failed            map[string]string
	dependencyTracker map[string][]string
	numberOfBranches  int
	sync.Mutex
}

func createTrackerFromDependencies(dependencies map[string][]string) *resultTrackerV2 {
	r := &resultTrackerV2{
		notStarted: map[string]struct{}{},
		started:    map[string]struct{}{},
		succeeded:  map[string]struct{}{},
		failed:     map[string]string{},
	}

	for k := range dependencies {
		r.notStarted[k] = struct{}{}
	}

	r.numberOfBranches = len(dependencies)
	r.dependencyTracker = dependencies

	return r
}

func (r *resultTrackerV2) Succeeded(k string) {
	r.Lock()
	delete(r.started, k)

	r.succeeded[k] = struct{}{}
	r.RemoveFromDepTracker(k)
	r.Unlock()
}

func (r *resultTrackerV2) Started(k string) {
	r.Lock()
	delete(r.notStarted, k)

	r.started[k] = struct{}{}
	r.Unlock()
}

func (r *resultTrackerV2) FailedV2(k, why string) {
	r.Lock()
	delete(r.started, k)
	delete(r.succeeded, k)
	r.failed[k] = why
	r.Unlock()
}

func (r *resultTrackerV2) RemoveFromDepTracker(k string) {
	for key, values := range r.dependencyTracker {
		var updatedValues []string
		for _, value := range values {
			if value != k {
				updatedValues = append(updatedValues, value)
			}
		}
		r.dependencyTracker[key] = updatedValues
	}
}

func (r *resultTrackerV2) isReadyToStart(k string) bool {
	r.Lock()
	defer r.Unlock()
	return len(r.dependencyTracker[k]) == 0
}

func (r *resultTrackerV2) ToObjectV2() map[string]any {
	succeeded := make([]any, 0, len(r.succeeded))
	notStarted := make([]any, 0, len(r.notStarted))
	started := make([]any, 0, len(r.started))
	failed := make(map[string]any, len(r.failed))

	for k := range r.succeeded {
		succeeded = append(succeeded, k)
	}
	sort.Slice(succeeded, func(i, j int) bool {
		return succeeded[i].(string) < succeeded[j].(string)
	})
	for k := range r.notStarted {
		notStarted = append(notStarted, k)
	}
	for k := range r.started {
		started = append(started, k)
	}
	for k, v := range r.failed {
		failed[k] = v
	}

	m := map[string]any{}
	if len(succeeded) > 0 {
		m["succeeded"] = succeeded
	}
	if len(notStarted) > 0 {
		m["notStarted"] = notStarted
	}
	if len(started) > 0 {
		m["started"] = started
	}
	if len(failed) > 0 {
		m["failed"] = failed
	}
	return m
}

// ProcessBatch applies workflow stages to each part of a message type.
func (w *WorkflowV2) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	w.mReceived.Incr(int64(msg.Len()))
	w.mBatchReceived.Incr(1)
	startedAt := time.Now()

	// Prevent resourced branches from being updated mid-flow.
	children, dependencies, unlock, err := w.children.LockV2()
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
		skipOnMeta[i] = map[string]struct{}{}
		return nil
	})

	propMsg, _ := tracing.WithChildSpans(w.tracer, "workflow", msg)

	records := createTrackerFromDependencies(dependencies)

	type collector struct {
		eid     string
		results [][]*message.Part
		errors  []error
	}

	batchResultChan := make(chan collector)

	go func() {
		mssge := <-batchResultChan
		var failed []branchMapError
		err := mssge.errors[0]
		if err == nil {
			failed, err = children[mssge.eid].overlayResult(msg, mssge.results[0])
		}
		if err != nil {
			w.mError.Incr(1)
			w.log.Error("Failed to perform enrichment '%v': %v\n", mssge.eid, err)
			records.FailedV2(mssge.eid, err.Error())
		}
		for _, e := range failed {
			records.FailedV2(mssge.eid, e.err.Error())
		}
	}()

	for len(records.succeeded)+len(records.failed) != records.numberOfBranches {
		for eid := range records.notStarted {

			results := make([][]*message.Part, 1)
			errors := make([]error, 1)

			if records.isReadyToStart(eid) {
				records.Started(eid)

				branchMsg, branchSpans := tracing.WithChildSpans(w.tracer, eid, propMsg.ShallowCopy())

				go func(id string) {
					branchParts := make([]*message.Part, branchMsg.Len())
					_ = branchMsg.Iter(func(partIndex int, part *message.Part) error {
						// Remove errors so that they aren't propagated into the
						// branch.
						part.ErrorSet(nil)
						branchParts[partIndex] = part
						return nil
					})

					var mapErrs []branchMapError
					results[0], mapErrs, errors[0] = children[id].createResult(ctx, branchParts, propMsg.ShallowCopy())
					for _, s := range branchSpans {
						s.Finish()
					}
					records.Succeeded((id))
					for _, e := range mapErrs {
						records.FailedV2(id, e.err.Error())
					}
					batchResult := collector{
						eid:     id,
						results: results,
						errors:  errors,
					}
					batchResultChan <- batchResult
				}(eid)
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
			current := records.ToObjectV2()
			if previous != nil {
				current["previous"] = previous
			}
			_, _ = gObj.Set(current, w.metaPath...)

			p.SetStructuredMut(gObj.Data())
			return nil
		})
	} else {
		_ = msg.Iter(func(i int, p *message.Part) error {
			if lf := len(records.failed); lf > 0 {
				failed := make([]string, 0, lf)
				for k := range records.failed {
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
