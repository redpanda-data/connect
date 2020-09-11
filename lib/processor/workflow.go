package processor

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
	"github.com/opentracing/opentracing-go"
	"github.com/quipo/dependencysolver"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWorkflow] = TypeSpec{
		constructor: NewWorkflow,
		Categories: []Category{
			CategoryComposition,
		},
		Beta: true,
		Summary: `
Executes a topology of ` + "[`branch` processors][processors.branch]" + `,
performing them in parallel where possible.`,
		Description: `
## Why Use a Workflow

### Performance

Most of the time the best way to compose processors is also the simplest, just configure them in series. This is because processors are often CPU bound, low-latency, and you can gain vertical scaling by increasing the number of processor pipeline threads, allowing Benthos to process [multiple messages in parallel][configuration.pipelines].

However, some processors such as ` + "[`http`][processors.http], [`lambda`][processors.lambda] or [`cache`][processors.cache]" + ` interact with external services and therefore spend most of their time waiting for a response. These processors tend to be high-latency and low CPU activity, which causes messages to process slowly.

When a processing pipeline contains multiple network processors that aren't dependent on each other we can benefit from performing these processors in parallel for each individual message, reducing the overall message processing latency.

### Simplifying Processor Topology

A workflow is often expressed as a [DAG][dag_wiki] of processing stages, where each stage can result in N possible next stages, until finally the flow ends at an exit node.

For example, if we had processing stages A, B, C and D, where stage A could result in either stage B or C being next, always followed by D, it might look something like this:

` + "```text" + `
     /--> B --\
A --|          |--> D
     \--> C --/
` + "```" + `

This flow would be easy to express in a standard Benthos config, we could simply use a ` + "[`switch` processor][processors.switch]" + ` to route to either B or C depending on a condition on the result of A. However, this method of flow control quickly becomes unfeasible as the DAG gets more complicated, imagine expressing this flow using switch processors:

` + "```text" + `
      /--> B -------------|--> D
     /                   /
A --|          /--> E --|
     \--> C --|          \
               \----------|--> F
` + "```" + `

And imagine doing so knowing that the diagram is subject to change over time. Yikes! Instead, with a workflow we can either trust it to automatically resolve the DAG or express it manually as simply as ` + "`order: [ [ A ], [ B, C ], [ E ], [ D, F ] ]`" + `, and the conditional logic for determining if a stage is executed is defined as part of the branch itself.`,
		Footnotes: `
## Structured Metadata

When the field ` + "`meta_path`" + ` is non-empty the workflow processor creates an object describing which workflows were successful, skipped or failed for each message and stores the object within the message at the end.

The object is of the following form:

` + "```json" + `
{
	"succeeded": [ "foo" ],
	"skipped": [ "bar" ],
	"failed": {
		"baz": "the error message from the branch"
	}
}
` + "```" + `

If a message already has a meta object at the given path when it is processed then the object is used in order to determine which branches have already been performed on the message (or skipped) and can therefore be skipped on this run.

This is a useful pattern when replaying messages that have failed some branches previously. For example, given the above example object the branches foo and bar would automatically be skipped, and baz would be reattempted.

The previous meta object will also be preserved in the field ` + "`<meta_path>.previous`" + ` when the new meta object is written, preserving a full record of all workflow executions.

If a field ` + "`<meta_path>.apply`" + ` exists in the meta object for a message and is an array then it will be used as an explicit list of stages to apply, all other stages will be skipped.

## Resources

It's common to configure processors (and other components) as resources in order to keep the pipeline configuration cleaner. With the workflow processor you can include branch processors configured as resources within your workflow by specifying them by name in the field ` + "`order`" + `, if Benthos doesn't find a branch within the workflow configuration of that name it'll refer to the resources.

However, it's not possible to use resource branches in a workflow without specifying an explicit ordering.

## Error Handling

The recommended approach to handle failures within a workflow is to query against the [structured metadata](#structured-metadata) it provides, as it provides granular information about exactly which branches failed and which ones succeeded and therefore aren't necessary to perform again.

For example, if our meta object is stored at the path ` + "`meta.workflow`" + ` and we wanted to check whether a message has failed for any branch we can do that using a [Bloblang query][guides.bloblang] like ` + "`this.meta.workflow.failed.length() | 0 > 0`" + `, or to check whether a specific branch failed we can use ` + "`this.exists(\"meta.workflow.failed.foo\")`" + `.

However, if structured metadata is disabled by setting the field ` + "`meta_path`" + ` to empty then the workflow processor instead adds a general error flag to messages when any executed branch fails. In this case it's possible to handle failures using [standard error handling patterns][configuration.error-handling].

[dag_wiki]: https://en.wikipedia.org/wiki/Directed_acyclic_graph
[processors.switch]: /docs/components/processors/switch
[processors.http]: /docs/components/processors/http
[processors.lambda]: /docs/components/processors/lambda
[processors.cache]: /docs/components/processors/cache
[processors.branch]: /docs/components/processors/branch
[guides.bloblang]: /docs/guides/bloblang/about
[configuration.pipelines]: /docs/configuration/processing_pipelines
[configuration.error-handling]: /docs/configuration/error_handling
`,
		Examples: []docs.AnnotatedExample{
			{
				Title: "Automatic Ordering",
				Summary: `
When the field ` + "`order`" + ` is omitted a best attempt is made to determine a dependency tree between branches based on their request and result mappings. In the following example the branches foo and bar will be executed first in parallel, and afterwards the branch baz will be executed.`,
				Config: `
pipeline:
  processors:
    - workflow:
        meta_path: meta.workflow
        branches:
          foo:
            request_map: 'root = ""'
            processors:
              - http:
                  url: TODO
            result_map: 'root.foo = this'

          bar:
            request_map: 'root = this.body'
            processors:
              - lambda:
                  function: TODO
            result_map: 'root.bar = this'

          baz:
            request_map: |
              root.fooid = this.foo.id
              root.barstuff = this.bar.content
            processors:
              - cache:
                  resource: TODO
                  operator: set
                  key: ${! json("fooid") }
                  value: ${! json("barstuff") }
`,
			},
			{
				Title: "Conditional Branches",
				Summary: `
Branches of a workflow are skipped when the ` + "`request_map`" + ` assigns ` + "`deleted()`" + ` to the root. In this example the branch A is executed when the document type is "foo", and branch B otherwise. Branch C is executed afterwards and is skipped unless either A or B successfully provided a result at ` + "`tmp.result`" + `.`,
				Config: `
pipeline:
  processors:
    - workflow:
        branches:
          A:
            request_map: |
              root = if this.document.type != "foo" {
                  deleted()
              }
            processors:
              - http:
                  url: TODO
            result_map: 'root.tmp.result = this'

          B:
            request_map: |
              root = if this.document.type == "foo" {
                  deleted()
              }
            processors:
              - lambda:
                  function: TODO
            result_map: 'root.tmp.result = this'

          C:
            request_map: |
              root = if this.tmp.result != null {
                  deleted()
              }
            processors:
              - http:
                  url: TODO_SOMEWHERE_ELSE
            result_map: 'root.tmp.result = this'
`,
			},
			{
				Title: "Resources",
				Summary: `
The ` + "`order`" + ` field can be used in order to refer to [branch processor resources](#resources), this can sometimes make your pipeline configuration cleaner, as well as allowing you to reuse branch configurations in order places. It's also possible to mix and match branches configured within the workflow and configured as resources.`,
				Config: `
pipeline:
  processors:
    - workflow:
        order: [ [ foo, bar ], [ baz ] ]
        branches:
          bar:
            request_map: 'root = this.body'
            processors:
              - lambda:
                  function: TODO
            result_map: 'root.bar = this'

resources:
  processors:
    foo:
      branch:
        request_map: 'root = ""'
        processors:
          - http:
              url: TODO
        result_map: 'root.foo = this'

    baz:
      branch:
        request_map: |
          root.fooid = this.foo.id
          root.barstuff = this.bar.content
        processors:
          - cache:
              resource: TODO
              operator: set
              key: ${! json("fooid") }
              value: ${! json("barstuff") }
`,
			},
		},
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("meta_path", "A [dot path](/docs/configuration/field_paths) indicating where to store and reference [structured metadata](#structured-metadata) about the workflow execution."),
			docs.FieldDeprecated("stages"),
			docs.FieldCommon(
				"order",
				"An explicit declaration of branch ordered tiers, which describes the order in which parallel tiers of branches should be executed. Branches should be identified by the name as they are configured in the field `branches`. It's also possible to specify branch processors configured [as a resource](#resources). ",
				[][]string{{"foo", "bar"}, {"baz"}},
				[][]string{{"foo"}, {"bar"}, {"baz"}},
			),
			docs.FieldCommon(
				"branches",
				"An object of named [`branch` processors](/docs/components/processors/branch) that make up the workflow. The order and parallelism in which branches are executed can either be made explicit with the field `order`, or if omitted an attempt is made to automatically resolve an ordering based on the mappings of each branch.",
			),
		},
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			sanitBranches := map[string]interface{}{}
			for k, v := range conf.Workflow.Branches {
				sanit, err := v.Sanitise()
				if err != nil {
					return nil, err
				}
				sanitBranches[k] = sanit
			}
			m := map[string]interface{}{
				"meta_path": conf.Workflow.MetaPath,
				"order":     conf.Workflow.Order,
				"branches":  sanitBranches,
			}
			if len(conf.Workflow.Stages) > 0 {
				sanitChildren := map[string]interface{}{}
				for k, v := range conf.Workflow.Stages {
					sanit, err := v.Sanitise()
					if err != nil {
						return nil, err
					}
					sanit["dependencies"] = v.Dependencies
					sanitChildren[k] = sanit
				}
				m["stages"] = sanitChildren
			}

			return m, nil
		},
	}
}

//------------------------------------------------------------------------------

// WorkflowConfig is a config struct containing fields for the Workflow
// processor.
type WorkflowConfig struct {
	MetaPath string                         `json:"meta_path" yaml:"meta_path"`
	Order    [][]string                     `json:"order" yaml:"order"`
	Branches map[string]BranchConfig        `json:"branches" yaml:"branches"`
	Stages   map[string]DepProcessMapConfig `json:"stages" yaml:"stages"`
}

// NewWorkflowConfig returns a default WorkflowConfig.
func NewWorkflowConfig() WorkflowConfig {
	return WorkflowConfig{
		MetaPath: "meta.workflow",
		Order:    [][]string{},
		Branches: map[string]BranchConfig{},
		Stages:   map[string]DepProcessMapConfig{},
	}
}

//------------------------------------------------------------------------------

type workflowBranch interface {
	targetsUsed() [][]string
	targetsProvided() [][]string
	createResult([]types.Part, types.Message) ([]types.Part, []branchMapError, error)
	overlayResult(types.Message, []types.Part) ([]branchMapError, error)
	lock()
	unlock()
	CloseAsync()
	WaitForClose(time.Duration) error
}

type resourcedBranch struct {
	name string
	mgr  procProvider
}

func (r *resourcedBranch) targetsUsed() [][]string {
	return nil
}

func (r *resourcedBranch) targetsProvided() [][]string {
	return nil
}

func (r *resourcedBranch) createResult(parts []types.Part, referenceMsg types.Message) ([]types.Part, []branchMapError, error) {
	p, err := r.mgr.GetProcessor(r.name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to obtain branch resource '%v': %w", r.name, err)
	}
	b, ok := p.(*Branch)
	if !ok {
		return nil, nil, fmt.Errorf("branch resource '%v' found incorrect processor type %T", r.name, p)
	}
	return b.createResult(parts, referenceMsg)
}

func (r *resourcedBranch) overlayResult(msg types.Message, parts []types.Part) ([]branchMapError, error) {
	p, err := r.mgr.GetProcessor(r.name)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain branch resource '%v': %w", r.name, err)
	}
	b, ok := p.(*Branch)
	if !ok {
		return nil, fmt.Errorf("branch resource '%v' found incorrect processor type %T", r.name, p)
	}
	return b.overlayResult(msg, parts)
}

// TODO: Once manager supports locking and updates.
func (r *resourcedBranch) lock()   {}
func (r *resourcedBranch) unlock() {}

// Not needed as the manager handles shut down.
func (r *resourcedBranch) CloseAsync() {}
func (r *resourcedBranch) WaitForClose(time.Duration) error {
	return nil
}

type normalBranch struct {
	*Branch
}

func (r *normalBranch) lock()   {}
func (r *normalBranch) unlock() {}

//------------------------------------------------------------------------------

// Workflow is a processor that applies a list of child processors to a new
// payload mapped from the original, and after processing attempts to overlay
// the results back onto the original payloads according to more mappings.
type Workflow struct {
	log   log.Modular
	stats metrics.Type

	children  map[string]workflowBranch
	dag       [][]string
	allStages map[string]struct{}
	metaPath  []string

	mCount           metrics.StatCounter
	mSent            metrics.StatCounter
	mSentParts       metrics.StatCounter
	mSkippedNoStages metrics.StatCounter
	mErr             metrics.StatCounter
	mErrJSON         metrics.StatCounter
	mErrMeta         metrics.StatCounter
	mErrOverlay      metrics.StatCounter
	mErrStages       map[string]metrics.StatCounter
	mSuccStages      map[string]metrics.StatCounter
}

// NewWorkflow returns a new workflow processor.
func NewWorkflow(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	if len(conf.Workflow.Stages) > 0 {
		if len(conf.Workflow.Branches) > 0 {
			return nil, fmt.Errorf("cannot combine both workflow branches and stages in the same processor")
		}
		if len(conf.Workflow.Order) > 0 {
			return nil, fmt.Errorf("cannot combine both manual ordering and stages in the same processor")
		}
		return newWorkflowDeprecated(conf, mgr, log, stats)
	}

	w := &Workflow{
		log:         log,
		stats:       stats,
		mErrStages:  map[string]metrics.StatCounter{},
		mSuccStages: map[string]metrics.StatCounter{},
		metaPath:    nil,
		allStages:   map[string]struct{}{},
	}
	if len(conf.Workflow.MetaPath) > 0 {
		w.metaPath = gabs.DotPathToSlice(conf.Workflow.MetaPath)
	}

	w.children = map[string]workflowBranch{}
	for k, v := range conf.Workflow.Branches {
		if len(processDAGStageName.FindString(k)) != len(k) {
			return nil, fmt.Errorf("workflow branch name '%v' contains invalid characters", k)
		}

		nsLog := log.NewModule(fmt.Sprintf(".%v", k))
		nsStats := metrics.Namespaced(stats, k)

		child, err := newBranch(v, mgr, nsLog, nsStats)
		if err != nil {
			return nil, fmt.Errorf("failed to create branch '%v': %v", k, err)
		}

		w.children[k] = &normalBranch{child}
		w.allStages[k] = struct{}{}
	}

	if len(conf.Workflow.Order) > 0 {
		remaining := map[string]struct{}{}
		seen := map[string]struct{}{}
		for id := range w.children {
			remaining[id] = struct{}{}
		}
		for i, tier := range conf.Workflow.Order {
			if len(tier) == 0 {
				return nil, fmt.Errorf("explicit order tier '%v' was empty", i)
			}
			for _, t := range tier {
				if _, exists := seen[t]; exists {
					return nil, fmt.Errorf("branch specified in order listed multiple times: %v", t)
				}
				if _, exists := remaining[t]; !exists {
					// TODO: V4 Remove this
					pProvider, ok := mgr.(procProvider)
					if !ok {
						return nil, errors.New("manager does not support processor resources")
					}
					// If we haven't specified the processor
					if p, err := pProvider.GetProcessor(t); err != nil {
						return nil, fmt.Errorf("branch specified in order not found: %v", t)
					} else if _, ok := p.(*Branch); !ok {
						return nil, fmt.Errorf(
							"found resource named '%v' with wrong type, expected a branch processor, found: %T",
							t, p,
						)
					} else {
						w.children[t] = &resourcedBranch{
							name: t,
							mgr:  pProvider,
						}
						w.allStages[t] = struct{}{}
					}
				}
				seen[t] = struct{}{}
				delete(remaining, t)
			}
		}
		if len(remaining) > 0 {
			names := make([]string, 0, len(remaining))
			for k := range remaining {
				names = append(names, k)
			}
			return nil, fmt.Errorf("the following branches were missing from order: %v", names)
		}
		w.dag = conf.Workflow.Order
	} else {
		var err error
		if w.dag, err = resolveBranchDAG(w.children); err != nil {
			return nil, err
		}
		w.log.Infof("Automatically resolved workflow DAG: %v\n", w.dag)
	}

	w.mCount = stats.GetCounter("count")
	w.mSent = stats.GetCounter("sent")
	w.mSentParts = stats.GetCounter("parts.sent")
	w.mSkippedNoStages = stats.GetCounter("skipped.no_stages")
	w.mErr = stats.GetCounter("error")
	w.mErrJSON = stats.GetCounter("error.json_parse")
	w.mErrMeta = stats.GetCounter("error.meta_set")
	w.mErrOverlay = stats.GetCounter("error.overlay")

	return w, nil
}

//------------------------------------------------------------------------------

func depHasPrefix(wanted, provided []string) bool {
	if len(wanted) < len(provided) {
		return false
	}
	for i, s := range provided {
		if wanted[i] != s {
			return false
		}
	}
	return true
}

func getBranchDeps(id string, wanted [][]string, branches map[string]workflowBranch) []string {
	dependencies := []string{}

eLoop:
	for k, b := range branches {
		if k == id {
			continue
		}
		for _, tp := range b.targetsProvided() {
			for _, tn := range wanted {
				if depHasPrefix(tn, tp) {
					dependencies = append(dependencies, k)
					continue eLoop
				}
			}
		}
	}

	return dependencies
}

func resolveBranchDAG(branches map[string]workflowBranch) ([][]string, error) {
	if branches == nil || len(branches) == 0 {
		return [][]string{}, nil
	}
	remaining := map[string]struct{}{}

	var entries []dependencysolver.Entry
	for id, b := range branches {
		wanted := b.targetsUsed()

		remaining[id] = struct{}{}
		entries = append(entries, dependencysolver.Entry{
			ID: id, Deps: getBranchDeps(id, wanted, branches),
		})
	}

	layers := dependencysolver.LayeredTopologicalSort(entries)
	for _, l := range layers {
		for _, id := range l {
			delete(remaining, id)
		}
	}

	if len(remaining) > 0 {
		var tProcs []string
		for k := range remaining {
			tProcs = append(tProcs, k)
		}
		sort.Strings(tProcs)
		return nil, fmt.Errorf("failed to automatically resolve DAG, circular dependencies detected for branches: %v", tProcs)
	}

	return layers, nil
}

//------------------------------------------------------------------------------

func (w *Workflow) incrStageErr(id string) {
	if ctr, exists := w.mErrStages[id]; exists {
		ctr.Incr(1)
		return
	}

	ctr := w.stats.GetCounter(fmt.Sprintf("%v.error", id))
	ctr.Incr(1)
	w.mErrStages[id] = ctr
}

func (w *Workflow) incrStageSucc(id string) {
	if ctr, exists := w.mSuccStages[id]; exists {
		ctr.Incr(1)
		return
	}

	ctr := w.stats.GetCounter(fmt.Sprintf("%v.success", id))
	ctr.Incr(1)
	w.mSuccStages[id] = ctr
}

//------------------------------------------------------------------------------

type resultTracker struct {
	succeeded map[string]struct{}
	skipped   map[string]struct{}
	failed    map[string]string
	sync.Mutex
}

func trackerFromTree(tree [][]string) *resultTracker {
	r := &resultTracker{
		succeeded: map[string]struct{}{},
		skipped:   map[string]struct{}{},
		failed:    map[string]string{},
	}
	for _, layer := range tree {
		for _, k := range layer {
			r.succeeded[k] = struct{}{}
		}
	}
	return r
}

func (r *resultTracker) Skipped(k string) {
	r.Lock()
	if _, exists := r.succeeded[k]; exists {
		delete(r.succeeded, k)
	}
	r.skipped[k] = struct{}{}
	r.Unlock()
}

func (r *resultTracker) Failed(k, why string) {
	r.Lock()
	if _, exists := r.succeeded[k]; exists {
		delete(r.succeeded, k)
	}
	if _, exists := r.skipped[k]; exists {
		delete(r.skipped, k)
	}
	r.failed[k] = why
	r.Unlock()
}

func (r *resultTracker) ToObject() map[string]interface{} {
	succeeded := make([]interface{}, 0, len(r.succeeded))
	skipped := make([]interface{}, 0, len(r.skipped))
	failed := make(map[string]interface{}, len(r.failed))

	for k := range r.succeeded {
		succeeded = append(succeeded, k)
	}
	sort.Slice(succeeded, func(i, j int) bool {
		return succeeded[i].(string) < succeeded[j].(string)
	})
	for k := range r.skipped {
		skipped = append(skipped, k)
	}
	sort.Slice(skipped, func(i, j int) bool {
		return skipped[i].(string) < skipped[j].(string)
	})
	for k, v := range r.failed {
		failed[k] = v
	}

	m := map[string]interface{}{}
	if len(succeeded) > 0 {
		m["succeeded"] = succeeded
	}
	if len(skipped) > 0 {
		m["skipped"] = skipped
	}
	if len(failed) > 0 {
		m["failed"] = failed
	}
	return m
}

// Returns a map of enrichment IDs that should be skipped for this payload.
func (w *Workflow) skipFromMeta(root interface{}) map[string]struct{} {
	skipList := map[string]struct{}{}
	if len(w.metaPath) == 0 {
		return skipList
	}

	gObj := gabs.Wrap(root)

	// If a whitelist is provided for this flow then skip stages that aren't
	// within it.
	if apply, ok := gObj.S(append(w.metaPath, "apply")...).Data().([]interface{}); ok {
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
	if succeeded, ok := gObj.S(append(w.metaPath, "succeeded")...).Data().([]interface{}); ok {
		for _, id := range succeeded {
			if idStr, isString := id.(string); isString {
				if _, exists := w.allStages[idStr]; exists {
					skipList[idStr] = struct{}{}
				}
			}
		}
	}

	// Skip stages that were already skipped in a previous run of this workflow.
	if skipped, ok := gObj.S(append(w.metaPath, "skipped")...).Data().([]interface{}); ok {
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

// ProcessMessage applies workflow stages to each part of a message type.
func (w *Workflow) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	w.mCount.Incr(1)

	// Prevent resourced branches from being updated mid-flow.
	for _, b := range w.children {
		b.lock()
	}
	defer func() {
		for _, b := range w.children {
			b.unlock()
		}
	}()

	skipOnMeta := make([]map[string]struct{}, msg.Len())
	payload := msg.DeepCopy()
	payload.Iter(func(i int, p types.Part) error {
		p.Get()
		p.Metadata()
		if jObj, err := p.JSON(); err == nil {
			skipOnMeta[i] = w.skipFromMeta(jObj)
		} else {
			skipOnMeta[i] = map[string]struct{}{}
		}
		return nil
	})

	propMsg, _ := tracing.WithChildSpans("workflow", payload)

	records := make([]*resultTracker, payload.Len())
	for i := range records {
		records[i] = trackerFromTree(w.dag)
	}

	for _, layer := range w.dag {
		results := make([][]types.Part, len(layer))
		errors := make([]error, len(layer))

		wg := sync.WaitGroup{}
		wg.Add(len(layer))
		for i, eid := range layer {
			go func(id string, index int) {
				requestParts := make([]types.Part, propMsg.Len())
				propMsg.Iter(func(partIndex int, p types.Part) error {
					if _, exists := skipOnMeta[partIndex][id]; !exists {
						part := p.Copy()
						// Remove errors so that they aren't propagated into the
						// branch.
						ClearFail(part)
						requestParts[partIndex] = part
					}
					return nil
				})

				var mapErrs []branchMapError
				var requestSpans []opentracing.Span
				requestParts, requestSpans = tracing.PartsWithChildSpans(id, requestParts)
				results[index], mapErrs, errors[index] = w.children[id].createResult(requestParts, propMsg)
				for _, s := range requestSpans {
					s.Finish()
				}
				for j, p := range results[index] {
					if p == nil {
						records[j].Skipped(id)
					}
				}
				for _, e := range mapErrs {
					records[e.index].Failed(id, e.err.Error())
				}
				wg.Done()
			}(eid, i)
		}
		wg.Wait()

		for i, id := range layer {
			var failed []branchMapError
			err := errors[i]
			if err == nil {
				if failed, err = w.children[id].overlayResult(payload, results[i]); err != nil {
					w.mErrOverlay.Incr(1)
				}
			}
			if err != nil {
				w.incrStageErr(id)
				w.mErr.Incr(1)
				w.log.Errorf("Failed to perform enrichment '%v': %v\n", id, err)
				for j := range records {
					records[j].Failed(id, err.Error())
				}
				continue
			}
			for _, e := range failed {
				records[e.index].Failed(id, e.err.Error())
			}
			w.incrStageSucc(id)
		}
	}

	// Finally, set the meta records of each document.
	if len(w.metaPath) > 0 {
		payload.Iter(func(i int, p types.Part) error {
			pJSON, err := p.JSON()
			if err != nil {
				w.mErr.Incr(1)
				w.mErrMeta.Incr(1)
				w.log.Errorf("Failed to parse message for meta update: %v\n", err)
				FlagErr(p, err)
				return nil
			}

			gObj := gabs.Wrap(pJSON)
			previous := gObj.S(w.metaPath...).Data()
			current := records[i].ToObject()
			if previous != nil {
				current["previous"] = previous
			}
			gObj.Set(current, w.metaPath...)

			p.SetJSON(gObj.Data())
			return nil
		})
	} else {
		payload.Iter(func(i int, p types.Part) error {
			if lf := len(records[i].failed); lf > 0 {
				failed := make([]string, 0, lf)
				for k := range records[i].failed {
					failed = append(failed, k)
				}
				sort.Strings(failed)
				FlagErr(p, fmt.Errorf("workflow branches failed: %v", failed))
			}
			return nil
		})
	}

	tracing.FinishSpans(propMsg)

	w.mSentParts.Incr(int64(payload.Len()))
	w.mSent.Incr(1)
	msgs := [1]types.Message{payload}
	return msgs[:], nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (w *Workflow) CloseAsync() {
	for _, c := range w.children {
		c.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (w *Workflow) WaitForClose(timeout time.Duration) error {
	stopBy := time.Now().Add(timeout)
	for _, c := range w.children {
		if err := c.WaitForClose(time.Until(stopBy)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
