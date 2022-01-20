package processor

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeWorkflow] = TypeSpec{
		constructor: NewWorkflow,
		Categories: []Category{
			CategoryComposition,
		},
		Status: docs.StatusStable,
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

It's common to configure processors (and other components) [as resources][configuration.resources] in order to keep the pipeline configuration cleaner. With the workflow processor you can include branch processors configured as resources within your workflow either by specifying them by name in the field ` + "`order`" + `, if Benthos doesn't find a branch within the workflow configuration of that name it'll refer to the resources.

Alternatively, if you do not wish to have an explicit ordering, you can add resource names to the field ` + "`branch_resources`" + ` and they will be included in the workflow with automatic DAG resolution along with any branches configured in the ` + "`branches`" + ` field.

### Resource Error Conditions

There are two error conditions that could potentially occur when resources included in your workflow are mutated, and if you are planning to mutate resources in your workflow it is important that you understand them.

The first error case is that a resource in the workflow is removed and not replaced, when this happens the workflow will still be executed but the individual branch will fail. This should only happen if you explicitly delete a branch resource, as any mutation operation will create the new resource before removing the old one.

The second error case is when automatic DAG resolution is being used and a resource in the workflow is changed in a way that breaks the DAG (circular dependencies, etc). When this happens it is impossible to execute the workflow and therefore the processor will fail, which is possible to capture and handle using [standard error handling patterns][configuration.error-handling].

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
[configuration.resources]: /docs/configuration/resources
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
              - aws_lambda:
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
              - aws_lambda:
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
              - aws_lambda:
                  function: TODO
            result_map: 'root.bar = this'

processor_resources:
  - label: foo
    branch:
      request_map: 'root = ""'
      processors:
        - http:
            url: TODO
      result_map: 'root.foo = this'

  - label: baz
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
			docs.FieldString("meta_path", "A [dot path](/docs/configuration/field_paths) indicating where to store and reference [structured metadata](#structured-metadata) about the workflow execution."),
			docs.FieldString(
				"order",
				"An explicit declaration of branch ordered tiers, which describes the order in which parallel tiers of branches should be executed. Branches should be identified by the name as they are configured in the field `branches`. It's also possible to specify branch processors configured [as a resource](#resources).",
				[][]string{{"foo", "bar"}, {"baz"}},
				[][]string{{"foo"}, {"bar"}, {"baz"}},
			).ArrayOfArrays(),
			docs.FieldString(
				"branch_resources",
				"An optional list of [`branch` processor](/docs/components/processors/branch) names that are configured as [resources](#resources). These resources will be included in the workflow with any branches configured inline within the [`branches`](#branches) field. The order and parallelism in which branches are executed is automatically resolved based on the mappings of each branch. When using resources with an explicit order it is not necessary to list resources in this field.",
			).AtVersion("3.38.0").Advanced().Array(),
			docs.FieldCommon(
				"branches",
				"An object of named [`branch` processors](/docs/components/processors/branch) that make up the workflow. The order and parallelism in which branches are executed can either be made explicit with the field `order`, or if omitted an attempt is made to automatically resolve an ordering based on the mappings of each branch.",
			).Map().WithChildren(branchFields...),
		},
	}
}

//------------------------------------------------------------------------------

// WorkflowConfig is a config struct containing fields for the Workflow
// processor.
type WorkflowConfig struct {
	MetaPath        string                  `json:"meta_path" yaml:"meta_path"`
	Order           [][]string              `json:"order" yaml:"order"`
	BranchResources []string                `json:"branch_resources" yaml:"branch_resources"`
	Branches        map[string]BranchConfig `json:"branches" yaml:"branches"`
}

// NewWorkflowConfig returns a default WorkflowConfig.
func NewWorkflowConfig() WorkflowConfig {
	return WorkflowConfig{
		MetaPath:        "meta.workflow",
		Order:           [][]string{},
		BranchResources: []string{},
		Branches:        map[string]BranchConfig{},
	}
}

//------------------------------------------------------------------------------

// Workflow is a processor that applies a list of child processors to a new
// payload mapped from the original, and after processing attempts to overlay
// the results back onto the original payloads according to more mappings.
type Workflow struct {
	log   log.Modular
	stats metrics.Type

	children  *workflowBranchMap
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
	metricsMut       sync.RWMutex
}

// NewWorkflow returns a new workflow processor.
func NewWorkflow(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
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

	var err error
	if w.children, err = newWorkflowBranchMap(conf.Workflow, mgr, log, stats); err != nil {
		return nil, err
	}
	for k := range w.children.dynamicBranches {
		w.allStages[k] = struct{}{}
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

func (w *Workflow) incrStageErr(id string) {
	w.metricsMut.RLock()
	ctr, exists := w.mErrStages[id]
	w.metricsMut.RUnlock()
	if exists {
		ctr.Incr(1)
		return
	}

	w.metricsMut.Lock()
	defer w.metricsMut.Unlock()

	ctr = w.stats.GetCounter(fmt.Sprintf("%v.error", id))
	ctr.Incr(1)
	w.mErrStages[id] = ctr
}

func (w *Workflow) incrStageSucc(id string) {
	w.metricsMut.RLock()
	ctr, exists := w.mSuccStages[id]
	w.metricsMut.RUnlock()
	if exists {
		ctr.Incr(1)
		return
	}

	w.metricsMut.Lock()
	defer w.metricsMut.Unlock()

	ctr = w.stats.GetCounter(fmt.Sprintf("%v.success", id))
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
	delete(r.succeeded, k)

	r.skipped[k] = struct{}{}
	r.Unlock()
}

func (r *resultTracker) Failed(k, why string) {
	r.Lock()
	delete(r.succeeded, k)
	delete(r.skipped, k)

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

	payload := msg.DeepCopy()

	// Prevent resourced branches from being updated mid-flow.
	dag, children, unlock, err := w.children.Lock()
	if err != nil {
		w.mErr.Incr(1)
		w.log.Errorf("Failed to establish workflow: %v\n", err)

		payload.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			return nil
		})
		w.mSentParts.Incr(int64(payload.Len()))
		w.mSent.Incr(1)
		return []types.Message{payload}, nil
	}
	defer unlock()

	skipOnMeta := make([]map[string]struct{}, msg.Len())
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
		records[i] = trackerFromTree(dag)
	}

	for _, layer := range dag {
		results := make([][]types.Part, len(layer))
		errors := make([]error, len(layer))

		wg := sync.WaitGroup{}
		wg.Add(len(layer))
		for i, eid := range layer {
			go func(id string, index int) {
				branchMsg, branchSpans := tracing.WithChildSpans(id, propMsg.Copy())

				branchParts := make([]types.Part, branchMsg.Len())
				branchMsg.Iter(func(partIndex int, part types.Part) error {
					// Remove errors so that they aren't propagated into the
					// branch.
					ClearFail(part)
					if _, exists := skipOnMeta[partIndex][id]; !exists {
						branchParts[partIndex] = part
					}
					return nil
				})

				var mapErrs []branchMapError
				results[index], mapErrs, errors[index] = children[id].createResult(branchParts, propMsg)
				for _, s := range branchSpans {
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
				if failed, err = children[id].overlayResult(payload, results[i]); err != nil {
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
	w.children.CloseAsync()
}

// WaitForClose blocks until the processor has closed down.
func (w *Workflow) WaitForClose(timeout time.Duration) error {
	return w.children.WaitForClose(timeout)
}

//------------------------------------------------------------------------------
