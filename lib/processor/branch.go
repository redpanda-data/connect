package processor

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

var branchFields = docs.FieldSpecs{
	docs.FieldBloblang(
		"request_map",
		"A [Bloblang mapping](/docs/guides/bloblang/about) that describes how to create a request payload suitable for the child processors of this branch. If left empty then the branch will begin with an exact copy of the origin message (including metadata).",
		`root = {
	"id": this.doc.id,
	"content": this.doc.body.text
}`,
		`root = if this.type == "foo" {
	this.foo.request
} else {
	deleted()
}`,
	).HasDefault(""),
	docs.FieldCommon(
		"processors",
		"A list of processors to apply to mapped requests. When processing message batches the resulting batch must match the size and ordering of the input batch, therefore filtering, grouping should not be performed within these processors.",
	).Array().HasType(docs.FieldTypeProcessor).HasDefault([]interface{}{}),
	docs.FieldBloblang(
		"result_map",
		"A [Bloblang mapping](/docs/guides/bloblang/about) that describes how the resulting messages from branched processing should be mapped back into the original payload. If left empty the origin message will remain unchanged (including metadata).",
		`meta foo_code = meta("code")
root.foo_result = this`,
		`meta = meta()
root.bar.body = this.body
root.bar.id = this.user.id`,
		`root.raw_result = content().string()`,
		`root.enrichments.foo = if errored() {
	throw(error())
} else {
	this
}`,
	).HasDefault(""),
}

func init() {
	Constructors[TypeBranch] = TypeSpec{
		Status:      docs.StatusStable,
		constructor: NewBranch,
		Categories: []Category{
			CategoryComposition,
		},
		Summary: `
The ` + "`branch`" + ` processor allows you to create a new request message via
a [Bloblang mapping](/docs/guides/bloblang/about), execute a list of processors
on the request messages, and, finally, map the result back into the source
message using another mapping.`,
		Description: `
This is useful for preserving the original message contents when using
processors that would otherwise replace the entire contents.

### Metadata

Metadata fields that are added to messages during branch processing will not be
automatically copied into the resulting message. In order to do this you should
explicitly declare in your ` + "`result_map`" + ` either a wholesale copy with
` + "`meta = meta()`" + `, or selective copies with
` + "`meta foo = meta(\"bar\")`" + ` and so on.

### Error Handling

If the ` + "`request_map`" + ` fails the child processors will not be executed.
If the child processors themselves result in an (uncaught) error then the
` + "`result_map`" + ` will not be executed. If the ` + "`result_map`" + ` fails
the message will remain unchanged. Under any of these conditions standard
[error handling methods](/docs/configuration/error_handling) can be used in
order to filter, DLQ or recover the failed messages.

### Conditional Branching

If the root of your request map is set to ` + "`deleted()`" + ` then the branch
processors are skipped for the given message, this allows you to conditionally
branch messages.`,
		Examples: []docs.AnnotatedExample{
			{
				Title: "HTTP Request",
				Summary: `
This example strips the request message into an empty body, grabs an HTTP
payload, and places the result back into the original message at the path
` + "`image.pull_count`" + `:`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: 'root = ""'
        processors:
          - http:
              url: https://hub.docker.com/v2/repositories/jeffail/benthos
              verb: GET
        result_map: root.image.pull_count = this.pull_count

# Example input:  {"id":"foo","some":"pre-existing data"}
# Example output: {"id":"foo","some":"pre-existing data","image":{"pull_count":1234}}
`,
			},
			{
				Title: "Non Structured Results",
				Summary: `
When the result of your branch processors is unstructured and you wish to simply set a resulting field to the raw output use the content function to obtain the raw bytes of the resulting message and then coerce it into your value type of choice:`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: 'root = this.document.id'
        processors:
          - cache:
              resource: descriptions_cache
              key: ${! content() }
              operator: get
        result_map: root.document.description = content().string()

# Example input:  {"document":{"id":"foo","content":"hello world"}}
# Example output: {"document":{"id":"foo","content":"hello world","description":"this is a cool doc"}}
`,
			},
			{
				Title: "Lambda Function",
				Summary: `
This example maps a new payload for triggering a lambda function with an ID and
username from the original message, and the result of the lambda is discarded,
meaning the original message is unchanged.`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: '{"id":this.doc.id,"username":this.user.name}'
        processors:
          - aws_lambda:
              function: trigger_user_update

# Example input: {"doc":{"id":"foo","body":"hello world"},"user":{"name":"fooey"}}
# Output matches the input, which is unchanged
`,
			},
			{
				Title: "Conditional Caching",
				Summary: `
This example caches a document by a message ID only when the type of the
document is a foo:`,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: |
          meta id = this.id
          root = if this.type == "foo" {
            this.document
          } else {
            deleted()
          }
        processors:
          - cache:
              resource: TODO
              operator: set
              key: ${! meta("id") }
              value: ${! content() }
`,
			},
		},
		FieldSpecs: branchFields,
	}
}

//------------------------------------------------------------------------------

// BranchConfig contains configuration fields for the Branch processor.
type BranchConfig struct {
	RequestMap string   `json:"request_map" yaml:"request_map"`
	Processors []Config `json:"processors" yaml:"processors"`
	ResultMap  string   `json:"result_map" yaml:"result_map"`
}

// NewBranchConfig returns a BranchConfig with default values.
func NewBranchConfig() BranchConfig {
	return BranchConfig{
		RequestMap: "",
		Processors: []Config{},
		ResultMap:  "",
	}
}

//------------------------------------------------------------------------------

// Branch contains conditions and maps for transforming a batch of messages into
// a subset of request messages, and mapping results from those requests back
// into the original message batch.
type Branch struct {
	log   log.Modular
	stats metrics.Type

	requestMap *mapping.Executor
	resultMap  *mapping.Executor
	children   []types.Processor

	// Metrics
	mCount     metrics.StatCounter
	mErr       metrics.StatCounter
	mErrParts  metrics.StatCounter
	mErrProc   metrics.StatCounter
	mErrAlign  metrics.StatCounter
	mErrReq    metrics.StatCounter
	mErrRes    metrics.StatCounter
	mSent      metrics.StatCounter
	mBatchSent metrics.StatCounter
}

// NewBranch creates a new branch processor.
func NewBranch(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	return newBranch(conf.Branch, mgr, log, stats)
}

func newBranch(
	conf BranchConfig, mgr types.Manager, log log.Modular, stats metrics.Type,
) (*Branch, error) {
	children := make([]types.Processor, 0, len(conf.Processors))
	for i, pconf := range conf.Processors {
		pMgr, pLog, pStats := interop.LabelChild(fmt.Sprintf("processor.%v", i), mgr, log, stats)
		proc, err := New(pconf, pMgr, pLog, pStats)
		if err != nil {
			return nil, fmt.Errorf("failed to init processor %v: %w", i, err)
		}
		children = append(children, proc)
	}
	if len(children) == 0 {
		return nil, errors.New("the branch processor requires at least one child processor")
	}

	b := &Branch{
		children: children,
		log:      log,
		stats:    stats,

		mCount:     stats.GetCounter("count"),
		mErr:       stats.GetCounter("error"),
		mErrParts:  stats.GetCounter("error_counts_diverged"),
		mErrProc:   stats.GetCounter("error_processors"),
		mErrAlign:  stats.GetCounter("error_result_alignment"),
		mErrReq:    stats.GetCounter("error_request_map"),
		mErrRes:    stats.GetCounter("error_result_map"),
		mSent:      stats.GetCounter("sent"),
		mBatchSent: stats.GetCounter("batch.sent"),
	}

	var err error
	if len(conf.RequestMap) > 0 {
		if b.requestMap, err = interop.NewBloblangMapping(mgr, conf.RequestMap); err != nil {
			return nil, fmt.Errorf("failed to parse request mapping: %w", err)
		}
	}
	if len(conf.ResultMap) > 0 {
		if b.resultMap, err = interop.NewBloblangMapping(mgr, conf.ResultMap); err != nil {
			return nil, fmt.Errorf("failed to parse result mapping: %w", err)
		}
	}

	return b, nil
}

//------------------------------------------------------------------------------

// TargetsUsed returns a list of paths that this branch depends on. Each path is
// prefixed by a namespace `metadata` or `path` indicating the source.
func (b *Branch) targetsUsed() [][]string {
	if b.requestMap == nil {
		return nil
	}

	var paths [][]string
	_, queryTargets := b.requestMap.QueryTargets(query.TargetsContext{})

pathLoop:
	for _, p := range queryTargets {
		path := make([]string, 0, len(p.Path)+1)
		switch p.Type {
		case query.TargetValue:
			path = append(path, "path")
		case query.TargetMetadata:
			path = append(path, "metadata")
		default:
			continue pathLoop
		}
		paths = append(paths, append(path, p.Path...))
	}

	return paths
}

// TargetsProvided returns a list of paths that this branch provides.
func (b *Branch) targetsProvided() [][]string {
	if b.resultMap == nil {
		return nil
	}

	var paths [][]string

pathLoop:
	for _, p := range b.resultMap.AssignmentTargets() {
		path := make([]string, 0, len(p.Path)+1)
		switch p.Type {
		case mapping.TargetValue:
			path = append(path, "path")
		case mapping.TargetMetadata:
			path = append(path, "metadata")
		default:
			continue pathLoop
		}
		paths = append(paths, append(path, p.Path...))
	}

	return paths
}

//------------------------------------------------------------------------------

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (b *Branch) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	branchMsg, propSpans := tracing.WithChildSpans(TypeBranch, msg.Copy())
	defer func() {
		for _, s := range propSpans {
			s.Finish()
		}
	}()

	parts := make([]types.Part, 0, branchMsg.Len())
	branchMsg.Iter(func(i int, p types.Part) error {
		// Remove errors so that they aren't propagated into the branch.
		ClearFail(p)
		parts = append(parts, p)
		return nil
	})

	resultParts, mapErrs, err := b.createResult(parts, msg)
	if err != nil {
		result := msg.Copy()
		// Add general error to all messages.
		result.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			return nil
		})
		// And override with mapping specific errors where appropriate.
		for _, e := range mapErrs {
			FlagErr(result.Get(e.index), e.err)
		}
		msgs := [1]types.Message{result}
		return msgs[:], nil
	}

	result := msg.DeepCopy()
	for _, e := range mapErrs {
		FlagErr(result.Get(e.index), e.err)
		b.log.Errorf("Branch error: %v", e.err)
	}

	if mapErrs, err = b.overlayResult(result, resultParts); err != nil {
		result.Iter(func(i int, p types.Part) error {
			FlagErr(p, err)
			return nil
		})
		msgs := [1]types.Message{result}
		return msgs[:], nil
	}
	for _, e := range mapErrs {
		FlagErr(result.Get(e.index), e.err)
		b.log.Errorf("Branch error: %v", e.err)
	}

	return []types.Message{result}, nil
}

//------------------------------------------------------------------------------

type branchMapError struct {
	index int
	err   error
}

func newBranchMapError(index int, err error) branchMapError {
	return branchMapError{index, err}
}

//------------------------------------------------------------------------------

// createResult performs reduction and child processors to a payload. The size
// of the payload will remain unchanged, where reduced indexes are nil. This
// result can be overlayed onto the original message in order to complete the
// map.
func (b *Branch) createResult(parts []types.Part, referenceMsg types.Message) ([]types.Part, []branchMapError, error) {
	b.mCount.Incr(1)

	originalLen := len(parts)

	// Create request payloads
	var skipped, failed []int
	var mapErrs []branchMapError

	newParts := make([]types.Part, 0, len(parts))
	for i := 0; i < len(parts); i++ {
		if parts[i] == nil {
			// Skip if the message part is nil.
			skipped = append(skipped, i)
			continue
		}
		if b.requestMap != nil {
			_ = parts[i].Set(nil)
			newPart, err := b.requestMap.MapOnto(parts[i], i, referenceMsg)
			if err != nil {
				b.mErrReq.Incr(1)
				b.log.Debugf("Failed to map request '%v': %v\n", i, err)

				// Skip if message part fails mapping.
				failed = append(failed, i)
				mapErrs = append(mapErrs, newBranchMapError(i, fmt.Errorf("request mapping failed: %w", err)))
			} else if newPart == nil {
				// Skip if the message part is deleted.
				skipped = append(skipped, i)
			} else {
				newParts = append(newParts, newPart)
			}
		} else {
			newParts = append(newParts, parts[i])
		}
	}
	parts = newParts

	// Execute child processors
	var procResults []types.Message
	var err error
	if len(parts) > 0 {
		var res types.Response
		msg := message.New(nil)
		msg.SetAll(parts)
		if procResults, res = ExecuteAll(b.children, msg); res != nil && res.Error() != nil {
			err = fmt.Errorf("child processors failed: %v", res.Error())
		}
		if len(procResults) == 0 {
			err = errors.New("child processors resulted in zero messages")
		}
		if err != nil {
			b.mErrProc.Incr(1)
			b.mErr.Incr(1)
			b.log.Errorf("Child processors failed: %v\n", err)
			return nil, mapErrs, err
		}
	}

	// Re-align processor results with original message indexes
	var alignedResult []types.Part
	if alignedResult, err = alignBranchResult(originalLen, skipped, failed, procResults); err != nil {
		b.mErrAlign.Incr(1)
		b.mErr.Incr(1)
		b.log.Errorf("Failed to align branch result: %v. Avoid using filters or archive/unarchive processors within your branch, or anything that increases or reduces the number of messages. These processors should instead be applied before or after the branch processor.\n", err)
		return nil, mapErrs, err
	}

	for i, p := range alignedResult {
		if p == nil {
			continue
		}
		if fail := GetFail(p); len(fail) > 0 {
			alignedResult[i] = nil
			mapErrs = append(mapErrs, newBranchMapError(i, fmt.Errorf("processors failed: %v", fail)))
		}
	}

	return alignedResult, mapErrs, nil
}

// overlayResult attempts to merge the result of a process_map with the original
// payload as per the map specified in the postmap and postmap_optional fields.
func (b *Branch) overlayResult(payload types.Message, results []types.Part) ([]branchMapError, error) {
	if exp, act := payload.Len(), len(results); exp != act {
		b.mErr.Incr(1)
		return nil, fmt.Errorf(
			"message count returned from branch has diverged from the request, started with %v messages, finished with %v",
			act, exp,
		)
	}

	resultMsg := message.New(nil)
	resultMsg.SetAll(results)

	var failed []branchMapError

	if b.resultMap != nil {
		parts := make([]types.Part, payload.Len())
		payload.Iter(func(i int, p types.Part) error {
			parts[i] = p
			return nil
		})

		for i, result := range results {
			if result == nil {
				continue
			}

			newPart, err := b.resultMap.MapOnto(payload.Get(i), i, resultMsg)
			if err != nil {
				b.mErrRes.Incr(1)
				b.log.Debugf("Failed to map result '%v': %v\n", i, err)

				failed = append(failed, newBranchMapError(i, fmt.Errorf("result mapping failed: %w", err)))
				continue
			}

			// TODO: Allow filtering here?
			if newPart != nil {
				parts[i] = newPart
			}
		}

		payload.SetAll(parts)
	}

	b.mBatchSent.Incr(1)
	b.mSent.Incr(int64(payload.Len()))
	return failed, nil
}

func alignBranchResult(length int, skipped, failed []int, result []types.Message) ([]types.Part, error) {
	resMsgParts := []types.Part{}
	for _, m := range result {
		m.Iter(func(i int, p types.Part) error {
			resMsgParts = append(resMsgParts, p)
			return nil
		})
	}

	skippedOrFailed := make([]int, len(skipped)+len(failed))
	i := copy(skippedOrFailed, skipped)
	copy(skippedOrFailed[i:], failed)

	sort.Ints(skippedOrFailed)

	// Check that size of response is aligned with payload.
	if rLen, pLen := len(resMsgParts)+len(skippedOrFailed), length; rLen != pLen {
		return nil, fmt.Errorf(
			"message count from branch processors does not match request, started with %v messages, finished with %v",
			rLen, pLen,
		)
	}

	var resultParts []types.Part
	if len(skippedOrFailed) == 0 {
		resultParts = resMsgParts
	} else {
		// Remember to insert nil for each skipped part at the correct index.
		resultParts = make([]types.Part, length)
		sIndex := 0
		for i = 0; i < len(resMsgParts); i++ {
			for sIndex < len(skippedOrFailed) && skippedOrFailed[sIndex] == (i+sIndex) {
				sIndex++
			}
			resultParts[i+sIndex] = resMsgParts[i]
		}
	}

	return resultParts, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (b *Branch) CloseAsync() {
	for _, child := range b.children {
		child.CloseAsync()
	}
}

// WaitForClose blocks until the processor has closed down.
func (b *Branch) WaitForClose(timeout time.Duration) error {
	until := time.Now().Add(timeout)
	for _, child := range b.children {
		if err := child.WaitForClose(time.Until(until)); err != nil {
			return err
		}
	}
	return nil
}

//------------------------------------------------------------------------------
