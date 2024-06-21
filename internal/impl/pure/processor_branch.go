package pure

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	branchProcFieldReqMap = "request_map"
	branchProcFieldProcs  = "processors"
	branchProcFieldResMap = "result_map"
)

func branchProcSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Composition").
		Stable().
		Summary(`The `+"`branch`"+` processor allows you to create a new request message via a [Bloblang mapping](/docs/guides/bloblang/about), execute a list of processors on the request messages, and, finally, map the result back into the source message using another mapping.`).
		Description(`
This is useful for preserving the original message contents when using processors that would otherwise replace the entire contents.

### Metadata

Metadata fields that are added to messages during branch processing will not be automatically copied into the resulting message. In order to do this you should explicitly declare in your `+"`result_map`"+` either a wholesale copy with `+"`meta = metadata()`"+`, or selective copies with `+"`meta foo = metadata(\"bar\")`"+` and so on. It is also possible to reference the metadata of the origin message in the `+"`result_map`"+` using the [`+"`@`"+` operator](/docs/guides/bloblang/about#metadata).

### Error Handling

If the `+"`request_map`"+` fails the child processors will not be executed. If the child processors themselves result in an (uncaught) error then the `+"`result_map`"+` will not be executed. If the `+"`result_map`"+` fails the message will remain unchanged. Under any of these conditions standard [error handling methods](/docs/configuration/error_handling) can be used in order to filter, DLQ or recover the failed messages.

### Conditional Branching

If the root of your request map is set to `+"`deleted()`"+` then the branch processors are skipped for the given message, this allows you to conditionally branch messages.`).
		Example("HTTP Request", `
This example strips the request message into an empty body, grabs an HTTP payload, and places the result back into the original message at the path `+"`image.pull_count`"+`:`, `
pipeline:
  processors:
    - branch:
        request_map: 'root = ""'
        processors:
          - http:
              url: https://hub.docker.com/v2/repositories/jeffail/benthos
              verb: GET
              headers:
                Content-Type: application/json
        result_map: root.image.pull_count = this.pull_count

# Example input:  {"id":"foo","some":"pre-existing data"}
# Example output: {"id":"foo","some":"pre-existing data","image":{"pull_count":1234}}
`).
		Example("Non Structured Results", `
When the result of your branch processors is unstructured and you wish to simply set a resulting field to the raw output use the content function to obtain the raw bytes of the resulting message and then coerce it into your value type of choice:`, `
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
`).
		Example("Lambda Function", `
This example maps a new payload for triggering a lambda function with an ID and username from the original message, and the result of the lambda is discarded, meaning the original message is unchanged.`, `
pipeline:
  processors:
    - branch:
        request_map: '{"id":this.doc.id,"username":this.user.name}'
        processors:
          - aws_lambda:
              function: trigger_user_update

# Example input: {"doc":{"id":"foo","body":"hello world"},"user":{"name":"fooey"}}
# Output matches the input, which is unchanged
`).
		Example("Conditional Caching", `
This example caches a document by a message ID only when the type of the document is a foo:`, `
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
              key: ${! @id }
              value: ${! content() }
`).
		Fields(branchSpecFields()...)
}

func branchSpecFields() []*service.ConfigField {
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
		service.NewProcessorListField(branchProcFieldProcs).
			Description("A list of processors to apply to mapped requests. When processing message batches the resulting batch must match the size and ordering of the input batch, therefore filtering, grouping should not be performed within these processors."),
		service.NewBloblangField(branchProcFieldResMap).
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) that describes how the resulting messages from branched processing should be mapped back into the original payload. If left empty the origin message will remain unchanged (including metadata).").
			Examples(`meta foo_code = metadata("code")
root.foo_result = this`,
				`meta = metadata()
root.bar.body = this.body
root.bar.id = this.user.id`,
				`root.raw_result = content().string()`,
				`root.enrichments.foo = if metadata("request_failed") != null {
  throw(metadata("request_failed"))
} else {
  this
}`, `# Retain only the updated metadata fields which were present in the origin message
meta = metadata().filter(v -> @.get(v.key) != null)`).
			Default(""),
	}
}

func init() {
	err := service.RegisterBatchProcessor(
		"branch", branchProcSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			b, err := newBranchFromParsed(conf, interop.UnwrapManagement(mgr))
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalBatchProcessor(b), nil
		})
	if err != nil {
		panic(err)
	}
}

// Branch contains conditions and maps for transforming a batch of messages into
// a subset of request messages, and mapping results from those requests back
// into the original message batch.
type Branch struct {
	log    log.Modular
	tracer trace.TracerProvider

	requestMap *mapping.Executor
	resultMap  *mapping.Executor
	children   []processor.V1

	// Metrics
	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

func newBranchFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (b *Branch, err error) {
	stats := mgr.Metrics()
	b = &Branch{
		log:    mgr.Logger(),
		tracer: mgr.Tracer(),

		mReceived:      stats.GetCounter("processor_received"),
		mBatchReceived: stats.GetCounter("processor_batch_received"),
		mSent:          stats.GetCounter("processor_sent"),
		mBatchSent:     stats.GetCounter("processor_batch_sent"),
		mError:         stats.GetCounter("processor_error"),
		mLatency:       stats.GetTimer("processor_latency_ns"),
	}

	var pChildren []*service.OwnedProcessor
	if pChildren, err = conf.FieldProcessorList(branchProcFieldProcs); err != nil {
		return
	}
	if len(pChildren) == 0 {
		return nil, errors.New("the branch processor requires at least one child processor")
	}
	b.children = make([]processor.V1, len(pChildren))
	for i, c := range pChildren {
		b.children[i] = interop.UnwrapOwnedProcessor(c)
	}

	if reqMapStr, _ := conf.FieldString(branchProcFieldReqMap); reqMapStr != "" {
		if b.requestMap, err = mgr.BloblEnvironment().NewMapping(reqMapStr); err != nil {
			return nil, fmt.Errorf("failed to parse request mapping: %w", err)
		}
	}
	if resMapStr, _ := conf.FieldString(branchProcFieldResMap); resMapStr != "" {
		if b.resultMap, err = mgr.BloblEnvironment().NewMapping(resMapStr); err != nil {
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

// ProcessBatch applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (b *Branch) ProcessBatch(ctx context.Context, batch message.Batch) ([]message.Batch, error) {
	b.mReceived.Incr(int64(batch.Len()))
	b.mBatchReceived.Incr(1)
	startedAt := time.Now()

	branchMsg, propSpans := tracing.WithChildSpans(b.tracer, "branch", batch.ShallowCopy())
	defer func() {
		for _, s := range propSpans {
			s.Finish()
		}
	}()

	parts := make([]*message.Part, 0, branchMsg.Len())
	_ = branchMsg.Iter(func(i int, p *message.Part) error {
		// Remove errors so that they aren't propagated into the branch.
		p.ErrorSet(nil)
		parts = append(parts, p)
		return nil
	})

	resultParts, mapErrs, err := b.createResult(ctx, parts, batch)
	if err != nil {
		// Add general error to all messages.
		_ = batch.Iter(func(i int, p *message.Part) error {
			p.ErrorSet(err)
			return nil
		})
		// And override with mapping specific errors where appropriate.
		for _, e := range mapErrs {
			batch.Get(e.index).ErrorSet(e.err)
		}
		msgs := [1]message.Batch{batch}
		return msgs[:], nil
	}

	for _, e := range mapErrs {
		batch.Get(e.index).ErrorSet(e.err)
		b.log.Error("Branch error: %v", e.err)
	}

	if mapErrs, err = b.overlayResult(batch, resultParts); err != nil {
		_ = batch.Iter(func(i int, p *message.Part) error {
			p.ErrorSet(err)
			return nil
		})
		return []message.Batch{batch}, nil
	}
	for _, e := range mapErrs {
		batch.Get(e.index).ErrorSet(e.err)
		b.log.Error("Branch error: %v", e.err)
	}

	b.mLatency.Timing(time.Since(startedAt).Nanoseconds())
	return []message.Batch{batch}, nil
}

//------------------------------------------------------------------------------

type branchMapError struct {
	index int
	err   error
}

func newBranchMapError(index int, err error) branchMapError {
	return branchMapError{index: index, err: err}
}

//------------------------------------------------------------------------------

// createResult performs reduction and child processors to a payload. The size
// of the payload will remain unchanged, where reduced indexes are nil. This
// result can be overlayed onto the original message in order to complete the
// map.
func (b *Branch) createResult(ctx context.Context, parts []*message.Part, referenceMsg message.Batch) ([]*message.Part, []branchMapError, error) {
	originalLen := len(parts)

	// Create request payloads
	var skipped, failed []int
	var mapErrs []branchMapError

	newParts := make([]*message.Part, 0, len(parts))
	for i := 0; i < len(parts); i++ {
		if parts[i] == nil {
			// Skip if the message part is nil.
			skipped = append(skipped, i)
			continue
		}
		if b.requestMap != nil {
			_ = parts[i].SetBytes(nil)
			newPart, err := b.requestMap.MapOnto(parts[i], i, referenceMsg)
			if err != nil {
				b.mError.Incr(1)
				b.log.Debug("Failed to map request '%v': %v\n", i, err)

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
	var procResults []message.Batch
	var err error
	if len(parts) > 0 {
		var res error
		if procResults, res = processor.ExecuteAll(ctx, b.children, parts); res != nil {
			err = fmt.Errorf("child processors failed: %v", res)
		}
		if len(procResults) == 0 {
			err = errors.New("child processors resulted in zero messages")
		}
		if err != nil {
			b.mError.Incr(1)
			b.log.Error("Child processors failed: %v\n", err)
			return nil, mapErrs, err
		}
	}

	// Re-align processor results with original message indexes
	var alignedResult []*message.Part
	if alignedResult, err = alignBranchResult(originalLen, skipped, failed, procResults); err != nil {
		b.mError.Incr(1)
		b.log.Error("Failed to align branch result: %v. Avoid using filters or archive/unarchive processors within your branch, or anything that increases or reduces the number of messages. These processors should instead be applied before or after the branch processor.\n", err)
		return nil, mapErrs, err
	}

	for i, p := range alignedResult {
		if p == nil {
			continue
		}
		if fail := p.ErrorGet(); fail != nil {
			alignedResult[i] = nil
			mapErrs = append(mapErrs, newBranchMapError(i, fmt.Errorf("processors failed: %w", fail)))
		}
	}

	return alignedResult, mapErrs, nil
}

// overlayResult attempts to merge the result of a process_map with the original
// payload as per the map specified in the postmap and postmap_optional fields.
func (b *Branch) overlayResult(payload message.Batch, results []*message.Part) ([]branchMapError, error) {
	if exp, act := payload.Len(), len(results); exp != act {
		b.mError.Incr(1)
		return nil, fmt.Errorf(
			"message count returned from branch has diverged from the request, started with %v messages, finished with %v",
			act, exp,
		)
	}

	var failed []branchMapError

	if b.resultMap != nil {
		for i, result := range results {
			if result == nil {
				continue
			}

			newPart, err := b.resultMap.MapOnto(payload.Get(i), i, message.Batch(results))
			if err != nil {
				b.mError.Incr(1)
				b.log.Debug("Failed to map result '%v': %v\n", i, err)

				failed = append(failed, newBranchMapError(i, fmt.Errorf("result mapping failed: %w", err)))
				continue
			}

			// TODO: Allow filtering here?
			if newPart != nil {
				payload[i] = newPart
			}
		}
	}

	b.mBatchSent.Incr(1)
	b.mSent.Incr(int64(payload.Len()))
	return failed, nil
}

func alignBranchResult(length int, skipped, failed []int, result []message.Batch) ([]*message.Part, error) {
	resMsgParts := []*message.Part{}
	for _, m := range result {
		_ = m.Iter(func(i int, p *message.Part) error {
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

	var resultParts []*message.Part
	if len(skippedOrFailed) == 0 {
		resultParts = resMsgParts
	} else {
		// Remember to insert nil for each skipped part at the correct index.
		resultParts = make([]*message.Part, length)
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

// Close blocks until the processor has closed down or the context is cancelled.
func (b *Branch) Close(ctx context.Context) error {
	for _, child := range b.children {
		if err := child.Close(ctx); err != nil {
			return err
		}
	}
	return nil
}
