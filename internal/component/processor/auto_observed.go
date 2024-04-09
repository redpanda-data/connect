package processor

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

// AutoObserved is a simpler processor interface to implement than V1 as it is
// not required to emit observability information within the implementation
// itself.
type AutoObserved interface {
	// Process a message into one or more resulting messages, or return an error
	// if one occurred during processing, in which case the message will
	// continue unchanged except for having that error now affiliated with it.
	//
	// If zero messages are returned and the error is nil then the message is
	// filtered.
	Process(ctx context.Context, p *message.Part) ([]*message.Part, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

// AutoObservedBatched is a simpler processor interface to implement than V1 as
// it is not required to emit observability information within the
// implementation itself.
type AutoObservedBatched interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if one occurred during processing, in which case all messages
	// will continue unchanged except for having that error now affiliated with
	// them.
	//
	// In order to associate individual messages with an error please use
	// ctx.OnError instead of msg.ErrorSet. They are similar, but using
	// ctx.OnError ensures observability data is updated as well as the message
	// being affiliated with the error.
	//
	// If zero message batches are returned and the error is nil then all
	// messages are filtered.
	ProcessBatch(ctx *BatchProcContext, b message.Batch) ([]message.Batch, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

// Implements V1.
type v2ToV1Processor struct {
	typeStr string
	p       AutoObserved
	mgr     component.Observability

	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewAutoObservedProcessor wraps an AutoObserved processor with an
// implementation of V1 which handles observability information.
func NewAutoObservedProcessor(typeStr string, p AutoObserved, mgr component.Observability) V1 {
	return &v2ToV1Processor{
		typeStr: typeStr, p: p, mgr: mgr,

		mReceived:      mgr.Metrics().GetCounter("processor_received"),
		mBatchReceived: mgr.Metrics().GetCounter("processor_batch_received"),
		mSent:          mgr.Metrics().GetCounter("processor_sent"),
		mBatchSent:     mgr.Metrics().GetCounter("processor_batch_sent"),
		mError:         mgr.Metrics().GetCounter("processor_error"),
		mLatency:       mgr.Metrics().GetTimer("processor_latency_ns"),
	}
}

func (a *v2ToV1Processor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	a.mReceived.Incr(int64(msg.Len()))
	a.mBatchReceived.Incr(1)

	tStarted := time.Now()

	newParts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		_, span := tracing.WithChildSpan(a.mgr.Tracer(), a.typeStr, part)

		nextParts, err := a.p.Process(ctx, part)
		if err != nil {
			a.mError.Incr(1)
			a.mgr.Logger().Debug("Processor failed: %v", err)
			MarkErr(part, span, err)
			nextParts = append(nextParts, part)
		}

		span.Finish()
		if len(nextParts) > 0 {
			newParts = append(newParts, nextParts...)
		}
		return nil
	})

	a.mLatency.Timing(time.Since(tStarted).Nanoseconds())
	if len(newParts) == 0 {
		return nil, nil
	}

	a.mSent.Incr(int64(len(newParts)))
	a.mBatchSent.Incr(1)
	return []message.Batch{newParts}, nil
}

func (a *v2ToV1Processor) Close(ctx context.Context) error {
	return a.p.Close(ctx)
}

//------------------------------------------------------------------------------

// TestBatchProcContext creates a context for batch processors. It's safe to
// provide nil spans and parts functions for testing purposes.
func TestBatchProcContext(ctx context.Context, spans []*tracing.Span, parts []*message.Part) *BatchProcContext {
	return &BatchProcContext{
		ctx:   ctx,
		spans: spans,
		parts: parts,
	}
}

// BatchProcContext provides methods for triggering observability updates and
// accessing processor specific spans.
type BatchProcContext struct {
	ctx   context.Context
	spans []*tracing.Span
	parts []*message.Part

	mError metrics.StatCounter
	logger log.Modular
}

// Context returns the underlying processor context.Context.
func (b *BatchProcContext) Context() context.Context {
	return b.ctx
}

// Span returns a span created specifically for the invocation of the processor.
// This can be used in order to add context to what the processor did.
func (b *BatchProcContext) Span(index int) *tracing.Span {
	if len(b.spans) <= index {
		return nil
	}
	return b.spans[index]
}

// OnError should be called when an individual message has encountered an error,
// this should be used instead of .ErrorSet() as it includes observability
// updates.
//
// This method can be called with index -1 in order to set generalised
// observability information without marking specific message errors.
func (b *BatchProcContext) OnError(err error, index int, p *message.Part) {
	if b.mError != nil {
		b.mError.Incr(1)
	}
	if b.logger != nil {
		b.logger.Debug("Processor failed: %v", err)
	}

	var span *tracing.Span
	if len(b.spans) > index && index >= 0 {
		span = b.spans[index]
	}
	if p == nil && len(b.parts) > index && index >= 0 {
		p = b.parts[index]
	}
	MarkErr(p, span, err)
}

// Implements types.Processor.
type v2BatchedToV1Processor struct {
	typeStr string
	p       AutoObservedBatched
	mgr     component.Observability

	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewAutoObservedBatchedProcessor wraps an AutoObservedBatched processor with an
// implementation of V1 which handles observability information.
func NewAutoObservedBatchedProcessor(typeStr string, p AutoObservedBatched, mgr component.Observability) V1 {
	return &v2BatchedToV1Processor{
		typeStr: typeStr, p: p, mgr: mgr,

		mReceived:      mgr.Metrics().GetCounter("processor_received"),
		mBatchReceived: mgr.Metrics().GetCounter("processor_batch_received"),
		mSent:          mgr.Metrics().GetCounter("processor_sent"),
		mBatchSent:     mgr.Metrics().GetCounter("processor_batch_sent"),
		mError:         mgr.Metrics().GetCounter("processor_error"),
		mLatency:       mgr.Metrics().GetTimer("processor_latency_ns"),
	}
}

func (a *v2BatchedToV1Processor) ProcessBatch(ctx context.Context, msg message.Batch) ([]message.Batch, error) {
	a.mReceived.Incr(int64(msg.Len()))
	a.mBatchReceived.Incr(1)

	tStarted := time.Now()
	_, spans := tracing.WithChildSpans(a.mgr.Tracer(), a.typeStr, msg)

	outputBatches, err := a.p.ProcessBatch(&BatchProcContext{
		ctx:    ctx,
		spans:  spans,
		parts:  msg,
		mError: a.mError,
		logger: a.mgr.Logger(),
	}, msg)
	if err != nil {
		a.mError.Incr(int64(msg.Len()))
		a.mgr.Logger().Debug("Processor failed: %v", err)
		_ = msg.Iter(func(i int, p *message.Part) error {
			MarkErr(p, spans[i], err)
			return nil
		})
		outputBatches = append(outputBatches, msg)
	}

	for _, s := range spans {
		s.Finish()
	}

	a.mLatency.Timing(time.Since(tStarted).Nanoseconds())
	if len(outputBatches) == 0 {
		return nil, nil
	}

	for _, m := range outputBatches {
		a.mSent.Incr(int64(m.Len()))
	}
	a.mBatchSent.Incr(int64(len(outputBatches)))
	return outputBatches, nil
}

func (a *v2BatchedToV1Processor) Close(ctx context.Context) error {
	return a.p.Close(ctx)
}
