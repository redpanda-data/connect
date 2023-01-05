package processor

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

// V2 is a simpler interface to implement than V1.
type V2 interface {
	// Process a message into one or more resulting messages, or return an error
	// if the message could not be processed. If zero messages are returned and
	// the error is nil then the message is filtered.
	Process(ctx context.Context, p *message.Part) ([]*message.Part, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

// V2Batched is a simpler interface to implement than V1 and allows batch-wide
// processing.
type V2Batched interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if the entire batch could not be processed. If zero messages are
	// returned and the error is nil then all messages are filtered.
	ProcessBatch(ctx context.Context, spans []*tracing.Span, b message.Batch) ([]message.Batch, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

// Implements V1.
type v2ToV1Processor struct {
	typeStr string
	p       V2
	mgr     component.Observability

	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewV2ToV1Processor wraps a processor.V2 with a struct that implements V1.
func NewV2ToV1Processor(typeStr string, p V2, mgr component.Observability) V1 {
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
			a.mgr.Logger().Debugf("Processor failed: %v", err)
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

// Implements types.Processor.
type v2BatchedToV1Processor struct {
	typeStr string
	p       V2Batched
	mgr     component.Observability

	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewV2BatchedToV1Processor wraps a processor.V2Batched with a struct that
// implements types.Processor.
func NewV2BatchedToV1Processor(typeStr string, p V2Batched, mgr component.Observability) V1 {
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

	outputBatches, err := a.p.ProcessBatch(ctx, spans, msg)
	if err != nil {
		a.mError.Incr(1)
		a.mgr.Logger().Debugf("Processor failed: %v", err)
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
