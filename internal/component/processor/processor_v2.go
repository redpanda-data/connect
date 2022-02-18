package processor

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
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
	ProcessBatch(ctx context.Context, spans []*tracing.Span, b *message.Batch) ([]*message.Batch, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

// Implements V1
type v2ToV1Processor struct {
	typeStr string
	p       V2
	sig     *shutdown.Signaller

	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewV2ToV1Processor wraps a processor.V2 with a struct that implements V1.
func NewV2ToV1Processor(typeStr string, p V2, stats metrics.Type) V1 {
	return &v2ToV1Processor{
		typeStr: typeStr, p: p, sig: shutdown.NewSignaller(),

		mReceived:      stats.GetCounter("processor_received"),
		mBatchReceived: stats.GetCounter("processor_batch_received"),
		mSent:          stats.GetCounter("processor_sent"),
		mBatchSent:     stats.GetCounter("processor_batch_sent"),
		mError:         stats.GetCounter("processor_error"),
		mLatency:       stats.GetTimer("processor_latency_ns"),
	}
}

func (a *v2ToV1Processor) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	a.mReceived.Incr(int64(msg.Len()))
	a.mBatchReceived.Incr(1)

	tStarted := time.Now()

	newParts := make([]*message.Part, 0, msg.Len())

	_ = msg.Iter(func(i int, part *message.Part) error {
		span := tracing.CreateChildSpan(a.typeStr, part)

		nextParts, err := a.p.Process(context.Background(), part)
		if err != nil {
			newPart := part.Copy()
			a.mError.Incr(1)
			MarkErr(newPart, span, err)
			nextParts = append(nextParts, newPart)
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

	newMsg := message.QuickBatch(nil)
	newMsg.SetAll(newParts)

	a.mSent.Incr(int64(newMsg.Len()))
	a.mBatchSent.Incr(1)
	return []*message.Batch{newMsg}, nil
}

func (a *v2ToV1Processor) CloseAsync() {
	go func() {
		if err := a.p.Close(context.Background()); err == nil {
			a.sig.ShutdownComplete()
		}
	}()
}

func (a *v2ToV1Processor) WaitForClose(tout time.Duration) error {
	select {
	case <-a.sig.HasClosedChan():
	case <-time.After(tout):
		return component.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

// Implements types.Processor
type v2BatchedToV1Processor struct {
	typeStr string
	p       V2Batched
	sig     *shutdown.Signaller

	mReceived      metrics.StatCounter
	mBatchReceived metrics.StatCounter
	mSent          metrics.StatCounter
	mBatchSent     metrics.StatCounter
	mError         metrics.StatCounter
	mLatency       metrics.StatTimer
}

// NewV2BatchedToV1Processor wraps a processor.V2Batched with a struct that
// implements types.Processor.
func NewV2BatchedToV1Processor(typeStr string, p V2Batched, stats metrics.Type) V1 {
	return &v2BatchedToV1Processor{
		typeStr: typeStr, p: p, sig: shutdown.NewSignaller(),

		mReceived:      stats.GetCounter("processor_received"),
		mBatchReceived: stats.GetCounter("processor_batch_received"),
		mSent:          stats.GetCounter("processor_sent"),
		mBatchSent:     stats.GetCounter("processor_batch_sent"),
		mError:         stats.GetCounter("processor_error"),
		mLatency:       stats.GetTimer("processor_latency_ns"),
	}
}

func (a *v2BatchedToV1Processor) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	a.mReceived.Incr(int64(msg.Len()))
	a.mBatchReceived.Incr(1)

	tStarted := time.Now()
	spans := tracing.CreateChildSpans(a.typeStr, msg)

	outputBatches, err := a.p.ProcessBatch(context.Background(), spans, msg)
	if err != nil {
		a.mError.Incr(1)
		outputBatch := msg.Copy()
		_ = outputBatch.Iter(func(i int, p *message.Part) error {
			MarkErr(p, spans[i], err)
			return nil
		})
		outputBatches = append(outputBatches, outputBatch)
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

func (a *v2BatchedToV1Processor) CloseAsync() {
	go func() {
		if err := a.p.Close(context.Background()); err == nil {
			a.sig.ShutdownComplete()
		}
	}()
}

func (a *v2BatchedToV1Processor) WaitForClose(tout time.Duration) error {
	select {
	case <-a.sig.HasClosedChan():
	case <-time.After(tout):
		return component.ErrTimeout
	}
	return nil
}
