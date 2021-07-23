package processor

import (
	"context"
	"time"

	"github.com/Jeffail/benthos/v3/internal/shutdown"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/tracing"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/opentracing/opentracing-go"
	olog "github.com/opentracing/opentracing-go/log"
)

// V2 is a simpler interface to implement than types.Processor.
type V2 interface {
	// Process a message into one or more resulting messages, or return an error
	// if the message could not be processed. If zero messages are returned and
	// the error is nil then the message is filtered.
	Process(ctx context.Context, p types.Part) ([]types.Part, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

// V2Batched is a simpler interface to implement than types.Processor and allows
// batch-wide processing.
type V2Batched interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if the entire batch could not be processed. If zero messages are
	// returned and the error is nil then all messages are filtered.
	ProcessBatch(ctx context.Context, p []types.Part) ([][]types.Part, error)

	// Close the component, blocks until either the underlying resources are
	// cleaned up or the context is cancelled. Returns an error if the context
	// is cancelled.
	Close(ctx context.Context) error
}

//------------------------------------------------------------------------------

// Implements types.Processor
type v2ToV1Processor struct {
	typeStr string
	p       V2
	sig     *shutdown.Signaller

	mCount   metrics.StatCounter
	mDropped metrics.StatCounter
	mErr     metrics.StatCounter
	mSent    metrics.StatCounter
}

// NewV2ToV1Processor wraps a processor.V2 with a struct that implements
// types.Processor.
func NewV2ToV1Processor(typeStr string, p V2, stats metrics.Type) types.Processor {
	return &v2ToV1Processor{
		typeStr: typeStr, p: p, sig: shutdown.NewSignaller(),

		mCount:   stats.GetCounter("count"),
		mErr:     stats.GetCounter("error"),
		mSent:    stats.GetCounter("sent"),
		mDropped: stats.GetCounter("dropped"),
	}
}

func (a *v2ToV1Processor) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	a.mCount.Incr(1)

	newParts := make([]types.Part, 0, msg.Len())

	msg.Iter(func(i int, part types.Part) error {
		span := tracing.GetSpan(part)
		if span == nil {
			span = opentracing.StartSpan(a.typeStr)
		} else {
			span = opentracing.StartSpan(
				a.typeStr,
				opentracing.ChildOf(span.Context()),
			)
		}

		nextParts, err := a.p.Process(context.Background(), part)
		if err != nil {
			newPart := part.Copy()
			a.mErr.Incr(1)
			processor.FlagErr(newPart, err)
			span.SetTag("error", true)
			span.LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
			nextParts = append(nextParts, newPart)
		}

		span.Finish()
		if len(nextParts) > 0 {
			newParts = append(newParts, nextParts...)
		} else {
			a.mDropped.Incr(1)
		}
		return nil
	})

	if len(newParts) == 0 {
		return nil, response.NewAck()
	}

	newMsg := message.New(nil)
	newMsg.SetAll(newParts)

	a.mSent.Incr(int64(newMsg.Len()))
	return []types.Message{newMsg}, nil
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
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------

// Implements types.Processor
type v2BatchedToV1Processor struct {
	typeStr string
	p       V2Batched
	sig     *shutdown.Signaller

	mCount metrics.StatCounter
	mErr   metrics.StatCounter
	mSent  metrics.StatCounter
}

// NewV2BatchedToV1Processor wraps a processor.V2Batched with a struct that
// implements types.Processor.
func NewV2BatchedToV1Processor(typeStr string, p V2Batched, stats metrics.Type) types.Processor {
	return &v2BatchedToV1Processor{
		typeStr: typeStr, p: p, sig: shutdown.NewSignaller(),

		mCount: stats.GetCounter("count"),
		mErr:   stats.GetCounter("error"),
		mSent:  stats.GetCounter("sent"),
	}
}

func (a *v2BatchedToV1Processor) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	a.mCount.Incr(1)

	newMsg, spans := tracing.WithChildSpans(a.typeStr, msg)
	parts := make([]types.Part, newMsg.Len())
	newMsg.Iter(func(i int, part types.Part) error {
		parts[i] = part
		return nil
	})

	var outputBatches []types.Message

	batches, err := a.p.ProcessBatch(context.Background(), parts)
	if err != nil {
		a.mErr.Incr(1)
		for i, p := range parts {
			parts[i] = p.Copy()
			processor.FlagErr(parts[i], err)
		}
		for _, s := range spans {
			s.SetTag("error", true)
			s.LogFields(
				olog.String("event", "error"),
				olog.String("type", err.Error()),
			)
		}
		newMsg.SetAll(parts)
		outputBatches = append(outputBatches, newMsg)
	} else {
		for _, batch := range batches {
			a.mSent.Incr(int64(len(batch)))
			nextMsg := message.New(nil)
			nextMsg.SetAll(batch)
			outputBatches = append(outputBatches, nextMsg)
		}
	}

	tracing.FinishSpans(newMsg)

	if len(outputBatches) == 0 {
		return nil, response.NewAck()
	}
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
		return types.ErrTimeout
	}
	return nil
}
