package service

import (
	"context"

	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// Processor is a Benthos processor implementation that works against single
// messages.
type Processor interface {
	// Process a message into one or more resulting messages, or return an error
	// if the message could not be processed. If zero messages are returned and
	// the error is nil then the message is filtered.
	//
	// The Message types returned MUST be derived from the provided message, and
	// CANNOT be custom implementations of Message. In order to copy the
	// provided message use CopyMessage.
	Process(context.Context, *Message) (MessageBatch, error)

	Closer
}

//------------------------------------------------------------------------------

// BatchProcessor is a Benthos processor implementation that works against
// batches of messages, which allows windowed processing.
type BatchProcessor interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if the entire batch could not be processed. If zero messages are
	// returned and the error is nil then all messages are filtered.
	//
	// The Message types returned MUST be derived from the provided messages,
	// and CANNOT be custom implementations of Message. In order to copy the
	// provided messages use CopyMessage.
	ProcessBatch(context.Context, MessageBatch) ([]MessageBatch, error)

	Closer
}

//------------------------------------------------------------------------------

// Implements types.Processor for a Processor.
type airGapProcessor struct {
	p Processor
}

func newAirGapProcessor(typeStr string, p Processor, stats metrics.Type) types.Processor {
	return processor.NewV2ToV1Processor(typeStr, &airGapProcessor{p}, stats)
}

func (a *airGapProcessor) Process(ctx context.Context, msg types.Part) ([]types.Part, error) {
	msgs, err := a.p.Process(ctx, newMessageFromPart(msg))
	if err != nil {
		return nil, err
	}
	parts := make([]types.Part, 0, len(msgs))
	for _, msg := range msgs {
		parts = append(parts, msg.part)
	}
	return parts, nil
}

func (a *airGapProcessor) Close(ctx context.Context) error {
	return a.p.Close(context.Background())
}

//------------------------------------------------------------------------------

// Implements types.Processor for a BatchProcessor.
type airGapBatchProcessor struct {
	p BatchProcessor
}

func newAirGapBatchProcessor(typeStr string, p BatchProcessor, stats metrics.Type) types.Processor {
	return processor.NewV2BatchedToV1Processor(typeStr, &airGapBatchProcessor{p}, stats)
}

func (a *airGapBatchProcessor) ProcessBatch(ctx context.Context, msgs []types.Part) ([][]types.Part, error) {
	inputBatch := make([]*Message, len(msgs))
	for i, msg := range msgs {
		inputBatch[i] = newMessageFromPart(msg)
	}

	outputBatches, err := a.p.ProcessBatch(ctx, inputBatch)
	if err != nil {
		return nil, err
	}

	newBatches := make([][]types.Part, len(outputBatches))
	for i, batch := range outputBatches {
		newBatch := make([]types.Part, len(batch))
		for j, msg := range batch {
			newBatch[j] = msg.part
		}
		newBatches[i] = newBatch
	}

	return newBatches, nil
}

func (a *airGapBatchProcessor) Close(ctx context.Context) error {
	return a.p.Close(context.Background())
}
