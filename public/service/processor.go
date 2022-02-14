package service

import (
	"context"
	"time"

	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

// Processor is a Benthos processor implementation that works against single
// messages.
type Processor interface {
	// Process a message into one or more resulting messages, or return an error
	// if the message could not be processed. If zero messages are returned and
	// the error is nil then the message is filtered.
	//
	// When an error is returned the input message will continue down the
	// pipeline but will be marked with the error with *message.SetError, and
	// metrics and logs will be emitted. The failed message can then be handled
	// with the patterns outlined in https://www.benthos.dev/docs/configuration/error_handling.
	//
	// The Message types returned MUST be derived from the provided message, and
	// CANNOT be custom implementations of Message. In order to copy the
	// provided message use the Copy method.
	Process(context.Context, *Message) (MessageBatch, error)

	Closer
}

//------------------------------------------------------------------------------

// BatchProcessor is a Benthos processor implementation that works against
// batches of messages, which allows windowed processing.
//
// Message batches must be created by upstream components (inputs, buffers, etc)
// otherwise this processor will simply receive batches containing single
// messages.
type BatchProcessor interface {
	// Process a batch of messages into one or more resulting batches, or return
	// an error if the entire batch could not be processed. If zero messages are
	// returned and the error is nil then all messages are filtered.
	//
	// The provided MessageBatch should NOT be modified, in order to return a
	// mutated batch a copy of the slice should be created instead.
	//
	// When an error is returned all of the input messages will continue down
	// the pipeline but will be marked with the error with *message.SetError,
	// and metrics and logs will be emitted.
	//
	// In order to add errors to individual messages of the batch for downstream
	// handling use *message.SetError(err) and return it in the resulting batch
	// with a nil error.
	//
	// The Message types returned MUST be derived from the provided messages,
	// and CANNOT be custom implementations of Message. In order to copy the
	// provided messages use the Copy method.
	ProcessBatch(context.Context, MessageBatch) ([]MessageBatch, error)

	Closer
}

//------------------------------------------------------------------------------

// Implements types.Processor for a Processor.
type airGapProcessor struct {
	p Processor
}

func newAirGapProcessor(typeStr string, p Processor, stats metrics.Type) processor.V1 {
	return processor.NewV2ToV1Processor(typeStr, &airGapProcessor{p}, stats)
}

func (a *airGapProcessor) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	msgs, err := a.p.Process(ctx, newMessageFromPart(msg))
	if err != nil {
		return nil, err
	}
	parts := make([]*message.Part, 0, len(msgs))
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

func newAirGapBatchProcessor(typeStr string, p BatchProcessor, stats metrics.Type) processor.V1 {
	return processor.NewV2BatchedToV1Processor(typeStr, &airGapBatchProcessor{p}, stats)
}

func (a *airGapBatchProcessor) ProcessBatch(ctx context.Context, spans []*tracing.Span, batch *message.Batch) ([]*message.Batch, error) {
	inputBatch := make([]*Message, batch.Len())
	_ = batch.Iter(func(i int, p *message.Part) error {
		inputBatch[i] = newMessageFromPart(p)
		return nil
	})

	outputBatches, err := a.p.ProcessBatch(ctx, inputBatch)
	if err != nil {
		return nil, err
	}

	newBatches := make([]*message.Batch, len(outputBatches))
	for i, batch := range outputBatches {
		newBatch := message.QuickBatch(nil)
		for _, msg := range batch {
			newBatch.Append(msg.part)
		}
		newBatches[i] = newBatch
	}

	return newBatches, nil
}

func (a *airGapBatchProcessor) Close(ctx context.Context) error {
	return a.p.Close(context.Background())
}

//------------------------------------------------------------------------------

// OwnedProcessor provides direct ownership of a processor extracted from a
// plugin config.
type OwnedProcessor struct {
	p processor.V1
}

// Process a single message, returns either a batch of zero or more resulting
// messages or an error if the message could not be processed.
func (o *OwnedProcessor) Process(ctx context.Context, msg *Message) (MessageBatch, error) {
	outMsg := message.QuickBatch(nil)

	// TODO: After V4 we can modify the internal message type to remove this
	// requirement.
	msg.ensureCopied()
	outMsg.Append(msg.part)

	iMsgs, res := o.p.ProcessMessage(outMsg)
	if res != nil {
		return nil, res
	}

	var b MessageBatch
	for _, iMsg := range iMsgs {
		_ = iMsg.Iter(func(i int, part *message.Part) error {
			b = append(b, newMessageFromPart(part))
			return nil
		})
	}
	return b, nil
}

// ProcessBatch attempts to process a batch of messages, returns zero or more
// batches of resulting messages or an error if the messages could not be
// processed.
func (o *OwnedProcessor) ProcessBatch(ctx context.Context, batch MessageBatch) ([]MessageBatch, error) {
	outMsg := message.QuickBatch(nil)

	for _, msg := range batch {
		// TODO: After V4 we can modify the internal message type to remove this
		// requirement.
		msg.ensureCopied()
		outMsg.Append(msg.part)
	}

	iMsgs, res := o.p.ProcessMessage(outMsg)
	if res != nil {
		return nil, res
	}

	var batches []MessageBatch
	for _, iMsg := range iMsgs {
		var b MessageBatch
		_ = iMsg.Iter(func(i int, part *message.Part) error {
			b = append(b, newMessageFromPart(part))
			return nil
		})
		batches = append(batches, b)
	}
	return batches, nil
}

// Close the processor, allowing it to clean up resources. It is
func (o *OwnedProcessor) Close(ctx context.Context) error {
	o.p.CloseAsync()
	for {
		// Gross but will do for now until we replace these with context params.
		if err := o.p.WaitForClose(time.Millisecond * 100); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

// ExecuteProcessors runs a set of batches through a series processors. If an
// error occurs during execution, then this function terminates and returns the
// error. It is important to note that this is unlike a regular processor chain
// when failed message continue to be processed.
func ExecuteProcessors(ctx context.Context, processors []*OwnedProcessor, inbatches ...MessageBatch) ([]MessageBatch, error) {
	if len(processors) == 0 {
		return inbatches, nil
	}

	proc := processors[0]

	nextBatches := make([]MessageBatch, 0, len(inbatches))
	for _, batch := range inbatches {
		batches, err := proc.ProcessBatch(ctx, batch)
		if err != nil {
			return nil, err
		}

		nextBatches = append(nextBatches, batches...)
	}

	return ExecuteProcessors(ctx, processors[1:], nextBatches...)
}
