package processor

import (
	"context"

	"github.com/benthosdev/benthos/v4/internal/message"
)

// ExecuteAll attempts to execute a slice of processors to a message. Returns
// N resulting messages or a response. The response may indicate either a NoAck
// in the event of the message being buffered or an unrecoverable error.
func ExecuteAll(ctx context.Context, procs []V1, msgs ...message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, len(msgs))
	copy(resultMsgs, msgs)

	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []message.Batch
		for _, m := range resultMsgs {
			rMsgs, err := procs[i].ProcessBatch(ctx, m)
			if err != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	return resultMsgs, nil
}

// ExecuteTryAll attempts to execute a slice of processors to messages, if a
// message has failed a processing step it is prevented from being sent to
// subsequent processors. Returns N resulting messages or a response. The
// response may indicate either a NoAck in the event of the message being
// buffered or an unrecoverable error.
func ExecuteTryAll(ctx context.Context, procs []V1, msgs ...message.Batch) ([]message.Batch, error) {
	resultMsgs := make([]message.Batch, len(msgs))
	copy(resultMsgs, msgs)

	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []message.Batch
		for _, m := range resultMsgs {
			// Skip messages that failed a prior stage.
			if m.Get(0).ErrorGet() != nil {
				nextResultMsgs = append(nextResultMsgs, m)
				continue
			}
			rMsgs, err := procs[i].ProcessBatch(ctx, m)
			if err != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	return resultMsgs, nil
}

type catchMessage struct {
	batches []message.Batch
	caught  bool
}

// ExecuteCatchAll attempts to execute a slice of processors to only messages
// that have failed a processing step. Returns N resulting messages or a
// response.
func ExecuteCatchAll(ctx context.Context, procs []V1, msgs ...message.Batch) ([]message.Batch, error) {
	// Preserves the original order of messages before entering the catch block.
	// Only processors that have failed a previous stage are "caught", and will
	// remain caught until all catch processors are executed.
	catchBatches := make([]catchMessage, len(msgs))
	for i, m := range msgs {
		catchBatches[i] = catchMessage{
			batches: []message.Batch{m},
			caught:  m.Get(0).ErrorGet() != nil,
		}
	}

	for i := 0; i < len(procs); i++ {
		for j := 0; j < len(catchBatches); j++ {
			if !catchBatches[j].caught || len(catchBatches[j].batches) == 0 {
				continue
			}

			var nextResultBatches []message.Batch
			for _, m := range catchBatches[j].batches {
				rMsgs, resultRes := procs[i].ProcessBatch(ctx, m)
				if resultRes != nil {
					// We immediately return if a processor hits an unrecoverable
					// error on a message.
					return nil, resultRes
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				nextResultBatches = append(nextResultBatches, rMsgs...)
			}
			catchBatches[j].batches = nextResultBatches
		}
	}

	var resultBatches []message.Batch
	for _, b := range catchBatches {
		resultBatches = append(resultBatches, b.batches...)
	}
	return resultBatches, nil
}
