package processor

import (
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	imessage "github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/message"
)

// ExecuteAll attempts to execute a slice of processors to a message. Returns
// N resulting messages or a response. The response may indicate either a NoAck
// in the event of the message being buffered or an unrecoverable error.
func ExecuteAll(procs []processor.V1, msgs ...*message.Batch) ([]*message.Batch, error) {
	resultMsgs := make([]*message.Batch, len(msgs))
	copy(resultMsgs, msgs)

	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []*message.Batch
		for _, m := range resultMsgs {
			rMsgs, resultRes := procs[i].ProcessMessage(m)
			if resultRes != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, resultRes
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
func ExecuteTryAll(procs []processor.V1, msgs ...*message.Batch) ([]*message.Batch, error) {
	resultMsgs := make([]*message.Batch, len(msgs))
	copy(resultMsgs, msgs)

	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []*message.Batch
		for _, m := range resultMsgs {
			// Skip messages that failed a prior stage.
			if HasFailed(m.Get(0)) {
				nextResultMsgs = append(nextResultMsgs, m)
				continue
			}
			rMsgs, resultRes := procs[i].ProcessMessage(m)
			if resultRes != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, resultRes
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	return resultMsgs, nil
}

type catchMessage struct {
	batches []*message.Batch
	caught  bool
}

// ExecuteCatchAll attempts to execute a slice of processors to only messages
// that have failed a processing step. Returns N resulting messages or a
// response.
func ExecuteCatchAll(procs []processor.V1, msgs ...*message.Batch) ([]*message.Batch, error) {
	// Preserves the original order of messages before entering the catch block.
	// Only processors that have failed a previous stage are "caught", and will
	// remain caught until all catch processors are executed.
	catchBatches := make([]catchMessage, len(msgs))
	for i, m := range msgs {
		catchBatches[i] = catchMessage{
			batches: []*message.Batch{m},
			caught:  HasFailed(m.Get(0)),
		}
	}

	for i := 0; i < len(procs); i++ {
		for j := 0; j < len(catchBatches); j++ {
			if !catchBatches[j].caught || len(catchBatches[j].batches) == 0 {
				continue
			}

			var nextResultBatches []*message.Batch
			for _, m := range catchBatches[j].batches {
				rMsgs, resultRes := procs[i].ProcessMessage(m)
				if resultRes != nil {
					// We immediately return if a processor hits an unrecoverable
					// error on a message.
					return nil, resultRes
				}
				nextResultBatches = append(nextResultBatches, rMsgs...)
			}
			catchBatches[j].batches = nextResultBatches
		}
	}

	var resultBatches []*message.Batch
	for _, b := range catchBatches {
		resultBatches = append(resultBatches, b.batches...)
	}
	return resultBatches, nil
}

//------------------------------------------------------------------------------

// FailFlagKey is a metadata key used for flagging processor errors in Benthos.
// If a message part has any non-empty value for this metadata key then it will
// be interpretted as having failed a processor step somewhere in the pipeline.
var FailFlagKey = imessage.FailFlagKey

// FlagFail marks a message part as having failed at a processing step.
func FlagFail(part *message.Part) {
	part.MetaSet(FailFlagKey, "true")
}

// FlagErr marks a message part as having failed at a processing step with an
// error message. If the error is nil the message part remains unchanged.
func FlagErr(part *message.Part, err error) {
	if err != nil {
		part.MetaSet(FailFlagKey, err.Error())
	}
}

// GetFail returns an error string for a message part if it has failed, or an
// empty string if not.
func GetFail(part *message.Part) string {
	return part.MetaGet(FailFlagKey)
}

// HasFailed checks whether a message part has failed a processing step.
func HasFailed(part *message.Part) bool {
	return len(part.MetaGet(FailFlagKey)) > 0
}

// ClearFail removes any existing failure flags from a message part.
func ClearFail(part *message.Part) {
	part.MetaDelete(FailFlagKey)
}

//------------------------------------------------------------------------------

func iterateParts(
	parts []int, msg *message.Batch,
	iter func(int, *message.Part) error,
) error {
	exec := func(i int) error {
		return iter(i, msg.Get(i))
	}
	if len(parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			if err := exec(i); err != nil {
				return err
			}
		}
	} else {
		for _, i := range parts {
			if err := exec(i); err != nil {
				return err
			}
		}
	}
	return nil
}

// IteratePartsWithSpanV2 iterates the parts of a message according to a slice
// of indexes (if empty all parts are iterated) and calls a func for each part
// along with a tracing span for that part. If an error is returned the part is
// flagged as failed and the span has the error logged.
func IteratePartsWithSpanV2(
	operationName string, parts []int, msg *message.Batch,
	iter func(int, *tracing.Span, *message.Part) error,
) {
	exec := func(i int) {
		part := msg.Get(i)
		span := tracing.CreateChildSpan(operationName, part)

		if err := iter(i, span, part); err != nil {
			FlagErr(part, err)
			span.SetTag("error", true)
			span.LogKV(
				"event", "error",
				"type", err.Error(),
			)
		}
		span.Finish()
	}
	if len(parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			exec(i)
		}
	} else {
		for _, i := range parts {
			exec(i)
		}
	}
}
