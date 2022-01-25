package processor

import (
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// ExecuteAll attempts to execute a slice of processors to a message. Returns
// N resulting messages or a response. The response may indicate either a NoAck
// in the event of the message being buffered or an unrecoverable error.
func ExecuteAll(procs []types.Processor, msgs ...types.Message) ([]types.Message, types.Response) {
	resultMsgs := make([]types.Message, len(msgs))
	copy(resultMsgs, msgs)

	var resultRes types.Response
	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []types.Message
		for _, m := range resultMsgs {
			var rMsgs []types.Message
			if rMsgs, resultRes = procs[i].ProcessMessage(m); resultRes != nil && resultRes.Error() != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, resultRes
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	if len(resultMsgs) == 0 {
		if resultRes == nil {
			resultRes = response.NewAck()
		}
		return nil, resultRes
	}
	return resultMsgs, nil
}

// ExecuteTryAll attempts to execute a slice of processors to messages, if a
// message has failed a processing step it is prevented from being sent to
// subsequent processors. Returns N resulting messages or a response. The
// response may indicate either a NoAck in the event of the message being
// buffered or an unrecoverable error.
func ExecuteTryAll(procs []types.Processor, msgs ...types.Message) ([]types.Message, types.Response) {
	resultMsgs := make([]types.Message, len(msgs))
	copy(resultMsgs, msgs)

	var resultRes types.Response
	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []types.Message
		for _, m := range resultMsgs {
			// Skip messages that failed a prior stage.
			if HasFailed(m.Get(0)) {
				nextResultMsgs = append(nextResultMsgs, m)
				continue
			}
			var rMsgs []types.Message
			if rMsgs, resultRes = procs[i].ProcessMessage(m); resultRes != nil && resultRes.Error() != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, resultRes
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	if len(resultMsgs) == 0 {
		if resultRes == nil {
			resultRes = response.NewAck()
		}
		return nil, resultRes
	}
	return resultMsgs, nil
}

// ExecuteCatchAll attempts to execute a slice of processors to only messages
// that have failed a processing step. Returns N resulting messages or a
// response. The response may indicate either a NoAck in the event of the
// message being buffered or an unrecoverable error.
func ExecuteCatchAll(procs []types.Processor, msgs ...types.Message) ([]types.Message, types.Response) {
	resultMsgs := make([]types.Message, len(msgs))
	copy(resultMsgs, msgs)

	var resultRes types.Response
	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []types.Message
		for _, m := range resultMsgs {
			// Skip messages that haven't failed a prior stage.
			if !HasFailed(m.Get(0)) {
				nextResultMsgs = append(nextResultMsgs, m)
				continue
			}
			var rMsgs []types.Message
			if rMsgs, resultRes = procs[i].ProcessMessage(m); resultRes != nil && resultRes.Error() != nil {
				// We immediately return if a processor hits an unrecoverable
				// error on a message.
				return nil, resultRes
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}

	if len(resultMsgs) == 0 {
		if resultRes == nil {
			resultRes = response.NewAck()
		}
		return nil, resultRes
	}
	return resultMsgs, nil
}

//------------------------------------------------------------------------------

// FailFlagKey is a metadata key used for flagging processor errors in Benthos.
// If a message part has any non-empty value for this metadata key then it will
// be interpretted as having failed a processor step somewhere in the pipeline.
var FailFlagKey = types.FailFlagKey

// FlagFail marks a message part as having failed at a processing step.
func FlagFail(part types.Part) {
	part.Metadata().Set(FailFlagKey, "true")
}

// FlagErr marks a message part as having failed at a processing step with an
// error message. If the error is nil the message part remains unchanged.
func FlagErr(part types.Part, err error) {
	if err != nil {
		part.Metadata().Set(FailFlagKey, err.Error())
	}
}

// GetFail returns an error string for a message part if it has failed, or an
// empty string if not.
func GetFail(part types.Part) string {
	return part.Metadata().Get(FailFlagKey)
}

// HasFailed checks whether a message part has failed a processing step.
func HasFailed(part types.Part) bool {
	return len(part.Metadata().Get(FailFlagKey)) > 0
}

// ClearFail removes any existing failure flags from a message part.
func ClearFail(part types.Part) {
	part.Metadata().Delete(FailFlagKey)
}

//------------------------------------------------------------------------------

func iterateParts(
	parts []int, msg types.Message,
	iter func(int, types.Part) error,
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
	operationName string, parts []int, msg types.Message,
	iter func(int, *tracing.Span, types.Part) error,
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

// Iterate the parts of a message, mutate them as required, and return either a
// boolean or an error. If the error is nil and the boolean is false then the
// message part is removed.
func iteratePartsFilterableWithSpan(
	operationName string, parts []int, msg types.Message,
	iter func(int, *tracing.Span, types.Part) (bool, error),
) {
	newParts := make([]types.Part, 0, msg.Len())
	exec := func(i int) bool {
		part := msg.Get(i)
		span := tracing.CreateChildSpan(operationName, part)

		var keep bool
		var err error
		if keep, err = iter(i, span, part); err != nil {
			FlagErr(part, err)
			span.SetTag("error", true)
			span.LogKV(
				"event", "error",
				"type", err.Error(),
			)
			keep = true
		}
		span.Finish()
		return keep
	}

	if len(parts) == 0 {
		for i := 0; i < msg.Len(); i++ {
			if exec(i) {
				newParts = append(newParts, msg.Get(i))
			}
		}
	} else {
		for _, i := range parts {
			if exec(i) {
				newParts = append(newParts, msg.Get(i))
			}
		}
	}

	msg.SetAll(newParts)
}

//------------------------------------------------------------------------------
