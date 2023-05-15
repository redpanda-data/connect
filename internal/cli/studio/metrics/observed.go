package metrics

// ObservedInput is a subset of input metrics that we're interested in.
type ObservedInput struct {
	Received int64 `json:"received"`
}

// ObservedOutput is a subset of output metrics that we're interested in.
type ObservedOutput struct {
	Sent  int64 `json:"sent"`
	Error int64 `json:"error"`
}

// ObservedProcessor is a subset of processor metrics that we're interested in.
type ObservedProcessor struct {
	Received int64 `json:"received"`
	Sent     int64 `json:"sent"`
	Error    int64 `json:"error"`
}

// Observed is a subset of typical Benthos metrics collected by streams that
// we're interested in for studios purposes.
type Observed struct {
	Input     map[string]ObservedInput     `json:"input"`
	Processor map[string]ObservedProcessor `json:"processor"`
	Output    map[string]ObservedOutput    `json:"output"`
}

func newObserved() *Observed {
	return &Observed{
		Input:     map[string]ObservedInput{},
		Processor: map[string]ObservedProcessor{},
		Output:    map[string]ObservedOutput{},
	}
}
