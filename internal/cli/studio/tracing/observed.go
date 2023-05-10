package tracing

import (
	"github.com/benthosdev/benthos/v4/internal/bundle/tracing"
)

type ObservedSummary struct {
	Input           int `json:"input"`
	Output          int `json:"output"`
	ProcessorErrors int `json:"processor_errors"`
}

type ObservedEvent struct {
	Type     string         `json:"type"`
	Content  string         `json:"content"`
	Metadata map[string]any `json:"metadata"`
}

// Observed is a structured form of tracing events extracted from Benthos
// components during execution. This is entirely unrelated to Open Telemetry
// tracing concepts and is Benthos specific.
type Observed struct {
	InputEvents     map[string][]ObservedEvent `json:"input_events,omitempty"`
	ProcessorEvents map[string][]ObservedEvent `json:"processor_events,omitempty"`
	OutputEvents    map[string][]ObservedEvent `json:"output_events,omitempty"`
}

// FromInternal converts internal tracing events into a format we can serialise
// as JSON for Studio sync requests. A nil might be returned if no events were
// extracted.
func FromInternal(summary *tracing.Summary) *Observed {
	inputEvents := map[string][]ObservedEvent{}
	for k, v := range summary.InputEvents(true) {
		var tEvents []ObservedEvent
		for _, e := range v {
			tEvents = append(tEvents, ObservedEvent{
				Type:     string(e.Type),
				Content:  e.Content,
				Metadata: e.Meta,
			})
		}
		if len(tEvents) > 0 {
			inputEvents[k] = tEvents
		}
	}

	processorEvents := map[string][]ObservedEvent{}
	for k, v := range summary.ProcessorEvents(true) {
		var tEvents []ObservedEvent
		for _, e := range v {
			tEvents = append(tEvents, ObservedEvent{
				Type:     string(e.Type),
				Content:  e.Content,
				Metadata: e.Meta,
			})
		}
		if len(tEvents) > 0 {
			processorEvents[k] = tEvents
		}
	}

	outputEvents := map[string][]ObservedEvent{}
	for k, v := range summary.OutputEvents(true) {
		var tEvents []ObservedEvent
		for _, e := range v {
			tEvents = append(tEvents, ObservedEvent{
				Type:     string(e.Type),
				Content:  e.Content,
				Metadata: e.Meta,
			})
		}
		if len(tEvents) > 0 {
			outputEvents[k] = tEvents
		}
	}

	if len(inputEvents)+len(outputEvents)+len(processorEvents) == 0 {
		return nil
	}

	return &Observed{
		InputEvents:     inputEvents,
		ProcessorEvents: processorEvents,
		OutputEvents:    outputEvents,
	}
}
