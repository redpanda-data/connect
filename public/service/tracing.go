package service

import (
	"sync/atomic"

	"github.com/benthosdev/benthos/v4/internal/bundle/tracing"
)

// TracingEventType describes the type of tracing event a component might
// experience during a config run.
//
// Experimental: This type may change outside of major version releases.
type TracingEventType string

// Various tracing event types.
//
// Experimental: This type may change outside of major version releases.
var (
	// Note: must match up with ./internal/bundle/tracing/events.go.
	TracingEventProduce TracingEventType = "PRODUCE"
	TracingEventConsume TracingEventType = "CONSUME"
	TracingEventDelete  TracingEventType = "DELETE"
	TracingEventError   TracingEventType = "ERROR"
	TracingEventUnknown TracingEventType = "UNKNOWN"
)

func convertTracingEventType(t tracing.EventType) TracingEventType {
	switch t {
	case tracing.EventProduce:
		return TracingEventProduce
	case tracing.EventConsume:
		return TracingEventConsume
	case tracing.EventDelete:
		return TracingEventDelete
	case tracing.EventError:
		return TracingEventError
	}
	return TracingEventUnknown
}

// TracingEvent represents a single event that occurred within the stream.
//
// Experimental: This type may change outside of major version releases.
type TracingEvent struct {
	Type    TracingEventType
	Content string
	Meta    map[string]any
}

// TracingSummary is a high level description of all traced events. When tracing
// a stream this should only be queried once the stream has ended.
//
// Experimental: This type may change outside of major version releases.
type TracingSummary struct {
	summary *tracing.Summary
}

// TotalInput returns the total traced input messages received.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) TotalInput() uint64 {
	return atomic.LoadUint64(&s.summary.Input)
}

// TotalProcessorErrors returns the total traced processor errors occurred.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) TotalProcessorErrors() uint64 {
	return atomic.LoadUint64(&s.summary.ProcessorErrors)
}

// TotalOutput returns the total traced output messages received.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) TotalOutput() uint64 {
	return atomic.LoadUint64(&s.summary.Output)
}

// InputEvents returns a map of input labels to events traced during the
// execution of a stream pipeline.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) InputEvents() map[string][]TracingEvent {
	m := map[string][]TracingEvent{}
	for k, v := range s.summary.InputEvents() {
		events := make([]TracingEvent, len(v))
		for i, e := range v {
			events[i] = TracingEvent{
				Type:    convertTracingEventType(e.Type),
				Content: e.Content,
				Meta:    e.Meta,
			}
		}
		m[k] = events
	}
	return m
}

// ProcessorEvents returns a map of processor labels to events traced during the
// execution of a stream pipeline.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) ProcessorEvents() map[string][]TracingEvent {
	m := map[string][]TracingEvent{}
	for k, v := range s.summary.ProcessorEvents() {
		events := make([]TracingEvent, len(v))
		for i, e := range v {
			events[i] = TracingEvent{
				Type:    convertTracingEventType(e.Type),
				Content: e.Content,
				Meta:    e.Meta,
			}
		}
		m[k] = events
	}
	return m
}

// OutputEvents returns a map of output labels to events traced during the
// execution of a stream pipeline.
//
// Experimental: This method may change outside of major version releases.
func (s *TracingSummary) OutputEvents() map[string][]TracingEvent {
	m := map[string][]TracingEvent{}
	for k, v := range s.summary.OutputEvents() {
		events := make([]TracingEvent, len(v))
		for i, e := range v {
			events[i] = TracingEvent{
				Type:    convertTracingEventType(e.Type),
				Content: e.Content,
				Meta:    e.Meta,
			}
		}
		m[k] = events
	}
	return m
}
