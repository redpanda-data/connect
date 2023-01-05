package tracing

import "sync"

// EventType describes the type of event a component might experience during
// a config run.
type EventType string

// Various event types.
var (
	EventProduce EventType = "PRODUCE"
	EventConsume EventType = "CONSUME"
	EventDelete  EventType = "DELETE"
	EventError   EventType = "ERROR"
)

// NodeEvent represents a single event that occurred within the stream.
type NodeEvent struct {
	Type    EventType
	Content string
	Meta    map[string]any
}

// Summary is a high level description of all traced events.
type Summary struct {
	Input           uint64
	Output          uint64
	ProcessorErrors uint64

	inputEvents     sync.Map
	processorEvents sync.Map
	outputEvents    sync.Map
}

// NewSummary creates a new tracing summary that can be passed to component
// constructors for adding traces.
func NewSummary() *Summary {
	return &Summary{}
}

// InputEvents returns a map of input labels to events traced during the
// execution of a stream pipeline.
func (s *Summary) InputEvents() map[string][]NodeEvent {
	m := map[string][]NodeEvent{}
	s.inputEvents.Range(func(key, value any) bool {
		m[key.(string)] = value.(*events).Extract()
		return true
	})
	return m
}

// ProcessorEvents returns a map of processor labels to events traced during the
// execution of a stream pipeline.
func (s *Summary) ProcessorEvents() map[string][]NodeEvent {
	m := map[string][]NodeEvent{}
	s.processorEvents.Range(func(key, value any) bool {
		m[key.(string)] = value.(*events).Extract()
		return true
	})
	return m
}

// OutputEvents returns a map of output labels to events traced during the
// execution of a stream pipeline.
func (s *Summary) OutputEvents() map[string][]NodeEvent {
	m := map[string][]NodeEvent{}
	s.outputEvents.Range(func(key, value any) bool {
		m[key.(string)] = value.(*events).Extract()
		return true
	})
	return m
}

//------------------------------------------------------------------------------

func (s *Summary) wInputEvents(label string) (e *events, counter *uint64) {
	i, _ := s.inputEvents.LoadOrStore(label, &events{})
	return i.(*events), &s.Input
}

func (s *Summary) wOutputEvents(label string) (e *events, counter *uint64) {
	i, _ := s.outputEvents.LoadOrStore(label, &events{})
	return i.(*events), &s.Output
}

func (s *Summary) wProcessorEvents(label string) (e *events, errCounter *uint64) {
	i, _ := s.processorEvents.LoadOrStore(label, &events{})
	return i.(*events), &s.ProcessorErrors
}

type events struct {
	mut sync.Mutex
	m   []NodeEvent
}

func (e *events) Add(t EventType, content string, metadata map[string]any) {
	e.mut.Lock()
	defer e.mut.Unlock()

	e.m = append(e.m, NodeEvent{
		Type:    t,
		Content: content,
		Meta:    metadata,
	})
}

func (e *events) Extract() []NodeEvent {
	e.mut.Lock()
	defer e.mut.Unlock()

	eventsCopy := make([]NodeEvent, len(e.m))
	copy(eventsCopy, e.m)

	return eventsCopy
}
