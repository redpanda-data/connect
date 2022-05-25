package processor

// GroupByElement represents a group determined by a condition and a list of
// group specific processors.
type GroupByElement struct {
	Check      string   `json:"check" yaml:"check"`
	Processors []Config `json:"processors" yaml:"processors"`
}

// GroupByConfig is a configuration struct containing fields for the GroupBy
// processor, which breaks message batches down into N batches of a smaller size
// according to conditions.
type GroupByConfig []GroupByElement

// NewGroupByConfig returns a GroupByConfig with default values.
func NewGroupByConfig() GroupByConfig {
	return GroupByConfig{}
}
