package processor

// BranchConfig contains configuration fields for the Branch processor.
type BranchConfig struct {
	RequestMap string   `json:"request_map" yaml:"request_map"`
	Processors []Config `json:"processors" yaml:"processors"`
	ResultMap  string   `json:"result_map" yaml:"result_map"`
}

// NewBranchConfig returns a BranchConfig with default values.
func NewBranchConfig() BranchConfig {
	return BranchConfig{
		RequestMap: "",
		Processors: []Config{},
		ResultMap:  "",
	}
}
