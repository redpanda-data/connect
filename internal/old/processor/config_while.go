package processor

// WhileConfig is a config struct containing fields for the While
// processor.
type WhileConfig struct {
	AtLeastOnce bool     `json:"at_least_once" yaml:"at_least_once"`
	MaxLoops    int      `json:"max_loops" yaml:"max_loops"`
	Check       string   `json:"check" yaml:"check"`
	Processors  []Config `json:"processors" yaml:"processors"`
}

// NewWhileConfig returns a default WhileConfig.
func NewWhileConfig() WhileConfig {
	return WhileConfig{
		AtLeastOnce: false,
		MaxLoops:    0,
		Check:       "",
		Processors:  []Config{},
	}
}
