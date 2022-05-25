package processor

// SleepConfig contains configuration fields for the Sleep processor.
type SleepConfig struct {
	Duration string `json:"duration" yaml:"duration"`
}

// NewSleepConfig returns a SleepConfig with default values.
func NewSleepConfig() SleepConfig {
	return SleepConfig{
		Duration: "",
	}
}
