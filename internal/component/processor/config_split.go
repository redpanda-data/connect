package processor

// SplitConfig is a configuration struct containing fields for the Split
// processor, which breaks message batches down into batches of a smaller size.
type SplitConfig struct {
	Size     int `json:"size" yaml:"size"`
	ByteSize int `json:"byte_size" yaml:"byte_size"`
}

// NewSplitConfig returns a SplitConfig with default values.
func NewSplitConfig() SplitConfig {
	return SplitConfig{
		Size:     1,
		ByteSize: 0,
	}
}
