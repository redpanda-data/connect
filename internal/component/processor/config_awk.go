package processor

// AWKConfig contains configuration fields for the AWK processor.
type AWKConfig struct {
	Codec   string `json:"codec" yaml:"codec"`
	Program string `json:"program" yaml:"program"`
}

// NewAWKConfig returns a AWKConfig with default values.
func NewAWKConfig() AWKConfig {
	return AWKConfig{
		Codec:   "",
		Program: "",
	}
}
