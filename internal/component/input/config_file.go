package input

// FileConfig contains configuration values for the File input type.
type FileConfig struct {
	Paths          []string `json:"paths" yaml:"paths"`
	Codec          string   `json:"codec" yaml:"codec"`
	MaxBuffer      int      `json:"max_buffer" yaml:"max_buffer"`
	DeleteOnFinish bool     `json:"delete_on_finish" yaml:"delete_on_finish"`
}

// NewFileConfig creates a new FileConfig with default values.
func NewFileConfig() FileConfig {
	return FileConfig{
		Paths:          []string{},
		Codec:          "lines",
		MaxBuffer:      1000000,
		DeleteOnFinish: false,
	}
}
