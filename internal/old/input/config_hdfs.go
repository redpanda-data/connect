package input

// HDFSConfig contains configuration fields for the HDFS input type.
type HDFSConfig struct {
	Hosts     []string `json:"hosts" yaml:"hosts"`
	User      string   `json:"user" yaml:"user"`
	Directory string   `json:"directory" yaml:"directory"`
}

// NewHDFSConfig creates a new Config with default values.
func NewHDFSConfig() HDFSConfig {
	return HDFSConfig{
		Hosts:     []string{},
		User:      "",
		Directory: "",
	}
}
