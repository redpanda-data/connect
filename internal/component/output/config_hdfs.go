package output

import "github.com/benthosdev/benthos/v4/internal/batch/policy/batchconfig"

// HDFSConfig contains configuration fields for the HDFS output type.
type HDFSConfig struct {
	Hosts       []string           `json:"hosts" yaml:"hosts"`
	User        string             `json:"user" yaml:"user"`
	Directory   string             `json:"directory" yaml:"directory"`
	Path        string             `json:"path" yaml:"path"`
	MaxInFlight int                `json:"max_in_flight" yaml:"max_in_flight"`
	Batching    batchconfig.Config `json:"batching" yaml:"batching"`
}

// NewHDFSConfig creates a new Config with default values.
func NewHDFSConfig() HDFSConfig {
	return HDFSConfig{
		Hosts:       []string{},
		User:        "",
		Directory:   "",
		Path:        `${!count("files")}-${!timestamp_unix_nano()}.txt`,
		MaxInFlight: 64,
		Batching:    batchconfig.NewConfig(),
	}
}
