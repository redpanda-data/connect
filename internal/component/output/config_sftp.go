package output

import (
	sftpSetup "github.com/benthosdev/benthos/v4/internal/impl/sftp/shared"
)

// SFTPConfig contains configuration fields for the SFTP output type.
type SFTPConfig struct {
	Address     string                `json:"address" yaml:"address"`
	Path        string                `json:"path" yaml:"path"`
	Codec       string                `json:"codec" yaml:"codec"`
	Credentials sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	MaxInFlight int                   `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewSFTPConfig creates a new Config with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address: "",
		Path:    "",
		Codec:   "all-bytes",
		Credentials: sftpSetup.Credentials{
			Username: "",
			Password: "",
		},
		MaxInFlight: 64,
	}
}
