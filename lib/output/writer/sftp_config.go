package writer

import sftpSetup "github.com/Jeffail/benthos/v3/internal/service/sftp"

//------------------------------------------------------------------------------

// SFTPConfig contains configuration fields for the SFTP output type.
type SFTPConfig struct {
	Address               string           `json:"address" yaml:"address"`
	Path                  string           `json:"path" yaml:"path"`
	Credentials           sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	MaxInFlight           int              `json:"max_in_flight" yaml:"max_in_flight"`
	MaxConnectionAttempts int              `json:"max_connection_attempts" yaml:"max_connection_attempts"`
	RetrySleepDuration    string           `json:"retry_sleep_duration" yaml:"retry_sleep_duration"`
}

// NewSFTPConfig creates a new Config with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address: "",
		Path:    "",
		Credentials: sftpSetup.Credentials{
			Username: "",
			Password: "",
		},
		MaxInFlight:           1,
		MaxConnectionAttempts: 10,
		RetrySleepDuration:    "5s",
	}
}

//------------------------------------------------------------------------------
