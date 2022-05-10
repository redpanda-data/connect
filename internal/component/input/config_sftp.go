package input

import (
	sftpSetup "github.com/benthosdev/benthos/v4/internal/impl/sftp/shared"
)

type watcherConfig struct {
	Enabled      bool   `json:"enabled" yaml:"enabled"`
	MinimumAge   string `json:"minimum_age" yaml:"minimum_age"`
	PollInterval string `json:"poll_interval" yaml:"poll_interval"`
	Cache        string `json:"cache" yaml:"cache"`
}

// SFTPConfig contains configuration fields for the SFTP input type.
type SFTPConfig struct {
	Address        string                `json:"address" yaml:"address"`
	Credentials    sftpSetup.Credentials `json:"credentials" yaml:"credentials"`
	Paths          []string              `json:"paths" yaml:"paths"`
	Codec          string                `json:"codec" yaml:"codec"`
	DeleteOnFinish bool                  `json:"delete_on_finish" yaml:"delete_on_finish"`
	MaxBuffer      int                   `json:"max_buffer" yaml:"max_buffer"`
	Watcher        watcherConfig         `json:"watcher" yaml:"watcher"`
}

// NewSFTPConfig creates a new SFTPConfig with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Address:        "",
		Credentials:    sftpSetup.Credentials{},
		Paths:          []string{},
		Codec:          "all-bytes",
		DeleteOnFinish: false,
		MaxBuffer:      1000000,
		Watcher: watcherConfig{
			Enabled:      false,
			MinimumAge:   "1s",
			PollInterval: "1s",
			Cache:        "",
		},
	}
}
