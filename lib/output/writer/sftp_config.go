package writer

//------------------------------------------------------------------------------

// SFTPConfig contains configuration fields for the SFTP output type.
type SFTPConfig struct {
	Server                string          `json:"server" yaml:"server"`
	Port                  int             `json:"port" yaml:"port"`
	Path                  string          `json:"path" yaml:"path"`
	Credentials           SFTPCredentials `json:"credentials" yaml:"credentials"`
	MaxInFlight           int             `json:"max_in_flight" yaml:"max_in_flight"`
	MaxConnectionAttempts int             `json:"max_connection_attempts" yaml:"max_connection_attempts"`
	RetrySleepDuration    int             `json:"retry_sleep_duration" yaml:"retry_sleep_duration"`
}

type SFTPCredentials struct {
	Username string `json:"username" yaml:"username"`
	Secret   string `json:"secret" yaml:"secret"`
}

// NewSFTPConfig creates a new Config with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Server: "",
		Port:   0,
		Path:   "",
		Credentials: SFTPCredentials{
			Username: "",
			Secret:   "",
		},
		MaxInFlight:           1,
		MaxConnectionAttempts: 10,
		RetrySleepDuration:    5000,
	}
}

//------------------------------------------------------------------------------
