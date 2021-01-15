package writer

//------------------------------------------------------------------------------

// AzureBlobStorageConfig contains configuration fields for the AzureBlobStorage output type.
type SFTPConfig struct {
	Server      string          `json:"server" yaml:"server"`
	Port        int             `json:"port" yaml:"port"`
	Filepath    string          `json:"filepath" yaml:"filepath"`
	Credentials SFTPCredentials `json:"credentials" yaml:"credentials"`
	MaxInFlight int             `json:"max_in_flight" yaml:"max_in_flight"`
}

type SFTPCredentials struct {
	Username string `json:"username" yaml:"username"`
	Secret   string `json:"secret" yaml:"secret"`
}

// NewAzureBlobStorageConfig creates a new Config with default values.
func NewSFTPConfig() SFTPConfig {
	return SFTPConfig{
		Server:   "",
		Port:     0,
		Filepath: "",
		Credentials: SFTPCredentials{
			Username: "",
			Secret:   "",
		},
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------
