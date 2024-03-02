package api

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpserver"
)

const (
	fieldAddress        = "address"
	fieldEnabled        = "enabled"
	fieldRootPath       = "root_path"
	fieldDebugEndpoints = "debug_endpoints"
	fieldCertFile       = "cert_file"
	fieldKeyFile        = "key_file"
	fieldCORS           = "cors"
	fieldBasicAuth      = "basic_auth"
)

// Config contains the configuration fields for the Benthos API.
type Config struct {
	Address        string                     `json:"address" yaml:"address"`
	Enabled        bool                       `json:"enabled" yaml:"enabled"`
	RootPath       string                     `json:"root_path" yaml:"root_path"`
	DebugEndpoints bool                       `json:"debug_endpoints" yaml:"debug_endpoints"`
	CertFile       string                     `json:"cert_file" yaml:"cert_file"`
	KeyFile        string                     `json:"key_file" yaml:"key_file"`
	CORS           httpserver.CORSConfig      `json:"cors" yaml:"cors"`
	BasicAuth      httpserver.BasicAuthConfig `json:"basic_auth" yaml:"basic_auth"`
}

// NewConfig creates a new API config with default values.
func NewConfig() Config {
	return Config{
		Address:        "0.0.0.0:4195",
		Enabled:        true,
		RootPath:       "/benthos",
		DebugEndpoints: false,
		CertFile:       "",
		KeyFile:        "",
		CORS:           httpserver.NewServerCORSConfig(),
		BasicAuth:      httpserver.NewBasicAuthConfig(),
	}
}

func FromParsed(pConf *docs.ParsedConfig) (conf Config, err error) {
	if conf.Address, err = pConf.FieldString(fieldAddress); err != nil {
		return
	}
	if conf.Enabled, err = pConf.FieldBool(fieldEnabled); err != nil {
		return
	}
	if conf.RootPath, err = pConf.FieldString(fieldRootPath); err != nil {
		return
	}
	if conf.DebugEndpoints, err = pConf.FieldBool(fieldDebugEndpoints); err != nil {
		return
	}
	if conf.CertFile, err = pConf.FieldString(fieldCertFile); err != nil {
		return
	}
	if conf.KeyFile, err = pConf.FieldString(fieldKeyFile); err != nil {
		return
	}
	if conf.CORS, err = httpserver.CORSConfigFromParsed(pConf); err != nil {
		return
	}
	if conf.BasicAuth, err = httpserver.BasicAuthConfigFromParsed(pConf); err != nil {
		return
	}
	return
}
