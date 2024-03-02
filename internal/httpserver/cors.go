package httpserver

import (
	"errors"
	"net/http"

	"github.com/gorilla/handlers"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

const (
	fieldCORS               = "cors"
	fieldCORSEnabled        = "enabled"
	fieldCORSAllowedOrigins = "allowed_origins"
)

// CORSConfig contains struct configuration for allowing CORS headers.
type CORSConfig struct {
	Enabled        bool     `json:"enabled" yaml:"enabled"`
	AllowedOrigins []string `json:"allowed_origins" yaml:"allowed_origins"`
}

// NewServerCORSConfig returns a new server CORS config with default fields.
func NewServerCORSConfig() CORSConfig {
	return CORSConfig{
		Enabled:        false,
		AllowedOrigins: []string{},
	}
}

// WrapHandler wraps a provided HTTP handler with middleware that enables CORS
// requests (when configured).
func (conf CORSConfig) WrapHandler(handler http.Handler) (http.Handler, error) {
	if !conf.Enabled {
		return handler, nil
	}
	if len(conf.AllowedOrigins) == 0 {
		return nil, errors.New("must specify at least one allowed origin")
	}
	return handlers.CORS(
		handlers.AllowedOrigins(conf.AllowedOrigins),
		handlers.AllowedMethods([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}),
	)(handler), nil
}

// ServerCORSFieldSpec returns a field spec for an http server CORS component.
func ServerCORSFieldSpec() docs.FieldSpec {
	return docs.FieldObject(fieldCORS, "Adds Cross-Origin Resource Sharing headers.").WithChildren(
		docs.FieldBool(fieldCORSEnabled, "Whether to allow CORS requests.").HasDefault(false),
		docs.FieldString(fieldCORSAllowedOrigins, "An explicit list of origins that are allowed for CORS requests.").Array().HasDefault([]any{}),
	).AtVersion("3.63.0").Advanced()
}

func CORSConfigFromParsed(pConf *docs.ParsedConfig) (conf CORSConfig, err error) {
	pConf = pConf.Namespace(fieldCORS)
	if conf.Enabled, err = pConf.FieldBool(fieldCORSEnabled); err != nil {
		return
	}
	if conf.AllowedOrigins, err = pConf.FieldStringList(fieldCORSAllowedOrigins); err != nil {
		return
	}
	return
}
