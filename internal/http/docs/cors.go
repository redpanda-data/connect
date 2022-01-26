package docs

import (
	"errors"
	"net/http"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/gorilla/handlers"
)

// ServerCORS contains configuration for allowing CORS headers.
type ServerCORS struct {
	Enabled        bool     `json:"enabled" yaml:"enabled"`
	AllowedOrigins []string `json:"allowed_origins" yaml:"allowed_origins"`
}

// NewServerCORS returns a new server CORS config with default fields.
func NewServerCORS() ServerCORS {
	return ServerCORS{
		Enabled:        false,
		AllowedOrigins: []string{},
	}
}

// WrapHandler wraps a provided HTTP handler with middleware that enables CORS
// requests (when configured).
func (conf ServerCORS) WrapHandler(handler http.Handler) (http.Handler, error) {
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
	return docs.FieldAdvanced("cors", "Adds Cross-Origin Resource Sharing headers.").WithChildren(
		docs.FieldBool("enabled", "Whether to allow CORS requests.").HasDefault(false),
		docs.FieldString("allowed_origins", "An explicit list of origins that are allowed for CORS requests.").Array().HasDefault([]string{}),
	)
}
