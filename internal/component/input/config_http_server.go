package input

import (
	"github.com/benthosdev/benthos/v4/internal/httpserver"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

// HTTPServerResponseConfig provides config fields for customising the response
// given from successful requests.
type HTTPServerResponseConfig struct {
	Status          string                       `json:"status" yaml:"status"`
	Headers         map[string]string            `json:"headers" yaml:"headers"`
	ExtractMetadata metadata.IncludeFilterConfig `json:"metadata_headers" yaml:"metadata_headers"`
}

// NewHTTPServerResponseConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerResponseConfig() HTTPServerResponseConfig {
	return HTTPServerResponseConfig{
		Status: "200",
		Headers: map[string]string{
			"Content-Type": "application/octet-stream",
		},
		ExtractMetadata: metadata.NewIncludeFilterConfig(),
	}
}

// HTTPServerConfig contains configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address            string                   `json:"address" yaml:"address"`
	Path               string                   `json:"path" yaml:"path"`
	WSPath             string                   `json:"ws_path" yaml:"ws_path"`
	WSWelcomeMessage   string                   `json:"ws_welcome_message" yaml:"ws_welcome_message"`
	WSRateLimitMessage string                   `json:"ws_rate_limit_message" yaml:"ws_rate_limit_message"`
	AllowedVerbs       []string                 `json:"allowed_verbs" yaml:"allowed_verbs"`
	Timeout            string                   `json:"timeout" yaml:"timeout"`
	RateLimit          string                   `json:"rate_limit" yaml:"rate_limit"`
	CertFile           string                   `json:"cert_file" yaml:"cert_file"`
	KeyFile            string                   `json:"key_file" yaml:"key_file"`
	CORS               httpserver.CORSConfig    `json:"cors" yaml:"cors"`
	Response           HTTPServerResponseConfig `json:"sync_response" yaml:"sync_response"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:            "",
		Path:               "/post",
		WSPath:             "/post/ws",
		WSWelcomeMessage:   "",
		WSRateLimitMessage: "",
		AllowedVerbs: []string{
			"POST",
		},
		Timeout:   "5s",
		RateLimit: "",
		CertFile:  "",
		KeyFile:   "",
		CORS:      httpserver.NewServerCORSConfig(),
		Response:  NewHTTPServerResponseConfig(),
	}
}
