package output

import "github.com/benthosdev/benthos/v4/internal/httpserver"

// HTTPServerConfig contains configuration fields for the HTTPServer output
// type.
type HTTPServerConfig struct {
	Address      string                `json:"address" yaml:"address"`
	Path         string                `json:"path" yaml:"path"`
	StreamPath   string                `json:"stream_path" yaml:"stream_path"`
	WSPath       string                `json:"ws_path" yaml:"ws_path"`
	AllowedVerbs []string              `json:"allowed_verbs" yaml:"allowed_verbs"`
	Timeout      string                `json:"timeout" yaml:"timeout"`
	CertFile     string                `json:"cert_file" yaml:"cert_file"`
	KeyFile      string                `json:"key_file" yaml:"key_file"`
	CORS         httpserver.CORSConfig `json:"cors" yaml:"cors"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:    "",
		Path:       "/get",
		StreamPath: "/get/stream",
		WSPath:     "/get/ws",
		AllowedVerbs: []string{
			"GET",
		},
		Timeout:  "5s",
		CertFile: "",
		KeyFile:  "",
		CORS:     httpserver.NewServerCORSConfig(),
	}
}
