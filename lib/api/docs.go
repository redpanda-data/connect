package api

import "github.com/Jeffail/benthos/v3/internal/docs"

// Spec returns a field spec for the API configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("enabled", "Whether to enable to HTTP server."),
		docs.FieldCommon("address", "The address to bind to."),
		docs.FieldCommon("root_path", "Specifies a general prefix for all endpoints, this can help isolate the service endpoints when using a reverse proxy with other shared services. All endpoints will still be registered at the root as well as behind the prefix, e.g. with a root_path set to `/foo` the endpoint `/version` will be accessible from both `/version` and `/foo/version`."),
		docs.FieldAdvanced("debug_endpoints", "Whether to register a few extra endpoints that can be useful for debugging performance or behavioral problems."),
		docs.FieldAdvanced("cert_file", "An optional certificate file for enabling TLS."),
		docs.FieldAdvanced("key_file", "An optional key file for enabling TLS."),
		docs.FieldDeprecated("read_timeout"),
	}
}
