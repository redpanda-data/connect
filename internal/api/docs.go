package api

import (
	"github.com/benthosdev/benthos/v4/internal/docs"
	httpdocs "github.com/benthosdev/benthos/v4/internal/http/docs"
)

// Spec returns a field spec for the API configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldBool("enabled", "Whether to enable to HTTP server.").HasDefault(true),
		docs.FieldString("address", "The address to bind to.").HasDefault("0.0.0.0:4195"),
		docs.FieldString(
			"root_path", "Specifies a general prefix for all endpoints, this can help isolate the service endpoints when using a reverse proxy with other shared services. All endpoints will still be registered at the root as well as behind the prefix, e.g. with a root_path set to `/foo` the endpoint `/version` will be accessible from both `/version` and `/foo/version`.",
		).HasDefault("/benthos"),
		docs.FieldBool(
			"debug_endpoints", "Whether to register a few extra endpoints that can be useful for debugging performance or behavioral problems.",
		).HasDefault(false),
		docs.FieldString("cert_file", "An optional certificate file for enabling TLS.").Advanced().HasDefault(""),
		docs.FieldString("key_file", "An optional key file for enabling TLS.").Advanced().HasDefault(""),
		httpdocs.ServerCORSFieldSpec(),
		docs.FieldObject("basic_auth", "Allows you to specify basic authentication.").WithChildren(
			docs.FieldBool("enabled", "Whether to use basic authentication in requests.").HasDefault(false),
			docs.FieldString("username", "Username required to authenticate.").HasDefault(""),
			docs.FieldString("password_hash", "Hashed password required to authenticate. (base64 encoded)").HasDefault(""),
			docs.FieldString("realm", "realm of the protection space.").HasDefault("restricted"),
			docs.FieldString("algorithm", "Encryption algorithm used to generate password_hash.", "md5", "sha256", "bcrypt", "scrypt").HasDefault("sha256"),
			docs.FieldString("salt", "Salt for scrypt algorithm. (base64 encoded)").HasDefault(""),
		).Advanced(),
	}
}
