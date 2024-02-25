package api

import (
	"bytes"
	"text/template"

	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpserver"

	_ "embed"
)

// Spec returns a field spec for the API configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldBool(fieldEnabled, "Whether to enable to HTTP server.").HasDefault(true),
		docs.FieldString(fieldAddress, "The address to bind to.").HasDefault("0.0.0.0:4195"),
		docs.FieldString(
			fieldRootPath, "Specifies a general prefix for all endpoints, this can help isolate the service endpoints when using a reverse proxy with other shared services. All endpoints will still be registered at the root as well as behind the prefix, e.g. with a root_path set to `/foo` the endpoint `/version` will be accessible from both `/version` and `/foo/version`.",
		).HasDefault("/benthos"),
		docs.FieldBool(
			fieldDebugEndpoints, "Whether to register a few extra endpoints that can be useful for debugging performance or behavioral problems.",
		).HasDefault(false),
		docs.FieldString(fieldCertFile, "An optional certificate file for enabling TLS.").Advanced().HasDefault(""),
		docs.FieldString(fieldKeyFile, "An optional key file for enabling TLS.").Advanced().HasDefault(""),
		httpserver.ServerCORSFieldSpec(),
		httpserver.BasicAuthFieldSpec(),
	}
}

//go:embed docs.md
var httpDocs string

type templateContext struct {
	Fields         []docs.FieldSpecCtx
	CommonConfig   string
	AdvancedConfig string
}

// DocsMarkdown returns a markdown document for the http documentation.
func DocsMarkdown() ([]byte, error) {
	httpDocsTemplate := docs.FieldsTemplate(false) + httpDocs

	var buf bytes.Buffer
	err := template.Must(template.New("http").Parse(httpDocsTemplate)).Execute(&buf, templateContext{
		Fields: docs.FieldObject("", "").WithChildren(Spec()...).FlattenChildrenForDocs(),
		CommonConfig: `
http:
  address: 0.0.0.0:4195
  enabled: true
  root_path: /benthos
  debug_endpoints: false
`,
		AdvancedConfig: `
http:
  address: 0.0.0.0:4195
  enabled: true
  root_path: /benthos
  debug_endpoints: false
  cert_file: ""
  key_file: ""
  cors:
    enabled: false
    allowed_origins: []
  basic_auth:
    enabled: false
    username: ""
    password_hash: ""
    algorithm: "sha256"
    salt: ""
`,
	})

	return buf.Bytes(), err
}

// EndpointCaveats is a documentation section for HTTP components that explains
// some of the caveats in registering endpoints due to their non-deterministic
// ordering and lack of explicit path terminators.
func EndpointCaveats() string {
	return `:::caution Endpoint Caveats
Components within a Benthos config will register their respective endpoints in a non-deterministic order. This means that establishing precedence of endpoints that are registered via multiple ` + "`http_server`" + ` inputs or outputs (either within brokers or from cohabiting streams) is not possible in a predictable way.

This ambiguity makes it difficult to ensure that paths which are both a subset of a path registered by a separate component, and end in a slash (` + "`/`" + `) and will therefore match against all extensions of that path, do not prevent the more specific path from matching against requests.

It is therefore recommended that you ensure paths of separate components do not collide unless they are explicitly non-competing.

For example, if you were to deploy two separate ` + "`http_server`" + ` inputs, one with a path ` + "`/foo/`" + ` and the other with a path ` + "`/foo/bar`" + `, it would not be possible to ensure that the path ` + "`/foo/`" + ` does not swallow requests made to ` + "`/foo/bar`" + `.
:::`
}
