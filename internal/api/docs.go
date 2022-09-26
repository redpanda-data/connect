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
