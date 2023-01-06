package svcdiscover

import (
	"bytes"
	"text/template"

	_ "embed"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Spec returns a field spec for the Sd configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("label", "The label.").HasDefault(""),
		docs.FieldString("type", "The type.").HasDefault("service_discover"),
		docs.FieldBool("enabled", "Whether to enable to Service discover/register, When HTTP is enabled").HasDefault(false),
		NacosSpec(),
	}
}

//go:embed docs.md
var sdDocs string

type templateContext struct {
	Fields         []docs.FieldSpecCtx
	CommonConfig   string
	AdvancedConfig string
}

// DocsMarkdown returns a markdown document for the http documentation.
func DocsMarkdown() ([]byte, error) {
	sdDocsTemplate := docs.FieldsTemplate(false) + sdDocs

	var buf bytes.Buffer
	err := template.Must(template.New("Serivce Discover").Parse(sdDocsTemplate)).Execute(&buf, templateContext{
		Fields: docs.FieldObject("", "").WithChildren(Spec()...).FlattenChildrenForDocs(),
		CommonConfig: `
sd:
  nacos:
    server_addr: 127.0.0.1
    server_port: 8848
    namespace: public
    service_name: "benthos"
`,
		AdvancedConfig: `
sd:
  nacos:
    server_addr: 127.0.0.1
    server_port: 8848
    namespace: public
    service_name: "benthos"
    registry_ip: "192.16.16.120"
`,
	})

	return buf.Bytes(), err
}
