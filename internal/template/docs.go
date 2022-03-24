package template

import (
	"bytes"
	"text/template"

	"github.com/benthosdev/benthos/v4/internal/docs"

	_ "embed"
)

//go:embed docs.md
var templateDocs string

type templateContext struct {
	Fields []docs.FieldSpecCtx
}

// DocsMarkdown returns a markdown document for the templates documentation.
func DocsMarkdown() ([]byte, error) {
	templateDocsTemplate := docs.FieldsTemplate(false) + templateDocs

	var buf bytes.Buffer
	err := template.Must(template.New("templates").Parse(templateDocsTemplate)).Execute(&buf, templateContext{
		Fields: docs.FieldObject("", "").WithChildren(ConfigSpec()...).FlattenChildrenForDocs(),
	})

	return buf.Bytes(), err
}
