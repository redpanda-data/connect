package twitter

import (
	_ "embed"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/template"
)

//go:embed template_input.yaml
var inputTemplate []byte

//go:embed template_output.yaml
var outputTemplate []byte

func init() {
	if err := template.RegisterTemplateYAML(bundle.GlobalEnvironment, inputTemplate); err != nil {
		panic(err)
	}
	if err := template.RegisterTemplateYAML(bundle.GlobalEnvironment, outputTemplate); err != nil {
		panic(err)
	}
}
