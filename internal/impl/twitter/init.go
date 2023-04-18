package twitter

import (
	_ "embed"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/template"
)

//go:embed template_search_input.yaml
var searchInputTemplate []byte

func init() {
	if err := template.RegisterTemplateYAML(bundle.GlobalEnvironment, searchInputTemplate); err != nil {
		panic(err)
	}
}
