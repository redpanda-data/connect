package twitter

import (
	_ "embed"
	// bloblang functions are registered in init functions under this package
	// so ensure they are loaded first
	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"

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
