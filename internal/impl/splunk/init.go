package splunk

import (
	_ "embed"

	"github.com/redpanda-data/benthos/v4/public/service"
)

//go:embed template_output.yaml
var outputTemplate []byte

func init() {
	if err := service.RegisterTemplateYAML(string(outputTemplate)); err != nil {
		panic(err)
	}
}
