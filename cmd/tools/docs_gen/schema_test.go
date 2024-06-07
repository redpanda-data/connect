package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func TestComponentExamples(t *testing.T) {
	env := service.GlobalEnvironment()
	linter := env.FullConfigSchema("", "").NewStreamConfigLinter()
	linter.SetRejectDeprecated(true)
	linter.SetSkipEnvVarCheck(true)

	testComponent := func(name string, config *service.ConfigView) {
		data, err := config.TemplateData()
		require.NoError(t, err, name)

		t.Run(data.Type+":"+name, func(t *testing.T) {
			for _, e := range data.Examples {
				lints, err := linter.LintYAML([]byte(e.Config))
				require.NoError(t, err)
				for _, l := range lints {
					t.Error(l.Error())
				}
			}
		})
	}

	env.WalkBuffers(testComponent)
	env.WalkCaches(testComponent)
	env.WalkInputs(testComponent)
	env.WalkMetrics(testComponent)
	env.WalkOutputs(testComponent)
	env.WalkProcessors(testComponent)
	env.WalkRateLimits(testComponent)
	env.WalkScanners(testComponent)
	env.WalkTracers(testComponent)
}
