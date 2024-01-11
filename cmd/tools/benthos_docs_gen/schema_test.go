package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestComponentExamples(t *testing.T) {
	confSpec := config.Spec()
	testComponent := func(componentType, typeName, title, conf string, deprecated bool) {
		node, err := docs.UnmarshalYAML([]byte(conf))
		require.NoError(t, err, "%v:%v:%v", componentType, typeName, title)

		pConf, err := confSpec.ParsedConfigFromAny(node)
		require.NoError(t, err, "%v:%v:%v", componentType, typeName, title)

		_, err = config.FromParsed(docs.DeprecatedProvider, pConf)
		require.NoError(t, err, "%v:%v:%v", componentType, typeName, title)

		lConf := docs.NewLintConfig()
		lConf.RejectDeprecated = !deprecated
		lints := confSpec.LintYAML(docs.NewLintContext(lConf), node)
		for _, lint := range lints {
			t.Errorf("%v %v:%v:%v", lint, componentType, typeName, title)
		}
	}

	for _, spec := range bundle.AllInputs.Docs() {
		for _, example := range spec.Examples {
			testComponent("input", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
	for _, spec := range bundle.AllBuffers.Docs() {
		for _, example := range spec.Examples {
			testComponent("buffer", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
	for _, spec := range bundle.AllProcessors.Docs() {
		for _, example := range spec.Examples {
			testComponent("processor", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
	for _, spec := range bundle.AllOutputs.Docs() {
		for _, example := range spec.Examples {
			testComponent("output", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
	for _, spec := range bundle.AllCaches.Docs() {
		for _, example := range spec.Examples {
			testComponent("cache", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
	for _, spec := range bundle.AllRateLimits.Docs() {
		for _, example := range spec.Examples {
			testComponent("ratelimit", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
	for _, spec := range bundle.AllScanners.Docs() {
		for _, example := range spec.Examples {
			testComponent("scanner", spec.Name, example.Title, example.Config, spec.Status == docs.StatusDeprecated)
		}
	}
}
