package manager

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/gabs/v2"
)

func lintResource(ctx docs.LintContext, line, col int, v interface{}) []docs.Lint {
	if _, ok := v.(map[string]interface{}); !ok {
		return nil
	}
	gObj := gabs.Wrap(v)
	label, _ := gObj.S("label").Data().(string)
	if label == "" {
		return []docs.Lint{
			docs.NewLintError(line, "The label field for resources must be unique and not empty"),
		}
	}
	return nil
}

// Spec returns a field spec for the manager configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon(
			"input_resources", "A list of input resources, each must have a unique label.",
		).Array().HasType(docs.FieldTypeInput).Linter(lintResource),

		docs.FieldCommon(
			"processor_resources", "A list of processor resources, each must have a unique label.",
		).Array().HasType(docs.FieldTypeProcessor).Linter(lintResource),

		docs.FieldCommon(
			"output_resources", "A list of output resources, each must have a unique label.",
		).Array().HasType(docs.FieldTypeOutput).Linter(lintResource),

		docs.FieldCommon(
			"cache_resources", "A list of cache resources, each must have a unique label.",
		).Array().HasType(docs.FieldTypeCache).Linter(lintResource),

		docs.FieldCommon(
			"rate_limit_resources", "A list of rate limit resources, each must have a unique label.",
		).Array().HasType(docs.FieldTypeRateLimit).Linter(lintResource),
	}
}
