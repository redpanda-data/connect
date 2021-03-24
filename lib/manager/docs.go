package manager

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/gabs/v2"
)

func lintResource(v interface{}) []docs.Lint {
	gObj := gabs.Wrap(v)
	label, _ := gObj.S("label").Data().(string)
	if label == "" {
		return []docs.Lint{
			docs.NewLintError(0, "The label field for resources must be unique and not empty"),
		}
	}
	return nil
}

// Spec returns a field spec for the manager configuration.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon(
			"resources", "A map of components identified by unique names that can be referenced throughout a Benthos config.",
		).WithChildren(
			docs.FieldCommon("inputs", "A map of inputs.").Map().HasType(docs.FieldInput),
			docs.FieldCommon("conditions", "A map of conditions.").Map().HasType(docs.FieldCondition),
			docs.FieldCommon("processors", "A map of processors.").Map().HasType(docs.FieldProcessor),
			docs.FieldCommon("outputs", "A map of outputs.").Map().HasType(docs.FieldOutput),
			docs.FieldCommon("caches", "A map of caches.").Map().HasType(docs.FieldCache),
			docs.FieldCommon("rate_limits", "A map of rate limits.").Map().HasType(docs.FieldRateLimit),
			docs.FieldAdvanced("plugins", "A map of resource plugins.").Map().HasType(docs.FieldObject),
		).OmitWhen(func(field, parent interface{}) (string, bool) {
			if len(gabs.Wrap(field).ChildrenMap()) == 0 {
				return "resources should be omitted when empty", true
			}
			return "", false
		}),

		docs.FieldCommon(
			"resource_inputs", "A list of input resources, each must have a unique label.",
		).Array().HasType(docs.FieldInput).Linter(lintResource),

		docs.FieldCommon(
			"resource_processors", "A list of processor resources, each must have a unique label.",
		).Array().HasType(docs.FieldProcessor).Linter(lintResource),

		docs.FieldCommon(
			"resource_outputs", "A list of output resources, each must have a unique label.",
		).Array().HasType(docs.FieldOutput).Linter(lintResource),

		docs.FieldCommon(
			"resource_caches", "A list of cache resources, each must have a unique label.",
		).Array().HasType(docs.FieldCache).Linter(lintResource),

		docs.FieldCommon(
			"resource_rate_limits", "A list of rate limit resources, each must have a unique label.",
		).Array().HasType(docs.FieldRateLimit).Linter(lintResource),
	}
}
