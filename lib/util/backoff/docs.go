package backoff

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
)

// FieldSpec returns a map of field specs for an Backoff type.
func FieldSpec() docs.FieldSpec {
	return docs.FieldAdvanced(
		"backoff", "Custom backoff settings can be used to override benthos defaults.",
	).WithChildren(
		docs.FieldString("type", "Backoff type to use.").HasAnnotatedOptions(
			"constant", "Constant retry timeout.",
			"exponential", "Exponential retry timeout.",
		).HasDefault("exponential"),
		docs.FieldString("interval", "The minimal period to wait between failed.").HasDefault("100ms"),
		docs.FieldString("max_interval", "The maximum period to wait between failed.").HasDefault("1s"),
		docs.FieldString("max_elapsed_time", "The maximum period to wait for success before stop. It never stops if max_elapsed_time is zero or empty."),
		docs.FieldInt("max_retries", "The maximum retries count before stop. Ignore if zero.").HasDefault(0),
		docs.FieldFloat("multiplier", "Backoff timeout multiplier.").HasDefault(1.5),
		docs.FieldFloat("randomization_factor", "Backoff timeout randomization factor.").HasDefault(0.5),
	)
}
