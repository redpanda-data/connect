package log

import "github.com/Jeffail/benthos/v3/internal/docs"

// Spec returns a field spec for the logger configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("level", "Set the minimum severity level for emitting logs.").HasOptions(
			"OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL",
		),
		docs.FieldCommon("format", "Set the format of emitted logs.").HasOptions(
			"json", "logfmt", "classic",
		),
		docs.FieldCommon("add_timestamp", "Whether to include timestamps in logs."),
		docs.FieldCommon("static_fields", "A map of key/value pairs to add to each structured log."),
		docs.FieldDeprecated("prefix"),
		docs.FieldDeprecated("json_format"),
	}
}
