package log

import "github.com/Jeffail/benthos/v3/internal/docs"

// Spec returns a field spec for the logger configuration fields.
func Spec() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldString("level", "Set the minimum severity level for emitting logs.").HasOptions(
			"OFF", "FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ALL",
		).HasDefault("INFO"),
		docs.FieldString("format", "Set the format of emitted logs.").HasOptions("json", "logfmt").HasDefault("logfmt"),
		docs.FieldBool("add_timestamp", "Whether to include timestamps in logs.").HasDefault(false),
		docs.FieldString("static_fields", "A map of key/value pairs to add to each structured log.").Map().HasDefault(map[string]string{
			"@service": "benthos",
		}),
	}
}
