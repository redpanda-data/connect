package retries

import "github.com/benthosdev/benthos/v4/internal/docs"

// FieldSpecs returns documentation specs for retry fields.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldAdvanced("max_retries", "The maximum number of retries before giving up on the request. If set to zero there is no discrete limit."),
		docs.FieldAdvanced("backoff", "Control time intervals between retry attempts.").WithChildren(
			docs.FieldAdvanced("initial_interval", "The initial period to wait between retry attempts."),
			docs.FieldAdvanced("max_interval", "The maximum period to wait between retry attempts."),
			docs.FieldAdvanced("max_elapsed_time", "The maximum period to wait before retry attempts are abandoned. If zero then no limit is used."),
		),
	}
}
