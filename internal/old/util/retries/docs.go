package retries

import "github.com/benthosdev/benthos/v4/internal/docs"

// FieldSpecs returns documentation specs for retry fields.
func FieldSpecs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldInt("max_retries", "The maximum number of retries before giving up on the request. If set to zero there is no discrete limit.").Advanced(),
		docs.FieldObject("backoff", "Control time intervals between retry attempts.").WithChildren(
			docs.FieldString("initial_interval", "The initial period to wait between retry attempts."),
			docs.FieldString("max_interval", "The maximum period to wait between retry attempts."),
			docs.FieldString("max_elapsed_time", "The maximum period to wait before retry attempts are abandoned. If zero then no limit is used."),
		).Advanced(),
	}
}
