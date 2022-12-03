package client

import (
	"github.com/benthosdev/benthos/v4/public/service"
)

// NewConfigSpec constructs a new Couchbase ConfigSpec with common config fields
func NewConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// TODO Stable().
		Field(service.NewStringField("url").Description("Couchbase connection string.").Example("couchbase://localhost:11210")).
		Field(service.NewStringField("username").Description("Username to connect to the cluster.").Optional()).
		Field(service.NewStringField("password").Description("Password to connect to the cluster.").Optional()).
		Field(service.NewStringField("bucket").Description("Couchbase bucket.")).
		Field(service.NewStringField("collection").Description("Bucket collection.").Default("_default").Advanced().Optional()).
		Field(service.NewStringAnnotatedEnumField("transcoder", map[string]string{
			string(TranscoderRaw):       "fetch a document.",
			string(TranscoderRawJSON):   "delete a document.",
			string(TranscoderRawString): "replace the contents of a document.",
			string(TranscoderJSON):      "insert a new document.",
			string(TranscoderLegacy):    "creates a new document if it does not exist, if it does exist then it updates it.",
		}).Description("Couchbase transcoder to use.").Default(string(TranscoderLegacy)).Advanced()).
		Field(service.NewDurationField("timeout").Description("Operation timeout.").Advanced().Default("15s"))
}
