package client

import (
	"github.com/benthosdev/benthos/v4/public/service"
)

// NewConfigSpec constructs a new Couchbase ConfigSpec with common config fields
func NewConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// TODO Stable().
		Field(service.NewURLField("url").Description("Couchbase connection string.").Example("couchbase://localhost:11210")).
		Field(service.NewStringField("username").Description("Username to connect to the cluster.").Optional()).
		Field(service.NewStringField("password").Description("Password to connect to the cluster.").Secret().Optional()).
		Field(service.NewStringField("bucket").Description("Couchbase bucket.")).
		Field(service.NewStringField("collection").Description("Bucket collection.").Default("_default").Advanced().Optional()).
		Field(service.NewStringAnnotatedEnumField("transcoder", map[string]string{
			string(TranscoderRaw):       `RawBinaryTranscoder implements passthrough behavior of raw binary data. This transcoder does not apply any serialization. This will apply the following behavior to the value: binary ([]byte) -> binary bytes, binary expectedFlags. default -> error.`,
			string(TranscoderRawJSON):   `RawJSONTranscoder implements passthrough behavior of JSON data. This transcoder does not apply any serialization. It will forward data across the network without incurring unnecessary parsing costs. This will apply the following behavior to the value: binary ([]byte) -> JSON bytes, JSON expectedFlags. string -> JSON bytes, JSON expectedFlags. default -> error.`,
			string(TranscoderRawString): `RawStringTranscoder implements passthrough behavior of raw string data. This transcoder does not apply any serialization. This will apply the following behavior to the value: string -> string bytes, string expectedFlags. default -> error.`,
			string(TranscoderJSON):      `JSONTranscoder implements the default transcoding behavior and applies JSON transcoding to all values. This will apply the following behavior to the value: binary ([]byte) -> error. default -> JSON value, JSON Flags.`,
			string(TranscoderLegacy):    `LegacyTranscoder implements the behaviour for a backward-compatible transcoder. This transcoder implements behaviour matching that of gocb v1.This will apply the following behavior to the value: binary ([]byte) -> binary bytes, Binary expectedFlags. string -> string bytes, String expectedFlags. default -> JSON value, JSON expectedFlags.`,
		}).Description("Couchbase transcoder to use.").Default(string(TranscoderLegacy)).Advanced()).
		Field(service.NewDurationField("timeout").Description("Operation timeout.").Advanced().Default("15s"))
}
