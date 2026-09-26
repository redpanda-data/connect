// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"github.com/redpanda-data/benthos/v4/public/service"
)

// NewConfigSpec constructs a new Couchbase ConfigSpec with common config fields.
func NewConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		// TODO Stable().
		Field(service.NewURLField("url").Description("The Couchbase connection string.").Example("couchbase://localhost:11210")).
		Field(service.NewStringField("username").Description("A username to authenticate with the Couchbase cluster.").Optional()).
		Field(service.NewStringField("password").Description("A password to authenticate with the Couchbase cluster.").Secret().Optional()).
		Field(service.NewStringField("bucket").Description("The name of the Couchbase bucket to access.")).
		Field(service.NewStringField("collection").Description("The name of the collection within the bucket to access.").Advanced().Optional()).
		Field(service.NewStringField("scope").Description("Bucket scope.").Advanced().Optional()).
		Field(service.NewStringAnnotatedEnumField("transcoder", map[string]string{
			string(TranscoderRaw):       `RawBinaryTranscoder implements passthrough behavior of raw binary data. This transcoder does not apply any serialization. This will apply the following behavior to the value: binary ([]byte) -> binary bytes, binary expectedFlags. default -> error.`,
			string(TranscoderRawJSON):   `RawJSONTranscoder implements passthrough behavior of JSON data. This transcoder does not apply any serialization. It will forward data across the network without incurring unnecessary parsing costs. This will apply the following behavior to the value: binary ([]byte) -> JSON bytes, JSON expectedFlags. string -> JSON bytes, JSON expectedFlags. default -> error.`,
			string(TranscoderRawString): `RawStringTranscoder implements passthrough behavior of raw string data. This transcoder does not apply any serialization. This will apply the following behavior to the value: string -> string bytes, string expectedFlags. default -> error.`,
			string(TranscoderJSON):      `JSONTranscoder implements the default transcoding behavior and applies JSON transcoding to all values. This will apply the following behavior to the value: binary ([]byte) -> error. default -> JSON value, JSON Flags.`,
			string(TranscoderLegacy):    `LegacyTranscoder implements the behavior for a backward-compatible transcoder. This transcoder implements behavior matching that of gocb v1.This will apply the following behavior to the value: binary ([]byte) -> binary bytes, Binary expectedFlags. string -> string bytes, String expectedFlags. default -> JSON value, JSON expectedFlags.`,
		}).Description("The Couchbase transcoder to use for converting the data format of documents written to and read from Couchbase.").Default(string(TranscoderLegacy)).Advanced()).
		Field(service.NewDurationField("timeout").Description("The timeout for Couchbase operations, including connecting to the cluster and waiting for the bucket to become ready.").Advanced().Default("15s"))
}
