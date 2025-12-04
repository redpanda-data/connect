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

package msgpack

import (
	"github.com/vmihailenco/msgpack/v5"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	msgpackParseSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Parses MessagePack binary data into a structured object. MessagePack is an efficient binary serialization format that is more compact than JSON while maintaining similar data structures. Commonly used for high-performance APIs and data interchange between microservices.").
		Example("Parse MessagePack data from hex-encoded content",
			`root = content().decode("hex").parse_msgpack()`,
			[2]string{
				`81a3666f6fa3626172`,
				`{"foo":"bar"}`,
			}).
		Example("Parse MessagePack from base64-encoded field",
			`root.decoded = this.msgpack_data.decode("base64").parse_msgpack()`,
			[2]string{
				`{"msgpack_data":"gaNmb2+jYmFy"}`,
				`{"decoded":{"foo":"bar"}}`,
			})

	if err := bloblang.RegisterMethodV2(
		"parse_msgpack", msgpackParseSpec,
		func(*bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				b, err := bloblang.ValueAsBytes(v)
				if err != nil {
					return nil, err
				}
				var jObj any
				if err := msgpack.Unmarshal(b, &jObj); err != nil {
					return nil, err
				}
				return jObj, nil
			}, nil
		},
	); err != nil {
		panic(err)
	}

	msgpackFormatSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Serializes structured data into MessagePack binary format. MessagePack is a compact binary serialization that is faster and more space-efficient than JSON, making it ideal for network transmission and storage of structured data. Returns a byte array that can be further encoded as needed.").
		Example("Serialize object to MessagePack and encode as hex for transmission",
			`root = this.format_msgpack().encode("hex")`,
			[2]string{
				`{"foo":"bar"}`,
				`81a3666f6fa3626172`,
			}).
		Example("Serialize data to MessagePack and base64 encode for embedding in JSON",
			`root.msgpack_payload = this.data.format_msgpack().encode("base64")`,
			[2]string{
				`{"data":{"foo":"bar"}}`,
				`{"msgpack_payload":"gaNmb2+jYmFy"}`,
			})

	if err := bloblang.RegisterMethodV2(
		"format_msgpack", msgpackFormatSpec,
		func(*bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				return msgpack.Marshal(v)
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
