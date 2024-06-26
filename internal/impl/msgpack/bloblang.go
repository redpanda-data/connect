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
		Description("Parses a https://msgpack.org/[MessagePack^] message into a structured document.").
		Example("",
			`root = content().decode("hex").parse_msgpack()`,
			[2]string{
				`81a3666f6fa3626172`,
				`{"foo":"bar"}`,
			}).
		Example("",
			`root = this.encoded.decode("base64").parse_msgpack()`,
			[2]string{
				`{"encoded":"gaNmb2+jYmFy"}`,
				`{"foo":"bar"}`,
			})

	if err := bloblang.RegisterMethodV2(
		"parse_msgpack", msgpackParseSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
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
		Description("Formats data as a https://msgpack.org/[MessagePack^] message in bytes format.").
		Example("",
			`root = this.format_msgpack().encode("hex")`,
			[2]string{
				`{"foo":"bar"}`,
				`81a3666f6fa3626172`,
			}).
		Example("",
			`root.encoded = this.format_msgpack().encode("base64")`,
			[2]string{
				`{"foo":"bar"}`,
				`{"encoded":"gaNmb2+jYmFy"}`,
			})

	if err := bloblang.RegisterMethodV2(
		"format_msgpack", msgpackFormatSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				return msgpack.Marshal(v)
			}, nil
		},
	); err != nil {
		panic(err)
	}
}
