// Copyright 2026 Redpanda Data, Inc.
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
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	parseSpec := bloblangv2.NewPluginSpec().
		Category("Parsing").
		Description("Parses MessagePack binary data into a structured object. MessagePack is an efficient binary serialization format that is more compact than JSON while maintaining similar data structures. Commonly used for high-performance APIs and data interchange between microservices.")

	bloblangv2.MustRegisterMethod(
		"parse_msgpack", parseSpec,
		func(*bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			return func(v any) (any, error) {
				b, err := valueAsBytes(v)
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
	)

	formatSpec := bloblangv2.NewPluginSpec().
		Category("Parsing").
		Description("Serializes structured data into MessagePack binary format. MessagePack is a compact binary serialization that is faster and more space-efficient than JSON, making it ideal for network transmission and storage of structured data. Returns a byte array that can be further encoded as needed.")

	bloblangv2.MustRegisterMethod(
		"format_msgpack", formatSpec,
		func(*bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			return func(v any) (any, error) {
				return msgpack.Marshal(v)
			}, nil
		},
	)
}

// valueAsBytes accepts the same set of receiver shapes that the V1 plugin's
// bloblang.ValueAsBytes helper accepted (string or []byte). The V2 BytesMethod
// wrapper is strict and would reject string receivers; callers already pass
// strings into parse_msgpack today so the coercion is preserved here to keep
// the plugin signature backwards compatible across V1 and V2.
func valueAsBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	}
	return nil, fmt.Errorf("expected string or bytes, got %T", v)
}
