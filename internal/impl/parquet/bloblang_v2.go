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

package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	parquetParseSpec := bloblangv2.NewPluginSpec().
		Category("Parsing").
		Description("Parses Apache Parquet binary data into an array of objects. Parquet is a columnar storage format optimized for analytics, commonly used with big data systems like Apache Spark, Hive, and cloud data warehouses. Each row in the Parquet file becomes an object in the output array.").
		Param(bloblangv2.NewBoolParam("byte_array_as_string").
			Description("Deprecated: This parameter is no longer used.").Default(false))

	bloblangv2.MustRegisterMethod(
		"parse_parquet", parquetParseSpec,
		func(*bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			return func(v any) (any, error) {
				b, err := valueAsBytes(v)
				if err != nil {
					return nil, err
				}

				rdr := bytes.NewReader(b)
				pRdr, err := newReaderWithoutPanic(rdr)
				if err != nil {
					return nil, err
				}

				rowBuf := make([]any, 10)
				var result []any

				for {
					n, err := readWithoutPanic(pRdr, rowBuf)
					if err != nil && !errors.Is(err, io.EOF) {
						return nil, err
					}
					if n == 0 {
						break
					}

					for i := range n {
						result = append(result, rowBuf[i])
					}
				}

				return result, nil
			}, nil
		},
	)
}

// valueAsBytes mirrors V1's bloblang.ValueAsBytes coercion (string or []byte
// receivers). V2's BytesMethod is strict and would reject string inputs;
// preserved here so existing callers don't need to add `.bytes()` upstream.
func valueAsBytes(v any) ([]byte, error) {
	switch t := v.(type) {
	case []byte:
		return t, nil
	case string:
		return []byte(t), nil
	}
	return nil, fmt.Errorf("expected string or bytes, got %T", v)
}
