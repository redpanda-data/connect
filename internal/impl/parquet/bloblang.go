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

package parquet

import (
	"bytes"
	"errors"
	"io"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func init() {
	// Note: The examples are run and tested from within
	// ./internal/bloblang/query/parsed_test.go

	parquetParseSpec := bloblang.NewPluginSpec().
		Category("Parsing").
		Description("Parses Apache Parquet binary data into an array of objects. Parquet is a columnar storage format optimized for analytics, commonly used with big data systems like Apache Spark, Hive, and cloud data warehouses. Each row in the Parquet file becomes an object in the output array.").
		Param(bloblang.NewBoolParam("byte_array_as_string").
			Description("Deprecated: This parameter is no longer used.").Default(false)).
		ExampleNotTested("Parse Parquet file data into structured objects",
			`root.records = content().parse_parquet()`).
		ExampleNotTested("Process Parquet data from a field and extract specific columns",
			`root.users = this.parquet_data.parse_parquet().map_each(row -> {"name": row.name, "email": row.email})`)

	if err := bloblang.RegisterMethodV2(
		"parse_parquet", parquetParseSpec,
		func(*bloblang.ParsedParams) (bloblang.Method, error) {
			return func(v any) (any, error) {
				b, err := bloblang.ValueAsBytes(v)
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
	); err != nil {
		panic(err)
	}
}
