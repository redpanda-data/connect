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

package sql

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func init() {
	vectorSpec := bloblangv2.NewPluginSpec().
		Category("SQL").
		Description(`Converts an array of numbers into a vector type suitable for insertion into SQL databases with vector/embedding support. This is commonly used with PostgreSQL's pgvector extension for storing and querying machine learning embeddings, enabling similarity search and vector operations in your database.`).
		Version("4.33.0")

	bloblangv2.MustRegisterMethod(
		"vector", vectorSpec,
		func(*bloblangv2.ParsedParams) (bloblangv2.Method, error) {
			return bloblangv2.ArrayMethod(func(a []any) (any, error) {
				vec := make([]float32, len(a))
				for i, e := range a {
					f, err := valueAsFloat32(e)
					if err != nil {
						return nil, fmt.Errorf("could not convert value at index %d to float32: %w", i, err)
					}
					vec[i] = f
				}
				return vector{vec}, nil
			}), nil
		},
	)
}

// valueAsFloat32 mirrors V1's bloblang.ValueAsFloat32 for the V2 port: V1
// admitted any numeric type, while V2's typed wrappers do not include a
// strict-or-coerced helper at float32 width. Coercion is per-element so the
// behaviour is identical to the V1 plugin's implementation.
func valueAsFloat32(v any) (float32, error) {
	switch n := v.(type) {
	case float32:
		return n, nil
	case float64:
		return float32(n), nil
	case int:
		return float32(n), nil
	case int32:
		return float32(n), nil
	case int64:
		return float32(n), nil
	case uint32:
		return float32(n), nil
	case uint64:
		return float32(n), nil
	}
	return 0, fmt.Errorf("expected number, got %T", v)
}
