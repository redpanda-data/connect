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

package sql

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

type vector struct {
	value []float32
}

func init() {
	vectorSpec := bloblang.NewPluginSpec().
		Beta().
		Category("SQL").
		Description(`Creates a vector from a given array of floating point numbers.

This vector can be inserted into various SQL databases if they have support for embeddings vectors (for example `+"`pgvector`).").
		Version("4.33.0").
		Example("Create a vector from an array literal",
			`root.embeddings = [1.2, 0.6, 0.9].vector()`,
		).
		Example("Create a vector from an array",
			`root.embedding_vector = this.embedding_array.vector()`,
		)

	if err := bloblang.RegisterMethodV2(
		"vector", vectorSpec,
		func(args *bloblang.ParsedParams) (bloblang.Method, error) {
			return bloblang.ArrayMethod(func(a []any) (any, error) {
				vec := make([]float32, len(a))
				for i, e := range a {
					f, err := bloblang.ValueAsFloat32(e)
					if err != nil {
						return nil, fmt.Errorf("could not convert value at index %d to float32: %w", i, err)
					}
					vec[i] = f
				}
				return vector{vec}, nil
			}), nil
		},
	); err != nil {
		panic(err)
	}
}
