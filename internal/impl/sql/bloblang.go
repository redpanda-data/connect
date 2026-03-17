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
		Description(`Converts an array of numbers into a vector type suitable for insertion into SQL databases with vector/embedding support. This is commonly used with PostgreSQL's pgvector extension for storing and querying machine learning embeddings, enabling similarity search and vector operations in your database.`).
		Version("4.33.0").
		ExampleNotTested("Convert embeddings array to vector for pgvector storage",
			`root.embedding = this.embeddings.vector()
root.text = this.text`).
		ExampleNotTested("Process ML model output into database-ready vector format",
			`root.doc_id = this.id
root.vector_embedding = this.model_output.map_each(num -> num.number()).vector()`)

	if err := bloblang.RegisterMethodV2(
		"vector", vectorSpec,
		func(*bloblang.ParsedParams) (bloblang.Method, error) {
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
