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

package qdrant

import (
	"fmt"

	pb "github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// newVectors converts the input into the appropriate *pb.Vectors format
func newVectors(input any) (*pb.Vectors, error) {
	namedVectors := make(map[string]*pb.Vector)

	switch vec := input.(type) {
	case []any:
		// If value is a list of floats or a list of lists of floats
		// root = [0.352,0.532,0.532,0.234]
		// root = [[0.352,0.532,0.532,0.234],[0.352,0.532,0.532,0.234]]
		// Dense vector: https://qdrant.tech/documentation/concepts/vectors/#dense-vectors
		// Multi-vector: https://qdrant.tech/documentation/concepts/vectors/#multivectors

		vector, err := handleDenseOrMultiVector(vec)
		if err != nil {
			return nil, err
		}

		// If a collection is created with the default, unnamed vector
		// https://qdrant.tech/documentation/concepts/collections/#create-a-collection
		// We can use an empty string as the name
		namedVectors[""] = vector

	case map[string]any:
		// If value is a map of vectors
		// root = {"vector_name":[0.352,0.532,0.532,0.234],"another_vector":{"indices":[23,325,532],"values":[0.352,0.532,0.532]}}
		// Multiple named vectors: https://qdrant.tech/documentation/concepts/collections/#collection-with-multiple-vectors
		for name, value := range vec {
			switch valueTyped := value.(type) {
			case []any:
				// "vector_name": [0.352,0.532,0.532,0.234]
				// "another_vector": [[0.352,0.532,0.532,0.234],[0.32,0.532,0.532,0.897]]
				// Dense vector: https://qdrant.tech/documentation/concepts/vectors/#dense-vectors
				// Multi-vector: https://qdrant.tech/documentation/concepts/vectors/#multivectors
				vector, err := handleDenseOrMultiVector(valueTyped)
				if err != nil {
					return nil, err
				}
				namedVectors[name] = vector

			case map[string]any:
				// Case 2.2:
				// "sparse_vector_name": {"indices":[23,325,532],"values":[0.352,0.532,0.532]}
				// Sparse vector: https://qdrant.tech/documentation/concepts/vectors/#sparse-vectors
				vector, err := handleSparseVector(valueTyped)
				if err != nil {
					return nil, err
				}
				namedVectors[name] = vector
			default:
				return nil, fmt.Errorf("unsupported value type for vector key %s: %T", name, value)
			}
		}

	default:
		return nil, fmt.Errorf("unsupported vector input type: %T", input)
	}

	return &pb.Vectors{
		VectorsOptions: &pb.Vectors_Vectors{
			Vectors: &pb.NamedVectors{
				Vectors: namedVectors,
			},
		},
	}, nil
}

// Handle dense and multi-vectors
func handleDenseOrMultiVector(input []any) (*pb.Vector, error) {
	var vector *pb.Vector
	var err error

	_, isMultiVector := input[0].([]any)
	if isMultiVector {
		// If value is a list of lists of floats
		vector, err = convertToMultiVector(input)
		if err != nil {
			return nil, err
		}
	} else {
		// If value is a list of floats
		vector, err = convertToDenseVector(input)
		if err != nil {
			return nil, err
		}
	}
	return vector, nil
}

// Convert a []any containing a dense vector to a *pb.Vector
func convertToDenseVector(input []any) (*pb.Vector, error) {
	data, err := convertToFloat32Slice(input)
	if err != nil {
		return nil, err
	}
	return &pb.Vector{
		Data: data,
	}, nil
}

// Convert a [][]any containing a multi-vector to a *pb.Vector
func convertToMultiVector(input []any) (*pb.Vector, error) {
	vectorsCount := uint32(len(input))

	// Convert the []any to [][]any
	inputTyped := make([][]any, len(input))
	for i, vec := range input {
		inputTyped[i] = vec.([]any)
	}

	// Flatten the input into a single slice
	flattenedInput := make([]any, 0, len(inputTyped)*len(inputTyped[0]))
	for _, vec := range inputTyped {
		flattenedInput = append(flattenedInput, vec...)
	}

	data, err := convertToFloat32Slice(flattenedInput)
	if err != nil {
		return nil, err
	}
	return &pb.Vector{
		Data:         data,
		VectorsCount: &vectorsCount,
	}, nil
}

// Convert a map[string]any containing a sparse vector to a *pb.Vector
func handleSparseVector(input map[string]any) (*pb.Vector, error) {
	var (
		indices []uint32
		data    []float32
		err     error
	)

	if idx, ok := input["indices"].([]any); ok {
		indices, err = convertToUint32Slice(idx)
		if err != nil {
			return nil, fmt.Errorf("failed to convert indices: %w", err)
		}
	}

	if vals, ok := input["values"].([]any); ok {
		data, err = convertToFloat32Slice(vals)
		if err != nil {
			return nil, fmt.Errorf("failed to convert values: %w", err)
		}
	}

	return &pb.Vector{
		Data: data,
		Indices: &pb.SparseIndices{
			Data: indices,
		},
	}, nil
}

// Convert a []any slice to a []float32 slice
func convertToFloat32Slice(input []any) ([]float32, error) {
	values := make([]float32, len(input))
	for i, v := range input {
		val, err := bloblang.ValueAsFloat32(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value to float32 at index %d: %w", i, err)
		}
		values[i] = val
	}
	return values, nil
}

// Convert a []any slice to a []uint32 slice
func convertToUint32Slice(input []any) ([]uint32, error) {
	values := make([]uint32, len(input))
	for i, v := range input {
		val, err := bloblang.ValueAsInt64(v)
		if err != nil {
			return nil, fmt.Errorf("failed to convert value to int64 at index %d: %w", i, err)
		}
		values[i] = uint32(val)
	}
	return values, nil
}
