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

	"github.com/qdrant/go-client/qdrant"
	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

// newPointID converts an ID of any type to a pb.PointId, returning an error if the type is invalid.
func newPointID(id any) (*qdrant.PointId, error) {
	switch v := id.(type) {
	case string:
		return qdrant.NewID(v), nil
	default:
		n, err := bloblang.ValueAsInt64(id)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, fmt.Errorf("ID cannot be a negative integer ID: %d", v)
		}
		return qdrant.NewIDNum(uint64(n)), nil
	}
}
