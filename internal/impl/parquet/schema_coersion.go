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
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gofrs/uuid/v5"
	"github.com/parquet-go/parquet-go"
)

type schemaVisitor interface {
	visitLeaf(value any, schemaNode parquet.Node) (any, error)
}

func visitWithSchema(visitor schemaVisitor, value any, schemaNode parquet.Node) (any, error) {
	if schemaNode.Leaf() {
		if schemaNode.Optional() && value == nil {
			return nil, nil
		}
		return visitor.visitLeaf(value, schemaNode)
	}

	switch group := value.(type) {
	case map[string]any:
		for _, childSchemaNode := range schemaNode.Fields() {
			name := childSchemaNode.Name()
			if childValue, ok := group[name]; ok {
				var err error
				group[name], err = visitWithSchema(visitor, childValue, childSchemaNode)
				if err != nil {
					return nil, fmt.Errorf("visiting [%s]: %w", name, err)
				}
			}
		}
		return group, nil

	case []any:
		for i := range group {
			var err error
			group[i], err = visitWithSchema(visitor, group[i], schemaNode)
			if err != nil {
				return nil, fmt.Errorf("visiting [%d]: %w", i, err)
			}
		}
		return group, nil

	case nil:
		return nil, nil

	default:
		panic(fmt.Sprintf("unexpected group value type: %T", value))
	}
}

type encodingCoersionVisitor struct{}

func (encodingCoersionVisitor) visitLeaf(value any, schemaNode parquet.Node) (any, error) {
	logicalType := schemaNode.Type().LogicalType()
	if logicalType == nil {
		return value, nil
	}
	if logicalType.Timestamp != nil {
		switch v := value.(type) {
		case string:
			ts, err := time.Parse(time.RFC3339, v)
			if err != nil {
				return nil, fmt.Errorf("parsing string RFC3339 timestamp: %w", err)
			}
			unit := logicalType.Timestamp.Unit
			if unit.Millis != nil {
				return ts.UnixMilli(), nil
			} else if unit.Micros != nil {
				return ts.UnixMicro(), nil
			} else if unit.Nanos != nil {
				return ts.UnixNano(), nil
			} else {
				panic("unreachable")
			}
		default:
			return nil, errors.New("TIMESTAMP values must be RFC3339-formatted strings")
		}
	} else if logicalType.Json != nil {
		switch value.(type) {
		case map[string]any, []any:
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("encoding value as JSON: %w", err)
			}
			return jsonBytes, nil
		default:
			return value, nil
		}
	} else if logicalType.UUID != nil {
		switch v := value.(type) {
		case string:
			id, err := uuid.FromString(v)
			if err != nil {
				return nil, fmt.Errorf("parsing string as UUID: %w", err)
			}
			return id.Bytes(), nil
		default:
			return value, nil
		}
	}

	return value, nil
}

type decodingCoersionVisitor struct {
	version int
}

func (d *decodingCoersionVisitor) visitLeaf(value any, schemaNode parquet.Node) (any, error) {
	logicalType := schemaNode.Type().LogicalType()
	if logicalType == nil {
		return value, nil
	}

	if d.version >= 1 {
		if logicalType.Timestamp != nil {
			tsNum, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("decoding timestamp but physical type is not an integer: %T", value)
			}

			schemaSpec := logicalType.Timestamp
			var ts time.Time
			if schemaSpec.Unit.Millis != nil {
				ts = time.UnixMilli(tsNum)
			} else if schemaSpec.Unit.Micros != nil {
				ts = time.UnixMicro(tsNum)
			} else if schemaSpec.Unit.Nanos != nil {
				ts = time.Unix(tsNum/1e9, tsNum%1e9)
			} else {
				panic("unreachable")
			}
			if schemaSpec.IsAdjustedToUTC {
				return ts.UTC(), nil
			} else {
				return ts.Local(), nil
			}
		} else if logicalType.UUID != nil {
			uuidBytes, ok := value.([]byte)
			if !ok {
				return nil, fmt.Errorf("decoding UUID, physical type is not []byte: %T", value)
			}
			id, err := uuid.FromBytes(uuidBytes)
			if err != nil {
				return nil, fmt.Errorf("parsing value as UUID: %w", err)
			}
			return id.String(), nil
		}
	}

	return value, nil
}
