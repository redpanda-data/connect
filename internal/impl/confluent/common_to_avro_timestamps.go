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

package confluent

import (
	"fmt"
	"time"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

// convertTimestampFields walks the JSON map data and replaces any ISO 8601
// timestamp strings at the field paths described by s with their
// epoch-millisecond int64 equivalents. Values that are already numeric or
// nil/null are left untouched.
//
// This is necessary because goavro's NativeFromTextual expects a numeric value
// for the Avro timestamp-millis logical type, but CDC sources emit time.Time
// values that JSON-marshal to ISO 8601 strings.
func convertTimestampFields(data map[string]any, s schema.Common) error {
	for _, child := range s.Children {
		if err := convertTimestampChild(data, child); err != nil {
			return err
		}
	}
	return nil
}

// convertTimestampChild converts a single schema child field within data.
func convertTimestampChild(data map[string]any, s schema.Common) error {
	val, exists := data[s.Name]
	if !exists {
		return nil
	}

	switch s.Type {
	case schema.Timestamp:
		if val == nil {
			return nil
		}
		str, ok := val.(string)
		if !ok {
			// Already numeric or some other type; leave it as-is.
			return nil
		}
		t, err := time.Parse(time.RFC3339Nano, str)
		if err != nil {
			return fmt.Errorf("parsing timestamp field %q: %w", s.Name, err)
		}
		data[s.Name] = t.UnixMilli()

	case schema.Object:
		switch v := val.(type) {
		case nil:
			// null union value — skip.
		case map[string]any:
			for _, child := range s.Children {
				if err := convertTimestampChild(v, child); err != nil {
					return err
				}
			}
		}

	case schema.Array:
		switch v := val.(type) {
		case nil:
			// null — skip.
		case []any:
			for i, elem := range v {
				m, ok := elem.(map[string]any)
				if !ok {
					continue
				}
				for _, child := range s.Children {
					if err := convertTimestampChild(m, child); err != nil {
						return fmt.Errorf("array index %d: %w", i, err)
					}
				}
			}
		}
	}

	return nil
}
