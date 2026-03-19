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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/schema"
)

func TestConvertTimestampFields(t *testing.T) {
	t.Run("simple timestamp", func(t *testing.T) {
		ts := "2026-03-19T10:05:09.934345Z"
		expected, err := time.Parse(time.RFC3339Nano, ts)
		require.NoError(t, err)

		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "created_at", Type: schema.Timestamp},
				{Name: "name", Type: schema.String},
				{Name: "id", Type: schema.Int32},
			},
		}
		data := map[string]any{
			"created_at": ts,
			"name":       "widget",
			"id":         42,
		}

		require.NoError(t, convertTimestampFields(data, s))
		assert.Equal(t, expected.UnixMilli(), data["created_at"])
		assert.Equal(t, "widget", data["name"])
		assert.Equal(t, 42, data["id"])
	})

	t.Run("null timestamp", func(t *testing.T) {
		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "updated_at", Type: schema.Timestamp, Optional: true},
			},
		}
		data := map[string]any{"updated_at": nil}

		require.NoError(t, convertTimestampFields(data, s))
		assert.Nil(t, data["updated_at"])
	})

	t.Run("already numeric", func(t *testing.T) {
		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "created_at", Type: schema.Timestamp},
			},
		}
		data := map[string]any{"created_at": float64(1742378709934)}

		require.NoError(t, convertTimestampFields(data, s))
		assert.Equal(t, float64(1742378709934), data["created_at"])
	})

	t.Run("missing field", func(t *testing.T) {
		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "created_at", Type: schema.Timestamp},
			},
		}
		data := map[string]any{"name": "widget"}

		require.NoError(t, convertTimestampFields(data, s))
		_, exists := data["created_at"]
		assert.False(t, exists)
	})

	t.Run("invalid timestamp string", func(t *testing.T) {
		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "created_at", Type: schema.Timestamp},
			},
		}
		data := map[string]any{"created_at": "not-a-timestamp"}

		err := convertTimestampFields(data, s)
		assert.ErrorContains(t, err, `parsing timestamp field "created_at"`)
	})

	t.Run("nested object with timestamp", func(t *testing.T) {
		ts := "2026-03-19T10:05:09Z"
		expected, err := time.Parse(time.RFC3339Nano, ts)
		require.NoError(t, err)

		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{
					Name: "metadata",
					Type: schema.Object,
					Children: []schema.Common{
						{Name: "created_at", Type: schema.Timestamp},
					},
				},
			},
		}
		data := map[string]any{
			"metadata": map[string]any{"created_at": ts},
		}

		require.NoError(t, convertTimestampFields(data, s))
		nested := data["metadata"].(map[string]any)
		assert.Equal(t, expected.UnixMilli(), nested["created_at"])
	})

	t.Run("null nested object", func(t *testing.T) {
		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{
					Name:     "metadata",
					Type:     schema.Object,
					Optional: true,
					Children: []schema.Common{
						{Name: "created_at", Type: schema.Timestamp},
					},
				},
			},
		}
		data := map[string]any{"metadata": nil}

		require.NoError(t, convertTimestampFields(data, s))
		assert.Nil(t, data["metadata"])
	})

	t.Run("multiple timestamps CDC scenario", func(t *testing.T) {
		tsCreated := "2026-03-19T10:05:09.934345Z"
		tsUpdated := "2026-03-19T12:00:00Z"
		expCreated, err := time.Parse(time.RFC3339Nano, tsCreated)
		require.NoError(t, err)
		expUpdated, err := time.Parse(time.RFC3339Nano, tsUpdated)
		require.NoError(t, err)

		s := schema.Common{
			Type: schema.Object,
			Children: []schema.Common{
				{Name: "id", Type: schema.Int32},
				{Name: "name", Type: schema.String},
				{Name: "price", Type: schema.String},
				{Name: "in_stock", Type: schema.Boolean},
				{Name: "created_at", Type: schema.Timestamp},
				{Name: "updated_at", Type: schema.Timestamp},
			},
		}
		data := map[string]any{
			"id":         float64(79),
			"name":       "budget gadget",
			"price":      "79.06",
			"in_stock":   true,
			"created_at": tsCreated,
			"updated_at": tsUpdated,
		}

		require.NoError(t, convertTimestampFields(data, s))
		assert.Equal(t, expCreated.UnixMilli(), data["created_at"])
		assert.Equal(t, expUpdated.UnixMilli(), data["updated_at"])
		assert.Equal(t, float64(79), data["id"])
		assert.Equal(t, "budget gadget", data["name"])
	})
}
