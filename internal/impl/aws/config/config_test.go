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

package config

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// sessionFieldVersions registers the fields in a throwaway input and reads
// each top-level field's name and version back from the schema.
func sessionFieldVersions(t *testing.T, fields []*service.ConfigField) map[string]string {
	t.Helper()
	env := service.NewEmptyEnvironment()
	require.NoError(t, env.RegisterInput("session_test", service.NewConfigSpec().Fields(fields...),
		func(*service.ParsedConfig, *service.Resources) (service.Input, error) { return nil, context.Canceled }))
	raw, err := env.FullConfigSchema("", "").MarshalJSONV0()
	require.NoError(t, err)

	var schema struct {
		Inputs []struct {
			Config struct {
				Children []struct {
					Name    string `json:"name"`
					Version string `json:"version"`
				} `json:"children"`
			} `json:"config"`
		} `json:"inputs"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))
	require.Len(t, schema.Inputs, 1)

	got := map[string]string{}
	var names []string
	for _, c := range schema.Inputs[0].Config.Children {
		got[c.Name] = c.Version
		names = append(names, c.Name)
	}
	require.Equal(t, sessionFieldNames, names, "sessionFieldNames must list the session fields in order")
	return got
}

func TestSessionFieldsWithVersions(t *testing.T) {
	assert.Equal(t, map[string]string{"region": "", "endpoint": "", "tcp": "4.69.0", "credentials": ""},
		sessionFieldVersions(t, SessionFields()))

	assert.Equal(t, map[string]string{"region": "", "endpoint": "4.84.0", "tcp": "4.84.0", "credentials": "4.84.0"},
		sessionFieldVersions(t, SessionFieldsWithVersions(map[string]string{
			"endpoint": "4.84.0", "tcp": "4.84.0", "credentials": "4.84.0",
		})))

	assert.PanicsWithValue(t, "unknown aws session field: regoin", func() {
		SessionFieldsWithVersions(map[string]string{"regoin": "4.53.0"})
	})
}
