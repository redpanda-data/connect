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

package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderConnectJSON(t *testing.T) {
	raw := []byte(`{
  "version": "4.200.0",
  "date": "2026-10-09T10:38:00Z",
  "config": [{"name": "http"}, {"name": "shutdown_delay"}],
  "inputs": [{"name": "kafka", "description": "Reads <records> & more."}, {"name": "zmq4"}],
  "processors": [{"name": "a2a_message", "config": {"default": 1.50}}, {"name": "branch"}],
  "bloblang-functions": [{"name": "now"}],
  "bloblang-methods": []
}`)
	cloudRaw := []byte(`{"config": [{"name": "http"}]}`)
	plat := platformSet{byKey: map[string]componentPlatform{
		componentKey("inputs", "kafka"):           {Cloud: true},
		componentKey("inputs", "zmq4"):            {CgoOnly: true},
		componentKey("processors", "a2a_message"): {Cloud: true},
		componentKey("processors", "branch"):      {},
	}}
	notInBinary := map[string]bool{
		componentKey("processors", "a2a_message"): true,
		// Not run in Cloud, so not cloud-only even though the binary lacks it.
		componentKey("processors", "branch"): true,
	}

	out, err := renderConnectJSON(raw, cloudRaw, plat, notInBinary)
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &doc))
	assert.Equal(t, "4.200.0", doc["version"])
	assert.Equal(t, "2026-10-09T10:38:00Z", doc["date"])

	byName := func(group string) map[string]map[string]any {
		m := map[string]map[string]any{}
		for _, c := range doc[group].([]any) {
			o := c.(map[string]any)
			m[o["name"].(string)] = o
		}
		return m
	}
	inputs, procs, config := byName("inputs"), byName("processors"), byName("config")

	assert.Equal(t, true, inputs["kafka"]["cloudSupported"])
	assert.Equal(t, false, inputs["kafka"]["requiresCgo"])
	assert.NotContains(t, inputs["kafka"], "cloudOnly")
	assert.Equal(t, false, inputs["zmq4"]["cloudSupported"])
	assert.Equal(t, true, inputs["zmq4"]["requiresCgo"])
	assert.Equal(t, true, procs["a2a_message"]["cloudOnly"])
	assert.NotContains(t, procs["branch"], "cloudOnly")
	assert.Equal(t, true, config["http"]["cloudSupported"])
	assert.Equal(t, false, config["shutdown_delay"]["cloudSupported"])
	assert.Equal(t, false, config["shutdown_delay"]["requiresCgo"])

	// Descriptions keep their characters, and numbers keep their form.
	assert.Contains(t, out, "Reads <records> & more.")
	assert.Contains(t, out, `"default": 1.50`)
	// Groups the schema doesn't have stay out of the file.
	assert.NotContains(t, doc, "outputs")
	assert.True(t, strings.HasSuffix(out, "}\n"))
}

func TestConnectJSONPath(t *testing.T) {
	assert.Equal(t, "attachments/connect-4.113.0.json", connectJSONPath("4.113.0"))
}
