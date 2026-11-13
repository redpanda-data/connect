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

package plugins

import (
	"bytes"
	"encoding/csv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInfoCSV(t *testing.T) {
	// This test parses the base csv and checks for any malformed fields.
	for k, v := range BaseInfo {
		assert.NotEmpty(t, v.Type, "plugin %v type field", k)
		assert.NotEmpty(t, v.Support, "plugin %v support field", k)
	}
}

func TestInfoCSVUseCases(t *testing.T) {
	// BaseInfo is loaded at init, which panics on an unknown use case, so this
	// only guards the IDs against a change to the parser.
	for k, v := range BaseInfo {
		for _, id := range v.UseCases {
			assert.Contains(t, UseCaseIDs, id, "plugin %v use_cases field", k)
		}
	}
}

func TestParseUseCases(t *testing.T) {
	ids, err := parseUseCases("")
	require.NoError(t, err)
	assert.Empty(t, ids)

	ids, err = parseUseCases("change-data-capture;data-lakehouse")
	require.NoError(t, err)
	assert.Equal(t, []string{"change-data-capture", "data-lakehouse"}, ids)

	for _, bad := range []string{"cdc", "change-data-capture; data-lakehouse", "change-data-capture,data-lakehouse", "change-data-capture;", "stream-processing;stream-processing"} {
		_, err := parseUseCases(bad)
		assert.Error(t, err, "use_cases value %q", bad)
	}
}

func TestPluginInfoFromMapRejectsUnknownUseCase(t *testing.T) {
	_, err := pluginInfoFromMap(map[string]string{"name": "foo", "type": "input", "use_cases": "not-a-use-case"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "foo-input")
	assert.Contains(t, err.Error(), `"not-a-use-case"`)
}

func TestPluginInfoFromMapRejectsUseCasesOnDeprecated(t *testing.T) {
	_, err := pluginInfoFromMap(map[string]string{"name": "foo", "type": "input", "deprecated": "y", "use_cases": "change-data-capture"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "foo-input")
	assert.Contains(t, err.Error(), "deprecated")

	info, err := pluginInfoFromMap(map[string]string{"name": "foo", "type": "input", "deprecated": "y"})
	require.NoError(t, err)
	assert.True(t, info.Deprecated)
	assert.Empty(t, info.UseCases)
}

func TestInfoCSVDeprecatedHaveNoUseCases(t *testing.T) {
	for k, v := range BaseInfo {
		if v.Deprecated {
			assert.Empty(t, v.UseCases, "plugin %v is deprecated", k)
		}
	}
}

func TestFormatCSVRoundTripsUseCases(t *testing.T) {
	in := InfoCollection{
		"foo-input":  {Name: "foo", Type: TypeInput, CommercialName: "foo", Support: "community", UseCases: []string{"change-data-capture", "data-lakehouse"}},
		"bar-output": {Name: "bar", Type: TypeOutput, CommercialName: "bar", Support: "community"},
	}
	out, err := in.FormatCSV()
	require.NoError(t, err)

	records, err := csv.NewReader(bytes.NewReader(out)).ReadAll()
	require.NoError(t, err)
	header := records[0]
	assert.Equal(t, "use_cases", strings.TrimSpace(header[len(header)-1]), "use_cases is the last column")

	got := InfoCollection{}
	for _, r := range records[1:] {
		m := map[string]string{}
		for i, v := range r {
			m[strings.TrimSpace(header[i])] = strings.TrimSpace(v)
		}
		info, err := pluginInfoFromMap(m)
		require.NoError(t, err)
		got[info.key()] = info
	}
	assert.Equal(t, in, got)
}
