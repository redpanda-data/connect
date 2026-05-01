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

package xml

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblangv2"
)

func TestParseXMLV2(t *testing.T) {
	testCases := []struct {
		name   string
		target string
		args   string
		exp    any
	}{
		{
			name:   "simple parsing",
			target: "<root><title>This is a title</title><content>This is some content</content></root>",
			exp:    map[string]any{"root": map[string]any{"content": "This is some content", "title": "This is a title"}},
		},
		{
			name:   "parsing numbers and bools without casting",
			target: `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			exp:    map[string]any{"root": map[string]any{"bool": "True", "number": map[string]any{"#text": "123", "-id": "99"}, "title": "This is a title"}},
		},
		{
			name:   "parsing numbers and bools with casting",
			target: `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			args:   `cast: true`,
			exp:    map[string]any{"root": map[string]any{"bool": true, "number": map[string]any{"#text": float64(123), "-id": float64(99)}, "title": "This is a title"}},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			exec, err := bloblangv2.Parse(fmt.Sprintf(`output = input.parse_xml(%v)`, test.args))
			require.NoError(t, err)

			res, err := exec.Query(test.target)
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
		})
	}
}

func TestFormatXMLV2(t *testing.T) {
	exec, err := bloblangv2.Parse(`output = input.format_xml(no_indent: true)`)
	require.NoError(t, err)

	res, err := exec.Query(map[string]any{"foo": map[string]any{"bar": "baz"}})
	require.NoError(t, err)

	got, ok := res.([]byte)
	require.True(t, ok)
	assert.Equal(t, `<foo><bar>baz</bar></foo>`, string(got))
}
