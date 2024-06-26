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

package xml

import (
	"fmt"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
)

func TestParseXML(t *testing.T) {
	testCases := []struct {
		name   string
		target any
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
			args:   `true`,
			exp:    map[string]any{"root": map[string]any{"bool": true, "number": map[string]any{"#text": float64(123), "-id": float64(99)}, "title": "This is a title"}},
		},
	}

	for _, test := range testCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			targetClone, err := gabs.ParseJSON([]byte(gabs.Wrap(test.target).String()))
			require.NoError(t, err)

			exec, err := bloblang.Parse(fmt.Sprintf(`root = this.parse_xml(%v)`, test.args))
			require.NoError(t, err)

			res, err := exec.Query(targetClone.Data())
			require.NoError(t, err)

			assert.Equal(t, test.exp, res)
			assert.Equal(t, test.target, targetClone.Data())
		})
	}
}
