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

package influxdb

import "testing"

func Test_encodeInfluxDBName(t *testing.T) {
	type test struct {
		desc      string
		name      string
		tagNames  []string
		tagValues []string
		encoded   string
	}

	tests := []test{
		{"empty name", "", nil, nil, ""},
		{"no tags", "name", nil, nil, "name"},
		{"one tag", "name", []string{"tag"}, []string{"value"}, "name,tag=value"},
		{"escaped", "name, with spaces", []string{"tag ", "t ag2 "}, []string{"value ", "value2"}, `name\,\ with\ spaces,t\ ag2\ =value2,tag\ =value\ `},
		{"bad length tags", "name", []string{"tag", ""}, []string{"value"}, "name"},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			result := encodeInfluxDBName(tt.name, tt.tagNames, tt.tagValues)
			if result != tt.encoded {
				t.Errorf("encoded '%s' but received '%s'", tt.encoded, result)
			}
		})
	}
}

func Test_decodeInfluxDBName(t *testing.T) {
	type test struct {
		desc      string
		name      string
		tagNames  []string
		tagValues []string
		encoded   string
	}
	tests := []test{
		{"empty name", "", nil, nil, ""},
		{"no tags", "name", nil, nil, "name"},
		{"one tag", "name", []string{"tag"}, []string{"value"}, "name,tag=value"},
		{"escaped", "name, with spaces", []string{"tag ", "t ag2 "}, []string{"value ", "value2"}, `name\,\ with\ spaces,t\ ag2\ =value2,tag\ =value\ `},
	}
	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			name, tags := decodeInfluxDBName(tt.encoded)

			if tt.name != name {
				t.Errorf("expected measurement name %s but received %s", tt.name, name)
			}

			if len(tt.tagNames) != len(tags) {
				t.Errorf("expected %d tags", len(tt.tagNames))
			}

			for k, tagName := range tt.tagNames {
				// contains
				if v, ok := tags[tagName]; ok {
					// value is the same
					if tt.tagValues[k] != v {
						t.Errorf("")
					}
				} else {
					t.Errorf("expected to find '%s' in resulting tags", v)
				}
			}
		})
	}
}
