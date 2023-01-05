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
