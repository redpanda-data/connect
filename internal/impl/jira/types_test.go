package jira

import "testing"

func TestParseResource(t *testing.T) {
	cases := []struct {
		in      string
		wantErr bool
	}{
		{"issue", false},
		{"issue_transition", false},
		{"role", false},
		{"user", false},
		{"project_version", false},
		{"project", false},
		{"project_category", false},
		{"project_type", false},
		{"", true},
		{"unknown", true},
	}

	for _, c := range cases {
		_, err := ParseResource(c.in)
		if (err != nil) != c.wantErr {
			t.Fatalf("ParseResource(%q) error=%v wantErr=%v", c.in, err, c.wantErr)
		}
	}
}
