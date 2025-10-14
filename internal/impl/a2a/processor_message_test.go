// Copyright 2025 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package a2a

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseAgentCardURL(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantBaseURL string
		wantPath    string
	}{
		{
			name:         "base URL without path",
			input:        "https://example.com",
			wantBaseURL:  "https://example.com",
			wantPath:     "/.well-known/agent.json",
		},
		{
			name:         "base URL with port without path",
			input:        "https://example.com:8080",
			wantBaseURL:  "https://example.com:8080",
			wantPath:     "/.well-known/agent.json",
		},
		{
			name:         "full URL with .well-known/agent.json",
			input:        "https://example.com/.well-known/agent.json",
			wantBaseURL:  "https://example.com",
			wantPath:     "/.well-known/agent.json",
		},
		{
			name:         "full URL with .well-known/agent-card.json",
			input:        "https://example.com/.well-known/agent-card.json",
			wantBaseURL:  "https://example.com",
			wantPath:     "/.well-known/agent-card.json",
		},
		{
			name:         "full URL with port and .well-known path",
			input:        "https://example.com:8080/.well-known/agent.json",
			wantBaseURL:  "https://example.com:8080",
			wantPath:     "/.well-known/agent.json",
		},
		{
			name:         "URL with path prefix before .well-known",
			input:        "https://example.com/api/v1/.well-known/agent.json",
			wantBaseURL:  "https://example.com/api/v1",
			wantPath:     "/.well-known/agent.json",
		},
		{
			name:         "base URL with trailing slash",
			input:        "https://example.com/",
			wantBaseURL:  "https://example.com/",
			wantPath:     "/.well-known/agent.json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotBaseURL, gotPath := parseAgentCardURL(tt.input)
			assert.Equal(t, tt.wantBaseURL, gotBaseURL, "baseURL mismatch")
			assert.Equal(t, tt.wantPath, gotPath, "path mismatch")
		})
	}
}
