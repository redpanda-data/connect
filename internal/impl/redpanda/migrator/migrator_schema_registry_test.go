// Copyright 2025 Redpanda Data, Inc.
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

package migrator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestParseVersions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected Versions
		wantErr  bool
	}{
		{
			name:     "valid latest version",
			input:    "latest",
			expected: VersionsLatest,
			wantErr:  false,
		},
		{
			name:     "valid all versions",
			input:    "all",
			expected: VersionsAll,
			wantErr:  false,
		},
		{
			name:     "invalid versions",
			input:    "invalid_versions",
			expected: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseVersions(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestVersionsString(t *testing.T) {
	assert.Equal(t, "latest", VersionsLatest.String())
	assert.Equal(t, "all", VersionsAll.String())
}

func TestSchemaEquals(t *testing.T) {
	tests := []struct {
		name string
		a    sr.Schema
		b    sr.Schema
		eq   bool
	}{
		{
			name: "equal when schema differs only by whitespace and newlines",
			a:    sr.Schema{Schema: "{\n  \"type\": \"string\"\n}\n"},
			b:    sr.Schema{Schema: "{\"type\":\"string\"}"},
			eq:   true,
		},
		{
			name: "not equal when schema text differs materially",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}"},
			b:    sr.Schema{Schema: "{\"type\":\"int\"}"},
			eq:   false,
		},
		{
			name: "not equal when other fields differ (Type)",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}", Type: sr.TypeJSON},
			b:    sr.Schema{Schema: "{\n\t\"type\": \"string\"\n}", Type: sr.TypeAvro},
			eq:   false,
		},
		{
			name: "not equal when references differ",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}", References: []sr.SchemaReference{{Name: "A", Subject: "s", Version: 1}}},
			b:    sr.Schema{Schema: "{\n\t\"type\": \"string\"\n}", References: []sr.SchemaReference{{Name: "B", Subject: "s", Version: 1}}},
			eq:   false,
		},
		{
			name: "equal when schema and all other fields equal",
			a:    sr.Schema{Schema: "{\"type\":\"string\"}", Type: sr.TypeAvro},
			b:    sr.Schema{Schema: "\n{\n  \"type\": \"string\"\n}\n", Type: sr.TypeAvro},
			eq:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.eq, schemaEquals(tt.a, tt.b))
		})
	}
}

func TestIsSubjectError(t *testing.T) {
	respErr := func(code int) error {
		return fmt.Errorf("create schema: %w", &sr.ResponseError{StatusCode: code})
	}
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"incompatible", respErr(http.StatusConflict), true},
		{"invalid schema", respErr(http.StatusUnprocessableEntity), true},
		{"not found", respErr(http.StatusNotFound), true},
		{"fixed ID collision", &fixedIDError{respErr(http.StatusConflict)}, false},
		{"unauthorized", respErr(http.StatusUnauthorized), false},
		{"forbidden", respErr(http.StatusForbidden), false},
		{"server error", respErr(http.StatusInternalServerError), false},
		{"unavailable", respErr(http.StatusServiceUnavailable), false},
		{"network", errors.New("dial tcp: connection refused"), false},
		{"canceled", context.Canceled, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isSubjectError(tc.err))
		})
	}
}

// TestDestinationSchemaIDFailedThenSynced checks that a lookup never observes a
// schema in neither state while a sync registers it. A schema that failed to
// sync is rejected, and once it is registered its ID is translated. If a lookup
// missed both states, it would return the source ID untranslated with strict
// disabled.
//
// One goroutine flips the schema between failed and registered, as a sync
// worker does, and the others call DestinationSchemaID in a loop. A call that
// returns the source ID without an error can only come from such a window.
func TestDestinationSchemaIDFailedThenSynced(t *testing.T) {
	const (
		srcID = 7
		dstID = 42
		flips = 200_000
	)

	// enabled() needs a destination client. No request is made.
	dst, err := sr.NewClient(sr.URLs("http://127.0.0.1:1"))
	require.NoError(t, err)

	failed := schemaState{err: errors.New("rejected")}
	m := &schemaRegistryMigrator{
		conf:          SchemaRegistryMigratorConfig{Enabled: true, TranslateIDs: true},
		dst:           dst,
		knownSubjects: make(map[schemaSubjectVersion]struct{}),
		schemas:       map[int]schemaState{srcID: failed},
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				id, err := m.DestinationSchemaID(srcID)
				if err == nil {
					assert.Equal(t, dstID, id, "source ID returned untranslated")
				}
			}
		})
	}

	for range flips {
		// The sync worker registers the schema.
		m.setSchemaSynced(sr.SubjectSchema{ID: srcID}, schemaInfo{ID: dstID})

		// Reset for the next round.
		m.mu.Lock()
		m.schemas[srcID] = failed
		m.mu.Unlock()
	}
	close(done)
	wg.Wait()
}
