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

package sftp

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pkg/sftp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/pool"
)

// TestDecideWatcherActionPathModeMatchesLegacy checks that the default `path`
// mode makes the same decision as the watcher did before cache_mode existed.
func TestDecideWatcherActionPathModeMatchesLegacy(t *testing.T) {
	t.Parallel()

	modTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// The condition used by the watcher before cache_mode was added.
	legacy := func(val []byte, found, followUpPoll bool) watcherAction {
		if !found || (!followUpPoll && string(val) == "!") {
			return watcherProcess
		}
		return watcherSkip
	}

	tests := []struct {
		name         string
		val          string
		found        bool
		followUpPoll bool
	}{
		{name: "not found first poll", found: false, followUpPoll: false},
		{name: "not found follow-up poll", found: false, followUpPoll: true},
		{name: "pending first poll", val: "!", found: true, followUpPoll: false},
		{name: "pending follow-up poll", val: "!", found: true, followUpPoll: true},
		{name: "done first poll", val: "@", found: true, followUpPoll: false},
		{name: "done follow-up poll", val: "@", found: true, followUpPoll: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := decideWatcherAction([]byte(tt.val), tt.found, tt.followUpPoll, watcherCacheModePath, modTime)
			assert.Equal(t, legacy([]byte(tt.val), tt.found, tt.followUpPoll), got)
		})
	}
}

// TestDecideWatcherActionPathModeModTimeValues covers values written by
// path_and_mod_time mode that path mode encounters after a switch back.
func TestDecideWatcherActionPathModeModTimeValues(t *testing.T) {
	t.Parallel()

	modTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		val          string
		followUpPoll bool
		want         watcherAction
	}{
		{name: "pending with mod time first poll", val: "!2026-01-01T00:00:00Z", followUpPoll: false, want: watcherProcess},
		{name: "pending with mod time follow-up poll", val: "!2026-01-01T00:00:00Z", followUpPoll: true, want: watcherSkip},
		{name: "done with mod time first poll", val: "@2026-01-01T00:00:00Z", followUpPoll: false, want: watcherSkip},
		{name: "done with mod time follow-up poll", val: "@2026-01-01T00:00:00Z", followUpPoll: true, want: watcherSkip},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := decideWatcherAction([]byte(tt.val), true, tt.followUpPoll, watcherCacheModePath, modTime)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWatcherMarkerPathMode(t *testing.T) {
	t.Parallel()

	w := &watcherPathProvider{cacheMode: watcherCacheModePath}
	for _, modTime := range []time.Time{{}, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)} {
		assert.Equal(t, []byte("!"), w.pendingMarker(modTime))
		assert.Equal(t, []byte("@"), w.doneMarker(modTime))
	}
}

func TestParseWatcherCacheMode(t *testing.T) {
	t.Parallel()

	mode, err := parseWatcherCacheMode("path")
	require.NoError(t, err)
	assert.Equal(t, watcherCacheModePath, mode)

	mode, err = parseWatcherCacheMode("path_and_mod_time")
	require.NoError(t, err)
	assert.Equal(t, watcherCacheModePathAndModTime, mode)

	_, err = parseWatcherCacheMode("nope")
	assert.Error(t, err)
}

func TestDecideWatcherActionPathAndModTime(t *testing.T) {
	t.Parallel()

	modTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		val  string
		want watcherAction
	}{
		{name: "legacy done marker", val: "@", want: watcherBackfill},
		{name: "same mod time", val: "@2026-01-01T00:00:00Z", want: watcherSkip},
		{name: "different mod time", val: "@2026-02-01T00:00:00Z", want: watcherProcess},
		{name: "unparseable mod time", val: "@garbage", want: watcherProcess},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := decideWatcherAction([]byte(tt.val), true, true, watcherCacheModePathAndModTime, modTime)
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestWatcherMinimumAgeWholeSecondModTime checks that the watcher does not
// queue a file that was changed less than minimum_age ago. SFTP sends
// modification times in whole seconds, so the reported value can be up to one
// second older than the real change.
func TestWatcherMinimumAgeWholeSecondModTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(path, nil, 0o600))

	serverConn, clientConn := net.Pipe()
	server, err := sftp.NewServer(serverConn)
	require.NoError(t, err)
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { _ = server.Close() })

	client, err := sftp.NewClientPipe(clientConn, clientConn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	// The real change is 1ms before the start of this second. The server
	// reports the start of the previous second, so the reported age is always
	// about 1s more than the real age.
	changed := time.Now().Truncate(time.Second).Add(-time.Millisecond)
	require.NoError(t, os.Chtimes(path, changed, changed))

	// minimum_age is 500ms more than the real age, and less than the reported
	// age. The file is too young and must not be queued.
	minAge := time.Since(changed) + 500*time.Millisecond

	w := &watcherPathProvider{
		clientPool: pool.NewCapped(1, func(context.Context, int) (*sftp.Client, error) {
			return client, nil
		}),
		mgr:          service.MockResources(service.MockResourcesOptAddCache("files_memory")),
		cacheName:    "files_memory",
		pollInterval: 100 * time.Millisecond,
		minAge:       minAge,
		targetPaths:  []string{filepath.Join(dir, "*.txt")},
		cacheMode:    watcherCacheModePath,
	}
	require.NoError(t, w.findNewPaths(t.Context()))
	require.Less(t, time.Since(changed), minAge, "the poll was too slow for the test to be valid")

	assert.Empty(t, w.expandedPaths)
}
