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

package s3

import (
	"testing"
	"time"
)

func TestSQSInFlightTrackerRefreshLifecycle(t *testing.T) {
	const timeout = 30 * time.Second
	tr := newSQSInFlightTracker(timeout)

	tr.Add("id-1", "rh-1")

	// A freshly added handle has a full timeout before it expires, so it is not
	// yet eligible for refresh.
	if entries := tr.PullToRefresh(time.Now()); len(entries) != 0 {
		t.Fatalf("expected no refresh for a freshly added handle, got %d", len(entries))
	}

	// Once half the timeout has elapsed the handle becomes eligible.
	entries := tr.PullToRefresh(time.Now().Add(timeout/2 + time.Second))
	if len(entries) != 1 {
		t.Fatalf("expected 1 handle to refresh, got %d", len(entries))
	}
	if got := *entries[0].Id; got != "id-1" {
		t.Errorf("unexpected id: %q", got)
	}
	if got := *entries[0].ReceiptHandle; got != "rh-1" {
		t.Errorf("unexpected receipt handle: %q", got)
	}
	if got, want := entries[0].VisibilityTimeout, int32(timeout.Seconds()); got != want {
		t.Errorf("unexpected visibility timeout: got %d want %d", got, want)
	}

	// Refreshing pushes the deadline out again, so an immediate pull is empty.
	if entries := tr.PullToRefresh(time.Now()); len(entries) != 0 {
		t.Fatalf("expected no refresh immediately after a refresh, got %d", len(entries))
	}

	// A removed handle is never refreshed again, even past its deadline.
	tr.Remove("id-1")
	if entries := tr.PullToRefresh(time.Now().Add(timeout * 2)); len(entries) != 0 {
		t.Fatalf("expected no refresh after removal, got %d", len(entries))
	}
}

func TestSQSInFlightTrackerRefreshesAllDueHandles(t *testing.T) {
	const timeout = 10 * time.Second
	tr := newSQSInFlightTracker(timeout)

	tr.Add("id-1", "rh-1")
	tr.Add("id-2", "rh-2")
	tr.Add("id-3", "rh-3")
	tr.Remove("id-2")

	entries := tr.PullToRefresh(time.Now().Add(timeout))
	if len(entries) != 2 {
		t.Fatalf("expected 2 handles to refresh, got %d", len(entries))
	}

	seen := map[string]string{}
	for _, e := range entries {
		seen[*e.Id] = *e.ReceiptHandle
	}
	if seen["id-1"] != "rh-1" || seen["id-3"] != "rh-3" {
		t.Errorf("unexpected refreshed handles: %v", seen)
	}
	if _, ok := seen["id-2"]; ok {
		t.Errorf("removed handle id-2 should not be refreshed")
	}
}
