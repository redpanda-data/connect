// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package dynamodb

import (
	"sync"
	"testing"
	"time"

	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func itemS(v string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{"pk": &dynamodbtypes.AttributeValueMemberS{Value: v}}
}

func always(time.Time) bool { return true }

func collect(t *testing.T, w *incrementalWindow) []string {
	t.Helper()
	var got []string
	require.NoError(t, w.ReleaseReady(always, func(p releasedPage) error {
		for _, it := range p.Items {
			got = append(got, it["pk"].(*dynamodbtypes.AttributeValueMemberS).Value)
		}
		return nil
	}))
	return got
}

func TestIncrementalWindowTouchBeforeHoldDrops(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	assert.Equal(t, 0, w.Touch("a"), "nothing held yet, recorded as touched")
	_, dropped := w.Hold(0, DynamoItems{itemS("a"), itemS("b")}, []string{"a", "b"}, t0, nil)
	assert.Equal(t, 1, dropped)
	assert.Equal(t, []string{"b"}, collect(t, w))
}

func TestIncrementalWindowTouchWhileHeldDrops(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	w.Hold(0, DynamoItems{itemS("a"), itemS("b")}, []string{"a", "b"}, t0, nil)
	assert.Equal(t, 1, w.Touch("b"))
	assert.Equal(t, 1, w.HeldItems())
	assert.Equal(t, []string{"a"}, collect(t, w))
}

func TestIncrementalWindowTouchAfterReleaseIsNoop(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	released, _ := w.Hold(0, DynamoItems{itemS("a")}, []string{"a"}, t0, nil)
	assert.Equal(t, []string{"a"}, collect(t, w))
	<-released
	assert.Equal(t, 0, w.Touch("a"))
}

func TestIncrementalWindowTouchHitsEveryOpenPage(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	w.Begin(1)
	w.Hold(0, DynamoItems{itemS("a")}, []string{"a"}, t0, nil)
	assert.Equal(t, 1, w.Touch("a"))
	_, dropped := w.Hold(1, DynamoItems{itemS("a")}, []string{"a"}, t0, nil)
	assert.Equal(t, 1, dropped, "segment 1 was open when a was touched")
}

func TestIncrementalWindowReleaseRespectsPredicate(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	w.Begin(1)
	w.Hold(0, DynamoItems{itemS("a")}, []string{"a"}, t0, nil)
	w.Hold(1, DynamoItems{itemS("b")}, []string{"b"}, t0.Add(time.Minute), nil)
	var got []int
	require.NoError(t, w.ReleaseReady(func(h time.Time) bool { return !h.After(t0) }, func(p releasedPage) error {
		got = append(got, p.Segment)
		return nil
	}))
	assert.Equal(t, []int{0}, got)
	assert.Equal(t, 1, w.HeldItems())
}

func TestIncrementalWindowAbortAndBeginReplaceUnheldPage(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	w.Touch("a")
	w.Begin(0) // retry after a failed request: the old open page is discarded
	_, dropped := w.Hold(0, DynamoItems{itemS("a")}, []string{"a"}, t0, nil)
	assert.Equal(t, 0, dropped, "the touch belonged to the discarded page")
	w.Begin(1)
	w.Abort(1)
	assert.Equal(t, []string{"a"}, collect(t, w))
}

func TestIncrementalWindowHeldPageStopsRecordingTouches(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	w.Hold(0, DynamoItems{itemS("a"), itemS("b")}, []string{"a", "b"}, t0, nil)
	w.Touch("c") // touch a non-held key
	// Verify that touched is nil on held page (read w.pages[0] directly)
	w.mu.Lock()
	p := w.pages[0]
	w.mu.Unlock()
	assert.Nil(t, p.touched, "held page should not record touches")
	assert.Equal(t, 2, w.HeldItems())
	// Verify Touch still drops held items
	assert.Equal(t, 1, w.Touch("a"))
	assert.Equal(t, 1, w.HeldItems())
}

// TestIncrementalWindowNoStaleAfterNewer races a stream reader against the
// release loop. Invariant: once the reader has sent key k's event, no
// snapshot item for k may be sent afterwards.
func TestIncrementalWindowNoStaleAfterNewer(t *testing.T) {
	for range 200 {
		w := newIncrementalWindow()
		var (
			mu       sync.Mutex
			sentCDC  = map[string]bool{}
			violated bool
		)
		keys := []string{"a", "b", "c", "d"}
		items := DynamoItems{itemS("a"), itemS("b"), itemS("c"), itemS("d")}
		w.Begin(0)
		w.Hold(0, items, keys, t0, nil)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			for _, k := range keys {
				w.Touch(k)
				mu.Lock()
				sentCDC[k] = true
				mu.Unlock()
			}
		}()
		go func() {
			defer wg.Done()
			_ = w.ReleaseReady(always, func(p releasedPage) error {
				mu.Lock()
				defer mu.Unlock()
				for _, it := range p.Items {
					if sentCDC[it["pk"].(*dynamodbtypes.AttributeValueMemberS).Value] {
						violated = true
					}
				}
				return nil
			})
		}()
		wg.Wait()
		require.False(t, violated, "a snapshot item was sent after its key's stream event")
	}
}

func TestIncrementalWindowResetDropsEveryPage(t *testing.T) {
	w := newIncrementalWindow()
	w.Begin(0)
	released, _ := w.Hold(0, DynamoItems{itemS("a")}, []string{"a"}, t0, nil)
	w.Begin(1) // open, never held
	w.Reset()
	select {
	case <-released:
	default:
		t.Fatal("held page's released channel must close on Reset")
	}
	assert.Equal(t, 0, w.HeldItems())
	assert.NotPanics(t, func() { w.Begin(0) }, "segment reusable after Reset")
	assert.Empty(t, collect(t, w))
}
