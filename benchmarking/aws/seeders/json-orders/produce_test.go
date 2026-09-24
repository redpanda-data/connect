// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"encoding/json"
	"math/rand"
	"testing"
)

func TestRowPadLen(t *testing.T) {
	for _, tc := range []struct {
		rowSize int
		want    int
	}{
		{1200, 1200 - fixedOverhead},
		{fixedOverhead, 0},
		{10, 0}, // below fixedOverhead must clamp to zero, not go negative
	} {
		if got := rowPadLen(tc.rowSize); got != tc.want {
			t.Errorf("rowPadLen(%d) = %d, want %d", tc.rowSize, got, tc.want)
		}
	}
}

func TestNewPayloadPool_SizeAndCharset(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	pool := newPayloadPool(rng)
	if len(pool.data) != poolSize {
		t.Fatalf("pool size = %d, want %d", len(pool.data), poolSize)
	}
	for i, b := range pool.data {
		if i > 1000 {
			break // full scan is unnecessary; a prefix check is enough
		}
		found := false
		for _, c := range []byte(charset) {
			if b == c {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("pool byte %d = %q not in charset", i, b)
		}
	}
}

func TestPayloadPool_Sample(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	pool := newPayloadPool(rng)

	if got := pool.sample(rng, 0); got != "" {
		t.Errorf("sample(0) = %q, want empty", got)
	}
	if got := pool.sample(rng, -5); got != "" {
		t.Errorf("sample(-5) = %q, want empty", got)
	}
	// A window larger than the pool itself must also degrade to empty rather
	// than panic on a negative Intn bound.
	if got := pool.sample(rng, poolSize+1); got != "" {
		t.Errorf("sample(poolSize+1) = %q, want empty", got)
	}

	const padLen = 64
	got := pool.sample(rng, padLen)
	if len(got) != padLen {
		t.Fatalf("sample(%d) length = %d, want %d", padLen, len(got), padLen)
	}
	// Consecutive samples must not collide on the same window every time —
	// this is the whole point of the random-offset design (see payloadPool's
	// doc comment): identical padding across records defeats the entropy
	// this pool exists to provide.
	again := pool.sample(rng, padLen)
	if got == again {
		t.Error("two consecutive samples were byte-identical; the offset must vary")
	}
}

func TestBuildRecord_ShapeAndFields(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	pool := newPayloadPool(rng)
	const padLen = 32

	rec := buildRecord(pool, rng, 42, 7, padLen)

	wantKeys := []string{"id", "ts", "region", "amount", "status", "payload"}
	if len(rec) != len(wantKeys) {
		t.Fatalf("record has %d fields, want %d: %#v", len(rec), len(wantKeys), rec)
	}
	for _, k := range wantKeys {
		if _, ok := rec[k]; !ok {
			t.Errorf("record missing field %q: %#v", k, rec)
		}
	}
	if rec["id"] != int64(42) {
		t.Errorf("id = %v, want 42 (id must be independent of varySeed)", rec["id"])
	}
	if got, ok := rec["region"].(string); !ok || got == "" {
		t.Errorf("region = %v, want a non-empty string", rec["region"])
	}
	if got, ok := rec["status"].(string); !ok || got == "" {
		t.Errorf("status = %v, want a non-empty string", rec["status"])
	}
	if got, ok := rec["payload"].(string); !ok || len(got) != padLen {
		t.Errorf("payload = %v, want a %d-byte string", rec["payload"], padLen)
	}

	// The record must be JSON-marshalable (it's produced straight to Kafka
	// as a JSON value).
	if _, err := json.Marshal(rec); err != nil {
		t.Errorf("json.Marshal: %v", err)
	}
}

func TestBuildRecord_VarySeedDrivesLowCardinalityFields(t *testing.T) {
	// A recurring id (bounded key space) must still vary region/status/amount
	// by varySeed, so a recurring id carries a distinct row image each time —
	// the shape an upsert actually sees.
	rng := rand.New(rand.NewSource(1))
	pool := newPayloadPool(rng)

	a := buildRecord(pool, rng, 0, 0, 16)
	b := buildRecord(pool, rng, 0, 1, 16)
	if a["id"] != b["id"] {
		t.Fatalf("ids should match for this test: %v vs %v", a["id"], b["id"])
	}
	if a["region"] == b["region"] && a["status"] == b["status"] && a["amount"] == b["amount"] {
		t.Error("varying varySeed should change at least one low-cardinality field")
	}
}

func TestGCD(t *testing.T) {
	for _, tc := range []struct{ a, b, want int64 }{
		{12, 8, 4},
		{17, 5, 1},
		{100, 0, 100},
	} {
		if got := gcd(tc.a, tc.b); got != tc.want {
			t.Errorf("gcd(%d, %d) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}
