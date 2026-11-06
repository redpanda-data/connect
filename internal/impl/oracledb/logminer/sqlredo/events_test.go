// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package sqlredo

import (
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionIDRawXIDRoundTripFromBytes(t *testing.T) {
	realXID, err := hex.DecodeString("04001700D0400200")
	require.NoError(t, err)

	raws := [][]byte{
		realXID,
		make([]byte, 8),
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		{0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00},
		{0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00},
		{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80},
		{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
	}
	rng := rand.New(rand.NewSource(42))
	for range 200 {
		b := make([]byte, 8)
		_, _ = rng.Read(b)
		raws = append(raws, b)
	}

	for _, raw := range raws {
		var id TransactionID
		require.NoError(t, id.Scan(raw))
		got, err := id.RawXID()
		require.NoError(t, err)
		assert.Equal(t, raw, got, "id %s", id)
	}
}

func TestTransactionIDRawXIDRoundTripFromIDs(t *testing.T) {
	ids := []TransactionID{"0.0.0", "1.2.3", "4.23.147664", "65535.65535.4294967295", "65535.0.0", "0.65535.0", "0.0.4294967295"}
	for _, id := range ids {
		raw, err := id.RawXID()
		require.NoError(t, err)
		require.Len(t, raw, 8)
		var back TransactionID
		require.NoError(t, back.Scan(raw))
		assert.Equal(t, id, back)
	}
}

func TestTransactionIDRawXIDKnownValue(t *testing.T) {
	raw, err := TransactionID("1.2.3").RawXID()
	require.NoError(t, err)
	assert.Equal(t, "0100020003000000", hex.EncodeToString(raw))
}

func TestTransactionIDRawXIDErrors(t *testing.T) {
	tests := []struct {
		name string
		id   TransactionID
	}{
		{"too few parts", "1.2"},
		{"too many parts", "1.2.3.4"},
		{"empty", ""},
		{"non-numeric usn", "x.2.3"},
		{"non-numeric slot", "1.x.3"},
		{"non-numeric seq", "1.2.x"},
		{"slot too large", "1.65536.3"},
		{"usn too large", "65536.2.3"},
		{"seq too large", "1.2.4294967296"},
		{"negative usn", "-1.2.3"},
		{"negative slot", "1.-2.3"},
		{"negative seq", "1.2.-3"},
		{"injection", "1.2.3 OR 1=1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw, err := test.id.RawXID()
			require.Error(t, err)
			assert.Nil(t, raw)
		})
	}
}

func FuzzTransactionIDRawXIDRoundTrip(f *testing.F) {
	f.Add([]byte{0x04, 0x00, 0x17, 0x00, 0xD0, 0x40, 0x02, 0x00})
	f.Add(make([]byte, 8))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) != 8 {
			t.Skip()
		}
		var id TransactionID
		require.NoError(t, id.Scan(raw))
		got, err := id.RawXID()
		require.NoError(t, err)
		require.Equal(t, raw, got)
	})
}
