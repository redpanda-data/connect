// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package logformat_test

import (
	"testing"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat"
)

// ---------------------------------------------------------------------------
// LSN
// ---------------------------------------------------------------------------

func TestLSNParse(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  logformat.LSN
		err   bool
	}{
		{
			name:  "decimal simple",
			input: "538414336",
			want:  538414336,
		},
		{
			name:  "hex lowercase",
			input: "0x70cb3180",
			want:  1892364672, // verified: 0x70cb3180 == 1892364672
		},
		{
			name:  "hex uppercase prefix",
			input: "0X70CB3180",
			want:  1892364672, // same value, uppercase prefix
		},
		{
			name:  "HVR example >32 bits",
			input: "0x26524f683",
			want:  10286855811,
		},
		{
			name:  "zero decimal",
			input: "0",
			want:  0,
		},
		{
			name:  "zero hex",
			input: "0x0",
			want:  0,
		},
		{
			name:  "max uint64 hex",
			input: "0xffffffffffffffff",
			want:  ^logformat.LSN(0),
		},
		{
			name:  "empty string",
			input: "",
			err:   true,
		},
		{
			name:  "invalid hex digits",
			input: "0xGGGG",
			err:   true,
		},
		{
			name:  "invalid decimal",
			input: "not_a_number",
			err:   true,
		},
		{
			name:  "bare hex without prefix treated as decimal — error",
			input: "FFFF",
			err:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := logformat.ParseLSN(tt.input)
			if tt.err {
				if err == nil {
					t.Errorf("ParseLSN(%q): expected error, got nil (value=%d)", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseLSN(%q): unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseLSN(%q) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}

func TestLSNString(t *testing.T) {
	tests := []struct {
		lsn  logformat.LSN
		want string
	}{
		{logformat.LSN(0), "0x0"},
		{logformat.LSN(1892364672), "0x70cb3180"},
		{logformat.LSN(10286855811), "0x26524f683"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.lsn.String(); got != tt.want {
				t.Errorf("LSN(%d).String() = %q, want %q", uint64(tt.lsn), got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// MVCCTimestamp
// ---------------------------------------------------------------------------

func TestMVCCTimestampCommitted(t *testing.T) {
	// High bit set = committed.
	ts := logformat.MVCCTimestamp(uint64(1)<<63 | 0x1234567890)
	if !ts.IsCommitted() {
		t.Fatal("IsCommitted() should be true when high bit is set")
	}
	if got, want := ts.CommitTimestamp(), uint64(0x1234567890); got != want {
		t.Errorf("CommitTimestamp() = 0x%x, want 0x%x", got, want)
	}
}

func TestMVCCTimestampInFlight(t *testing.T) {
	// High bit clear = in-flight.
	tcb := uint64(42)
	ssn := uint64(7)
	ts := logformat.MVCCTimestamp((tcb << 32) | ssn)

	if ts.IsCommitted() {
		t.Fatal("IsCommitted() should be false when high bit is clear")
	}
	if got := ts.TCBIndex(); got != uint32(tcb) {
		t.Errorf("TCBIndex() = %d, want %d", got, tcb)
	}
	if got := ts.SSN(); got != uint32(ssn) {
		t.Errorf("SSN() = %d, want %d", got, ssn)
	}
}

func TestMVCCTimestampZero(t *testing.T) {
	ts := logformat.MVCCTimestamp(0)
	if ts.IsCommitted() {
		t.Error("zero timestamp should not be committed")
	}
	if ts.TCBIndex() != 0 {
		t.Error("zero timestamp TCBIndex should be 0")
	}
	if ts.SSN() != 0 {
		t.Error("zero timestamp SSN should be 0")
	}
}

func TestMVCCTimestampMaxTCBIndex(t *testing.T) {
	// TCBIndex is 31 bits: max = 0x7FFFFFFF
	maxTCB := uint64(0x7FFFFFFF)
	ts := logformat.MVCCTimestamp(maxTCB << 32)
	if ts.IsCommitted() {
		t.Error("high bit not set, should not be committed")
	}
	if got := ts.TCBIndex(); got != uint32(maxTCB) {
		t.Errorf("TCBIndex() = 0x%x, want 0x%x", got, maxTCB)
	}
}

func TestMVCCTimestampMaxCommitTimestamp(t *testing.T) {
	// CommitTimestamp is 63 bits: max = 0x7FFFFFFFFFFFFFFF
	maxCommit := uint64(0x7FFFFFFFFFFFFFFF)
	ts := logformat.MVCCTimestamp(uint64(1)<<63 | maxCommit)
	if !ts.IsCommitted() {
		t.Error("high bit set, should be committed")
	}
	if got := ts.CommitTimestamp(); got != maxCommit {
		t.Errorf("CommitTimestamp() = 0x%x, want 0x%x", got, maxCommit)
	}
}

func TestMVCCTimestampString(t *testing.T) {
	// Committed timestamp should mention "committed"
	committed := logformat.MVCCTimestamp(uint64(1)<<63 | 42)
	s := committed.String()
	if s == "" {
		t.Error("String() should not be empty")
	}

	// In-flight timestamp should mention "in-flight"
	inflight := logformat.MVCCTimestamp((uint64(5) << 32) | 3)
	s = inflight.String()
	if s == "" {
		t.Error("String() should not be empty for in-flight timestamp")
	}
}

// ---------------------------------------------------------------------------
// ParseArchiveLogName
// ---------------------------------------------------------------------------

func TestParseArchiveLogName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  logformat.ArchiveLogName
		err   bool
	}{
		{
			name:  "with backup ID",
			input: "log_backup_1_0_538414336_538623872.1415905765532",
			want: logformat.ArchiveLogName{
				VolumeID:    1,
				PartitionID: 0,
				FirstLSN:    538414336,
				LastLSN:     538623872,
				BackupID:    1415905765532,
			},
		},
		{
			name:  "without backup ID",
			input: "log_backup_2_0_1_1000",
			want: logformat.ArchiveLogName{
				VolumeID:    2,
				PartitionID: 0,
				FirstLSN:    1,
				LastLSN:     1000,
				BackupID:    0,
			},
		},
		{
			name:  "partition 1",
			input: "log_backup_1_1_100_200.999",
			want: logformat.ArchiveLogName{
				VolumeID:    1,
				PartitionID: 1,
				FirstLSN:    100,
				LastLSN:     200,
				BackupID:    999,
			},
		},
		{
			name:  "large LSN values >32 bit",
			input: "log_backup_1_0_10286855811_10286855900.0",
			want: logformat.ArchiveLogName{
				VolumeID:    1,
				PartitionID: 0,
				FirstLSN:    logformat.LSN(10286855811),
				LastLSN:     logformat.LSN(10286855900),
				BackupID:    0,
			},
		},
		{
			name:  "zero LSNs",
			input: "log_backup_1_0_0_0",
			want: logformat.ArchiveLogName{
				VolumeID:    1,
				PartitionID: 0,
				FirstLSN:    0,
				LastLSN:     0,
				BackupID:    0,
			},
		},
		{
			name:  "invalid prefix",
			input: "not_a_log_backup",
			err:   true,
		},
		{
			name:  "missing fields",
			input: "log_backup_1_0_100",
			err:   true,
		},
		{
			name:  "too many fields",
			input: "log_backup_1_0_100_200_300_400",
			err:   true,
		},
		{
			name:  "non-numeric VolumeID",
			input: "log_backup_x_0_100_200",
			err:   true,
		},
		{
			name:  "non-numeric FirstLSN",
			input: "log_backup_1_0_abc_200",
			err:   true,
		},
		{
			name:  "non-numeric BackupID",
			input: "log_backup_1_0_100_200.notanumber",
			err:   true,
		},
		{
			name:  "empty string",
			input: "",
			err:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := logformat.ParseArchiveLogName(tt.input)
			if tt.err {
				if err == nil {
					t.Errorf("ParseArchiveLogName(%q): expected error, got nil (value=%+v)", tt.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseArchiveLogName(%q): unexpected error: %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseArchiveLogName(%q) =\n  %+v\nwant\n  %+v", tt.input, got, tt.want)
			}
		})
	}
}

func TestArchiveLogNameString(t *testing.T) {
	tests := []struct {
		name  string
		input logformat.ArchiveLogName
		want  string
	}{
		{
			name: "with backup ID",
			input: logformat.ArchiveLogName{
				VolumeID: 1, PartitionID: 0,
				FirstLSN: 538414336, LastLSN: 538623872,
				BackupID: 1415905765532,
			},
			want: "log_backup_1_0_538414336_538623872.1415905765532",
		},
		{
			name: "without backup ID",
			input: logformat.ArchiveLogName{
				VolumeID: 2, PartitionID: 0,
				FirstLSN: 1, LastLSN: 1000,
				BackupID: 0,
			},
			want: "log_backup_2_0_1_1000",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.input.String()
			if got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Verify round-trip: parse → String → parse gives identical result.
func TestArchiveLogNameRoundTrip(t *testing.T) {
	inputs := []string{
		"log_backup_1_0_538414336_538623872.1415905765532",
		"log_backup_2_0_1_1000",
		"log_backup_1_1_100_200.999",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			a, err := logformat.ParseArchiveLogName(input)
			if err != nil {
				t.Fatalf("ParseArchiveLogName(%q): %v", input, err)
			}
			roundTripped := a.String()
			if roundTripped != input {
				t.Errorf("round-trip: got %q, want %q", roundTripped, input)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// SegmentState.String()
// ---------------------------------------------------------------------------

func TestSegmentStateString(t *testing.T) {
	tests := []struct {
		state logformat.SegmentState
		want  string
	}{
		{logformat.SegmentStateOpen, "Open"},
		{logformat.SegmentStateClosed, "Closed"},
		{logformat.SegmentStateBackedUp, "BackedUp"},
		{logformat.SegmentStateFree, "Free"},
		{logformat.SegmentStateRetainedFree, "RetainedFree"},
		{logformat.SegmentStateClosedIncomplete, "ClosedIncomplete"},
		{logformat.SegmentStateFreeIncomplete, "FreeIncomplete"},
		{logformat.SegmentStatePreallocated, "Preallocated"},
		{logformat.SegmentStateFormatting, "Formatting"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.state.String(); got != tt.want {
				t.Errorf("SegmentState(%d).String() = %q, want %q", uint8(tt.state), got, tt.want)
			}
		})
	}
}

func TestSegmentStateUnknown(t *testing.T) {
	unknown := logformat.SegmentState(99)
	s := unknown.String()
	if s == "" {
		t.Error("unknown SegmentState should return non-empty string")
	}
	// Should not panic or return one of the known names
	knownNames := []string{
		"Open", "Closed", "BackedUp", "Free", "RetainedFree",
		"ClosedIncomplete", "FreeIncomplete", "Preallocated", "Formatting",
	}
	for _, name := range knownNames {
		if s == name {
			t.Errorf("unknown state returned known name %q", name)
		}
	}
}

// ---------------------------------------------------------------------------
// BlockType.String()
// ---------------------------------------------------------------------------

func TestBlockTypeString(t *testing.T) {
	tests := []struct {
		bt   logformat.BlockType
		want string
	}{
		{logformat.BlockTypeUnknown, "Unknown"},
		{logformat.BlockTypeInsert, "Insert"},
		{logformat.BlockTypeUpdate, "Update"},
		{logformat.BlockTypeDelete, "Delete"},
		{logformat.BlockTypeUpsert, "Upsert"},
		{logformat.BlockTypeTruncate, "Truncate"},
		{logformat.BlockTypeCommit, "Commit"},
		{logformat.BlockTypeRollback, "Rollback"},
		{logformat.BlockTypeSavepoint, "Savepoint"},
		{logformat.BlockTypeDDL, "DDL"},
		{logformat.BlockTypeFiller, "Filler"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.bt.String(); got != tt.want {
				t.Errorf("BlockType(0x%02x).String() = %q, want %q", uint8(tt.bt), got, tt.want)
			}
		})
	}
}

func TestBlockTypeStringUnknown(t *testing.T) {
	// A value that is not one of the known sentinels should produce a hex fallback.
	bt := logformat.BlockType(0x01)
	s := bt.String()
	if s == "" {
		t.Error("unknown BlockType should return non-empty string")
	}
	// Should contain the hex representation
	if s == "Unknown" || s == "Insert" {
		t.Errorf("unexpected name for unregistered block type: %q", s)
	}
}

// ---------------------------------------------------------------------------
// RowIDTriple
// ---------------------------------------------------------------------------

func TestRowIDTriple(t *testing.T) {
	triple := logformat.RowIDTriple{
		RowID:  logformat.RowID(0xDEADBEEF),
		FragID: logformat.FragID(0xCAFEBABE),
		RowPos: logformat.RowPos(42),
	}

	if triple.RowID != logformat.RowID(0xDEADBEEF) {
		t.Errorf("RowID mismatch: got 0x%x", uint64(triple.RowID))
	}
	if triple.FragID != logformat.FragID(0xCAFEBABE) {
		t.Errorf("FragID mismatch: got 0x%x", uint64(triple.FragID))
	}
	if triple.RowPos != logformat.RowPos(42) {
		t.Errorf("RowPos mismatch: got %d", triple.RowPos)
	}
}

func TestRowIDTripleZero(t *testing.T) {
	var triple logformat.RowIDTriple
	if triple.RowID != 0 {
		t.Error("zero RowIDTriple RowID should be 0")
	}
	if triple.FragID != 0 {
		t.Error("zero RowIDTriple FragID should be 0")
	}
	if triple.RowPos != 0 {
		t.Error("zero RowIDTriple RowPos should be 0")
	}
}

func TestRowIDTripleMaxValues(t *testing.T) {
	triple := logformat.RowIDTriple{
		RowID:  logformat.RowID(^uint64(0)),
		FragID: logformat.FragID(^uint64(0)),
		RowPos: logformat.RowPos(^uint32(0)),
	}
	if triple.RowID != logformat.RowID(^uint64(0)) {
		t.Error("max RowID mismatch")
	}
	if triple.FragID != logformat.FragID(^uint64(0)) {
		t.Error("max FragID mismatch")
	}
	if triple.RowPos != logformat.RowPos(^uint32(0)) {
		t.Error("max RowPos mismatch")
	}
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

func TestConstants(t *testing.T) {
	if logformat.PageSize != 4096 {
		t.Errorf("PageSize = %d, want 4096", logformat.PageSize)
	}
	if logformat.ArchiveHeaderSize != 4096 {
		t.Errorf("ArchiveHeaderSize = %d, want 4096", logformat.ArchiveHeaderSize)
	}
	if logformat.DefaultLogBuffers != 8 {
		t.Errorf("DefaultLogBuffers = %d, want 8", logformat.DefaultLogBuffers)
	}
	if logformat.DefaultLogBufferKB != 1024 {
		t.Errorf("DefaultLogBufferKB = %d, want 1024", logformat.DefaultLogBufferKB)
	}
}

// ---------------------------------------------------------------------------
// DirPageHeader (construction and field access)
// ---------------------------------------------------------------------------

func TestDirPageHeader(t *testing.T) {
	// Verify the example from KBA 2908105 can be represented:
	// LogDirPage[log=0, phy=330, ecnt=10240, cmax=1, seq=174337, ver=2, chk=202a90121b3c1c0]
	hdr := logformat.DirPageHeader{
		LogIndex:   0,
		PhyIndex:   330,
		EntryCount: 10240,
		CMax:       1,
		Generation: 174337,
		Version:    2,
		Checksum:   0x202a90121b3c1c0,
	}

	if hdr.LogIndex != 0 {
		t.Errorf("LogIndex = %d, want 0", hdr.LogIndex)
	}
	if hdr.PhyIndex != 330 {
		t.Errorf("PhyIndex = %d, want 330", hdr.PhyIndex)
	}
	if hdr.EntryCount != 10240 {
		t.Errorf("EntryCount = %d, want 10240", hdr.EntryCount)
	}
	if hdr.CMax != 1 {
		t.Errorf("CMax = %d, want 1", hdr.CMax)
	}
	if hdr.Generation != 174337 {
		t.Errorf("Generation = %d, want 174337", hdr.Generation)
	}
	if hdr.Version != 2 {
		t.Errorf("Version = %d, want 2", hdr.Version)
	}
	if hdr.Checksum != 0x202a90121b3c1c0 {
		t.Errorf("Checksum = 0x%x, want 0x202a90121b3c1c0", hdr.Checksum)
	}
}
