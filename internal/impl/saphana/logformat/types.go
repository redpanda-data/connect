// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// Package logformat contains types and parser scaffolding for the SAP HANA
// redo log binary format, reverse-engineered from vendor CDC tool documentation,
// trace output, and internal analysis. This is NOT an official SAP specification.
// See doc/ for per-block-type documentation and sources.
package logformat

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Ensure encoding/binary is used; it is available for callers performing
// little-endian reads from log pages.
var _ = binary.LittleEndian

// PageSize is the fixed redo log page size.
// All log I/O in SAP HANA is aligned to 4096-byte boundaries.
const PageSize = 4096

// ArchiveHeaderSize is the extra header prepended to archive (backup) log files.
// Source: rtdi.io — "An archive log is a transaction log with an additional
// 4KB of data at the beginning."
// See doc/01_file_layout.md for details.
const ArchiveHeaderSize = 4096

// DefaultLogBuffers is the default number of in-memory log buffers per service.
// Configurable via log_buffer_count in global.ini.
const DefaultLogBuffers = 8

// DefaultLogBufferKB is the default size of each log buffer in kilobytes.
// Configurable via log_buffer_size_kb in global.ini.
const DefaultLogBufferKB = 1024

// LSN (Log Sequence Number) is a monotonically increasing uint64 identifying
// a position in the redo log. Displayed as hex in trace output (e.g.,
// "0x70cb3180"); decimal in archive log filenames (e.g., "538414336").
//
// LSNs are confirmed wider than 32 bits: HVR example 0x26524f683 =
// 10,286,855,811. Stored as uint64 in SYS.M_LOG_SEGMENTS (BIGINT columns).
//
// Source: SYS.M_LOG_SEGMENTS MIN_POSITION/MAX_POSITION columns (BIGINT).
type LSN uint64

// ParseLSN parses a decimal or hexadecimal (0x-prefixed) string into an LSN.
// Returns an error for empty strings or strings with invalid characters.
func ParseLSN(s string) (LSN, error) {
	if s == "" {
		return 0, errors.New("logformat: ParseLSN: empty string")
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		v, err := strconv.ParseUint(s[2:], 16, 64)
		if err != nil {
			return 0, fmt.Errorf("logformat: ParseLSN %q: %w", s, err)
		}
		return LSN(v), nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("logformat: ParseLSN %q: %w", s, err)
	}
	return LSN(v), nil
}

// String returns the LSN as a hexadecimal string (matching HANA trace output format).
func (l LSN) String() string {
	return fmt.Sprintf("0x%x", uint64(l))
}

// RowID is the immutable 64-bit identifier assigned to each row at insert time.
// Never changes through delta merges. Used in redo log entries to reference rows.
// Source: SAP HANA internal analysis.
type RowID uint64

// FragID is the immutable fragment identifier. Changes when data migrates to a
// new fragment during delta merge. Combined with RowPos for O(1) row lookup.
// Source: SAP HANA internal analysis.
type FragID uint64

// RowPos is the row's position (array offset) within its fragment's column arrays.
// Local to the fragment; changes after delta merges. Max value 2^31 (uint32).
// Source: SAP HANA internal analysis.
type RowPos uint32

// RowIDTriple is the complete row reference used in redo log entries.
// Resolution: if the fragment (FragID) still exists, use (FragID, RowPos)
// for O(1) direct lookup. Otherwise, use RowID to search the inverted index.
//
// Source: SAP HANA internal analysis.
type RowIDTriple struct {
	RowID  RowID
	FragID FragID
	RowPos RowPos
}

// MVCCTimestamp is a 64-bit MVCC version timestamp.
//
// Encoding:
//
//	High bit 0 (in-flight): bits[62:32]=TCBIndex(31-bit), bits[31:0]=SSN(32-bit)
//	High bit 1 (committed): bits[62:0]=CommitTimestamp(63-bit monotonic)
//
// Source: SAP HANA internal analysis; see doc/13_rowid_encoding.md.
type MVCCTimestamp uint64

// IsCommitted returns true if this timestamp represents a committed transaction.
// A committed timestamp has the high bit set (bit 63 = 1).
func (ts MVCCTimestamp) IsCommitted() bool {
	return ts>>63 == 1
}

// CommitTimestamp returns the 63-bit monotonic commit timestamp.
// Only valid when IsCommitted() is true.
// Bits[62:0] with the high bit cleared.
func (ts MVCCTimestamp) CommitTimestamp() uint64 {
	return uint64(ts &^ (MVCCTimestamp(1) << 63))
}

// TCBIndex returns the Transaction Control Block index for in-flight transactions.
// Only valid when IsCommitted() is false.
// Bits[62:32], a 31-bit value.
func (ts MVCCTimestamp) TCBIndex() uint32 {
	return uint32((ts >> 32) & 0x7FFFFFFF)
}

// SSN returns the Statement Sequence Number for in-flight transactions.
// Only valid when IsCommitted() is false.
// Bits[31:0].
func (ts MVCCTimestamp) SSN() uint32 {
	return uint32(ts & 0xFFFFFFFF)
}

// String returns a human-readable representation of the MVCC timestamp.
func (ts MVCCTimestamp) String() string {
	if ts.IsCommitted() {
		return fmt.Sprintf("MVCCTimestamp{committed, ts=%d}", ts.CommitTimestamp())
	}
	return fmt.Sprintf("MVCCTimestamp{in-flight, tcb=%d, ssn=%d}", ts.TCBIndex(), ts.SSN())
}

// BlockType identifies the type of a redo log block entry.
// Numeric values are NOT publicly documented by SAP.
// Type names are inferred from rtdi.io, HVR docs, and internal analysis.
//
// The actual wire size (uint8 vs uint16) is UNKNOWN. This type uses uint8 as
// a conservative assumption; the sentinel constants below use values ≥0xF6
// to signal "real code unknown."
type BlockType uint8

const (
	// BlockTypeUnknown is the zero value, used when the type cannot be identified.
	BlockTypeUnknown BlockType = 0

	// The following constants use sentinel values (0xF6+) to indicate the real
	// numeric codes on the wire are not yet known. Do NOT use these values for
	// binary parsing — they are placeholders for documentation and type-switch
	// use in CDC reader code.

	// BlockTypeInsert — INSERT block: RowID + all column values.
	// Source: rtdi.io.
	BlockTypeInsert BlockType = 0xFF // UNKNOWN wire numeric code

	// BlockTypeUpdate — UPDATE block: N before-RowIDs, 1 after-RowID, column bitmap, N×values.
	// Source: rtdi.io, HVR docs.
	BlockTypeUpdate BlockType = 0xFE // UNKNOWN wire numeric code

	// BlockTypeDelete — DELETE block: RowID only, no column values.
	// Source: rtdi.io.
	BlockTypeDelete BlockType = 0xFD // UNKNOWN wire numeric code

	// BlockTypeUpsert — UPSERT/REPLACE block: distinct type, not decomposed.
	// Source: rtdi.io, HVR docs.
	BlockTypeUpsert BlockType = 0xFC // UNKNOWN wire numeric code

	// BlockTypeTruncate — TRUNCATE block: single block per table, no per-row events.
	// Source: rtdi.io.
	BlockTypeTruncate BlockType = 0xFB // UNKNOWN wire numeric code

	// BlockTypeCommit — COMMIT block: closes transaction, writes commit timestamp.
	// Source: rtdi.io.
	BlockTypeCommit BlockType = 0xFA // UNKNOWN wire numeric code

	// BlockTypeRollback — ROLLBACK block: transaction abort, CDC readers must discard.
	// Source: rtdi.io.
	BlockTypeRollback BlockType = 0xF9 // UNKNOWN wire numeric code

	// BlockTypeSavepoint — SAVEPOINT block: internal persistence checkpoint.
	// Source: SAP administration docs (inferred).
	BlockTypeSavepoint BlockType = 0xF8 // UNKNOWN wire numeric code

	// BlockTypeDDL — DDL (dictionary change) block: schema modification.
	// Source: SAP HANA internal analysis.
	BlockTypeDDL BlockType = 0xF7 // UNKNOWN wire numeric code

	// BlockTypeFiller — alignment padding to 4KB page boundary.
	// Source: rtdi.io (implicit from page alignment description).
	BlockTypeFiller BlockType = 0xF6 // UNKNOWN wire numeric code
)

// knownBlockTypes maps sentinel placeholder values to their names.
var knownBlockTypes = map[BlockType]string{
	BlockTypeUnknown:   "Unknown",
	BlockTypeInsert:    "Insert",
	BlockTypeUpdate:    "Update",
	BlockTypeDelete:    "Delete",
	BlockTypeUpsert:    "Upsert",
	BlockTypeTruncate:  "Truncate",
	BlockTypeCommit:    "Commit",
	BlockTypeRollback:  "Rollback",
	BlockTypeSavepoint: "Savepoint",
	BlockTypeDDL:       "DDL",
	BlockTypeFiller:    "Filler",
}

// String returns the block type name, or "BlockType(0x%02x)" for unrecognized values.
func (bt BlockType) String() string {
	if name, ok := knownBlockTypes[bt]; ok {
		return name
	}
	return fmt.Sprintf("BlockType(0x%02x)", uint8(bt))
}

// SegmentState is the lifecycle state of a log segment.
// Source: SYS.M_LOG_SEGMENTS STATE column; confirmed values from community posts
// and KBA posts.
type SegmentState uint8

const (
	// SegmentStateOpen — currently receiving log writes.
	SegmentStateOpen SegmentState = 0
	// SegmentStateClosed — full; no longer receiving writes; not yet backed up.
	SegmentStateClosed SegmentState = 1
	// SegmentStateBackedUp — successfully included in a log backup.
	SegmentStateBackedUp SegmentState = 2
	// SegmentStateFree — LSN horizon past this segment; can be reused.
	SegmentStateFree SegmentState = 3
	// SegmentStateRetainedFree — free but retained (e.g., pending replication).
	SegmentStateRetainedFree SegmentState = 4
	// SegmentStateClosedIncomplete — closed but write did not complete cleanly.
	SegmentStateClosedIncomplete SegmentState = 5
	// SegmentStateFreeIncomplete — was incomplete; now freed.
	SegmentStateFreeIncomplete SegmentState = 6
	// SegmentStatePreallocated — reserved on disk but not yet initialized.
	SegmentStatePreallocated SegmentState = 7
	// SegmentStateFormatting — currently being initialized (zeroed/header written).
	SegmentStateFormatting SegmentState = 8
)

var segmentStateNames = map[SegmentState]string{
	SegmentStateOpen:             "Open",
	SegmentStateClosed:           "Closed",
	SegmentStateBackedUp:         "BackedUp",
	SegmentStateFree:             "Free",
	SegmentStateRetainedFree:     "RetainedFree",
	SegmentStateClosedIncomplete: "ClosedIncomplete",
	SegmentStateFreeIncomplete:   "FreeIncomplete",
	SegmentStatePreallocated:     "Preallocated",
	SegmentStateFormatting:       "Formatting",
}

// String returns the segment state name, or "SegmentState(%d)" for unknown values.
func (s SegmentState) String() string {
	if name, ok := segmentStateNames[s]; ok {
		return name
	}
	return fmt.Sprintf("SegmentState(%d)", uint8(s))
}

// DirPageHeader is the header of a directory file page.
//
// Source: SAP KBA 2908105 error output:
//
//	LogDirPage[log=0, phy=330, ecnt=10240, cmax=1, seq=174337, ver=2, chk=202a90121b3c1c0]
//
// Exact byte offsets and sizes within the 4096-byte page are UNKNOWN.
// See doc/03_directory_file.md for the selection algorithm and shadow paging details.
type DirPageHeader struct {
	// LogIndex is the logical page index ('log' field).
	// Multiple of EntryCount for the first logical page in the physical pair.
	LogIndex uint32

	// PhyIndex is the physical page index on disk ('phy' field).
	// Shadow pairs use physical pages 2N and 2N+1 for logical page N.
	PhyIndex uint32

	// EntryCount is the number of segment metadata entries per page ('ecnt' field).
	// Default observed value: 10240 (0x2800).
	EntryCount uint32

	// CMax is the shadow page capacity slot count ('cmax' field).
	// Observed value: 1. Likely the number of shadow copies (1 or 2).
	CMax uint32

	// Generation is the shadow page generation counter ('seq' field).
	// In a shadow pair, the physical page with the higher Generation and a valid
	// checksum is the active (most recent) page.
	Generation uint64

	// Version is the format version ('ver' field).
	// Observed value: 2.
	Version uint16

	// Checksum is the page integrity checksum ('chk' field).
	// Observed value: 0x202a90121b3c1c0 (57-bit hex, algorithm UNKNOWN).
	Checksum uint64
}

// ArchiveLogName encodes the fields from an archive log backup filename.
//
// Format: log_backup_<VolumeID>_<PartitionID>_<FirstLSN>_<LastLSN>[.<BackupID>]
//
// Example: log_backup_1_0_538414336_538623872.1415905765532
//
// See doc/01_file_layout.md for a worked example with field-by-field explanation.
type ArchiveLogName struct {
	// VolumeID is the service volume identifier (typically 1 for the index server).
	VolumeID int
	// PartitionID is the partition within the volume (typically 0 for single-node).
	PartitionID int
	// FirstLSN is the first Log Sequence Number in the segment.
	FirstLSN LSN
	// LastLSN is the last Log Sequence Number in the segment.
	LastLSN LSN
	// BackupID is the backup identifier in millisecond epoch format.
	// Zero if not present in the filename (some older formats omit it).
	BackupID int64
}

// ParseArchiveLogName parses an archive log filename into its component fields.
//
// Accepted formats:
//   - log_backup_<VolumeID>_<PartitionID>_<FirstLSN>_<LastLSN>.<BackupID>
//   - log_backup_<VolumeID>_<PartitionID>_<FirstLSN>_<LastLSN>
//
// Returns an error if the filename does not start with "log_backup_" or if any
// numeric field cannot be parsed.
func ParseArchiveLogName(name string) (ArchiveLogName, error) {
	const prefix = "log_backup_"
	if !strings.HasPrefix(name, prefix) {
		return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: does not start with %q", name, prefix)
	}

	rest := name[len(prefix):]

	// Split off optional BackupID suffix (after the last dot, if present).
	var backupStr string
	if dotIdx := strings.LastIndex(rest, "."); dotIdx >= 0 {
		backupStr = rest[dotIdx+1:]
		rest = rest[:dotIdx]
	}

	parts := strings.Split(rest, "_")
	if len(parts) != 4 {
		return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: expected 4 underscore-separated fields after prefix, got %d", name, len(parts))
	}

	volumeID, err := strconv.Atoi(parts[0])
	if err != nil {
		return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: VolumeID: %w", name, err)
	}

	partitionID, err := strconv.Atoi(parts[1])
	if err != nil {
		return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: PartitionID: %w", name, err)
	}

	firstLSN, err := ParseLSN(parts[2])
	if err != nil {
		return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: FirstLSN: %w", name, err)
	}

	lastLSN, err := ParseLSN(parts[3])
	if err != nil {
		return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: LastLSN: %w", name, err)
	}

	var backupID int64
	if backupStr != "" {
		backupID, err = strconv.ParseInt(backupStr, 10, 64)
		if err != nil {
			return ArchiveLogName{}, fmt.Errorf("logformat: ParseArchiveLogName %q: BackupID: %w", name, err)
		}
	}

	return ArchiveLogName{
		VolumeID:    volumeID,
		PartitionID: partitionID,
		FirstLSN:    firstLSN,
		LastLSN:     lastLSN,
		BackupID:    backupID,
	}, nil
}

// String returns the archive log filename in the standard SAP HANA format.
// If BackupID is zero, it is omitted from the output.
func (a ArchiveLogName) String() string {
	base := fmt.Sprintf("log_backup_%d_%d_%d_%d", a.VolumeID, a.PartitionID, uint64(a.FirstLSN), uint64(a.LastLSN))
	if a.BackupID != 0 {
		return fmt.Sprintf("%s.%d", base, a.BackupID)
	}
	return base
}
