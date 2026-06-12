// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

//go:build ignore

// block_decoder.go — isolates individual block types by binary-diffing log
// segments before/after each DML operation. Run directly:
//
//	go run ./internal/impl/saphana/logformat/investigate/block_decoder.go \
//	   --log-dir /tmp/hana-express-data/log/mnt00001
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

const hanaVer = "2.00.088.00.1760424921"

type BlockSample struct {
	OpType       string   `json:"op_type"`
	HANAVersion  string   `json:"hana_version"`
	RawBytes     string   `json:"raw_bytes_hex"` // hex of new bytes since last snapshot
	PageIndex    int      `json:"page_index"`
	OffsetInPage int      `json:"offset_in_page"`
	Notes        []string `json:"notes"`
	Timestamp    string   `json:"timestamp"`
}

var samples []BlockSample

func hdbsql(sql string) string {
	cmd := exec.Command("docker", "exec", "hxe",
		"/usr/sap/HXE/HDB90/exe/hdbsql",
		"-i", "90", "-u", "SYSTEM", "-p", "HXEHana1", "-d", "SYSTEMDB",
		"-a", sql)
	cmd.Env = append(os.Environ(), "DOCKER_API_VERSION=1.46")
	out, _ := cmd.CombinedOutput()
	return strings.TrimSpace(string(out))
}

func savepoint() {
	hdbsql("ALTER SYSTEM SAVEPOINT")
	time.Sleep(2 * time.Second)
}

func readLatestSegment(logDir string) (string, []byte) {
	pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	var segs []struct {
		path   string
		minLSN uint64
	}
	for _, f := range matches {
		d, err := os.ReadFile(f)
		if err != nil || len(d) < page.Size {
			continue
		}
		for i := range len(d) / page.Size {
			if binary.LittleEndian.Uint64(d[i*page.Size:i*page.Size+8]) == page.Magic {
				lsn := binary.LittleEndian.Uint64(d[i*page.Size+24 : i*page.Size+32])
				if lsn > 0 {
					segs = append(segs, struct {
						path   string
						minLSN uint64
					}{f, lsn})
					break
				}
			}
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].minLSN < segs[j].minLSN })
	if len(segs) == 0 {
		return "", nil
	}
	f := segs[len(segs)-1].path
	d, _ := os.ReadFile(f)
	return f, d
}

// diffSegments returns newly-written pages since before snapshot.
// A "new" page is one that was zero/non-magic before but has magic now, or
// whose payload bytes changed.
func diffSegments(before, after []byte, segPath string) []BlockSample {
	var out []BlockSample
	numPages := min(len(before)/page.Size, len(after)/page.Size)
	for i := range numPages {
		bMagic := len(before) > i*page.Size+8 && binary.LittleEndian.Uint64(before[i*page.Size:i*page.Size+8]) == page.Magic
		aMagic := binary.LittleEndian.Uint64(after[i*page.Size:i*page.Size+8]) == page.Magic
		if !aMagic {
			continue
		}

		bPayload := after[i*page.Size+page.HeaderSize : (i+1)*page.Size]
		usedBytes := int(binary.LittleEndian.Uint32(after[i*page.Size+76 : i*page.Size+80]))
		if usedBytes <= 0 || usedBytes > page.Size-page.HeaderSize {
			continue
		}
		bPayload = bPayload[:usedBytes]

		if bMagic && len(before) >= (i+1)*page.Size {
			beforePayload := before[i*page.Size+page.HeaderSize : (i+1)*page.Size]
			bUsed := int(binary.LittleEndian.Uint32(before[i*page.Size+76 : i*page.Size+80]))
			if bUsed == usedBytes {
				same := true
				for j := range bUsed {
					if beforePayload[j] != bPayload[j] {
						same = false
						break
					}
				}
				if same {
					continue
				} // payload unchanged
			}
		}

		out = append(out, BlockSample{
			HANAVersion:  hanaVer,
			PageIndex:    i,
			OffsetInPage: page.HeaderSize,
			RawBytes:     hexStr(bPayload),
		})
	}
	return out
}

func hexStr(b []byte) string {
	sb := strings.Builder{}
	for i, v := range b {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "%02x", v)
	}
	return sb.String()
}

func analyzePayload(raw []byte, opType string) []string {
	var notes []string
	notes = append(notes, fmt.Sprintf("payload length: %d bytes", len(raw)))

	// Look for known patterns
	intSentinel := []byte{0x2A, 0x00, 0x00, 0x00} // int 42 LE
	if pos := findBytes(raw, intSentinel); len(pos) > 0 {
		notes = append(notes, fmt.Sprintf("INTEGER 42 [2a 00 00 00] at payload+%d", pos[0]))
		// First byte before the int might give context
		if pos[0] >= 1 {
			notes = append(notes, fmt.Sprintf("  byte before int: 0x%02x", raw[pos[0]-1]))
		}
	}

	// Look for VARCHAR PROBE marker
	probe := []byte("PROBE")
	if pos := findBytes(raw, probe); len(pos) > 0 {
		notes = append(notes, fmt.Sprintf("VARCHAR probe at payload+%d", pos[0]))
		if pos[0] >= 1 {
			notes = append(notes, fmt.Sprintf("  1-byte prefix: 0x%02x = %d", raw[pos[0]-1], raw[pos[0]-1]))
		}
	}

	// Look for INT64_MAX sentinel (end marker?)
	endSentinel := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F}
	if pos := findBytes(raw, endSentinel); len(pos) > 0 {
		notes = append(notes, fmt.Sprintf("INT64_MAX sentinel at payload+%d (possible end-of-record marker)", pos[0]))
	}

	// Find the first non-repeating byte (the block type code candidate)
	if len(raw) > 0 {
		notes = append(notes, fmt.Sprintf("first byte: 0x%02x (block type candidate)", raw[0]))
		if len(raw) >= 4 {
			notes = append(notes, fmt.Sprintf("first 4 bytes: %s (uint32 LE = 0x%08x)", hexStr(raw[:4]), binary.LittleEndian.Uint32(raw[:4])))
		}
	}

	// Scan for null bitmap pattern (usually after RowID)
	// RowID is uint64 (8 bytes). After RowID, look for a bitmap byte
	if len(raw) >= 8 {
		rowID := binary.LittleEndian.Uint64(raw[:8])
		if rowID > 0 && rowID < 100000 {
			notes = append(notes, fmt.Sprintf("possible RowID at payload+0: %d (if block starts with RowID)", rowID))
		}
	}

	_ = opType
	return notes
}

func findBytes(data, needle []byte) []int {
	var out []int
	for i := 0; i <= len(data)-len(needle); i++ {
		ok := true
		for j := range needle {
			if data[i+j] != needle[j] {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, i)
		}
	}
	return out
}

func captureAndAnalyze(logDir, opType, sql string, before []byte, beforeFile string) []BlockSample {
	hdbsql(sql)
	savepoint()
	afterFile, after := readLatestSegment(logDir)
	_ = afterFile

	var beforeData []byte
	if beforeFile == afterFile {
		beforeData = before
	}
	if len(beforeData) == 0 {
		beforeData = make([]byte, len(after)) // zeros = treat all as new
	}

	diffs := diffSegments(beforeData, after, afterFile)
	for i := range diffs {
		diffs[i].OpType = opType
		raw, _ := parseHexToBytes(diffs[i].RawBytes)
		diffs[i].Notes = analyzePayload(raw, opType)
		diffs[i].Timestamp = time.Now().Format(time.RFC3339)
		fmt.Printf("  [%s] page %d payload (%d bytes): %s\n", opType, diffs[i].PageIndex, len(raw), diffs[i].RawBytes[:min(64, len(diffs[i].RawBytes))])
		for _, n := range diffs[i].Notes {
			fmt.Printf("    %s\n", n)
		}
	}
	return diffs
}

func parseHexToBytes(hex string) ([]byte, error) {
	parts := strings.Fields(hex)
	out := make([]byte, len(parts))
	for i, p := range parts {
		var b byte
		fmt.Sscanf(p, "%02x", &b)
		out[i] = b
	}
	return out, nil
}

func tryChecksums(logDir string) {
	pattern := filepath.Join(logDir, "hdb*", "logsegment_000_directory.dat")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		return
	}

	data, _ := os.ReadFile(matches[0])
	if len(data) < page.Size {
		return
	}

	fmt.Println("\n=== Checksum investigation ===")
	fmt.Printf("First 48 bytes of directory page 0:\n")
	for off := 0; off < 48; off += 8 {
		fmt.Printf("  [%2d]: %02x %02x %02x %02x  %02x %02x %02x %02x\n", off,
			data[off], data[off+1], data[off+2], data[off+3],
			data[off+4], data[off+5], data[off+6], data[off+7])
	}

	// Try checksum at every 8-byte boundary in first 64 bytes
	for chkOff := 0; chkOff < 64; chkOff += 8 {
		if chkOff+8 > len(data) {
			continue
		}
		stored := binary.LittleEndian.Uint64(data[chkOff : chkOff+8])
		if stored == 0 {
			continue
		}

		// Zero out candidate field and try algorithms
		zeroed := make([]byte, page.Size)
		copy(zeroed, data[:page.Size])
		binary.LittleEndian.PutUint64(zeroed[chkOff:chkOff+8], 0)

		crc64 := computeCRC64(zeroed)
		fl64 := computeFletcher64(zeroed)
		xor64 := computeXOR64(zeroed)
		sum64 := computeSum64(zeroed)   // simple addition
		adler := computeAdler32(zeroed) // Adler-32 (32-bit)
		crc32v := computeCRC32(zeroed)  // CRC-32

		match := "no match"
		if stored == crc64 {
			match = "CRC-64/ECMA-182"
		}
		if stored == fl64 {
			match = "Fletcher-64"
		}
		if stored == xor64 {
			match = "XOR-64"
		}
		if stored == sum64 {
			match = "Sum-64"
		}
		if stored&0xFFFFFFFF == uint64(adler) {
			match = "Adler-32 (low 32 bits)"
		}
		if stored&0xFFFFFFFF == uint64(crc32v) {
			match = "CRC-32 (low 32 bits)"
		}

		fmt.Printf("  offset %2d: stored=0x%016x → %s\n", chkOff, stored, match)
	}

	// Try: checksum over just the header (first 32 bytes with checksum zeroed)
	for chkOff := 0; chkOff < 64; chkOff += 8 {
		if chkOff+8 > len(data) {
			continue
		}
		stored := binary.LittleEndian.Uint64(data[chkOff : chkOff+8])
		if stored == 0 {
			continue
		}
		// Compute over just first chkOff bytes (header before checksum)
		headerOnly := make([]byte, chkOff)
		copy(headerOnly, data[:chkOff])
		crc := computeCRC64(headerOnly)
		fl := computeFletcher64(headerOnly)
		if stored == crc {
			fmt.Printf("  CRC-64 over first %d bytes matches offset %d!\n", chkOff, chkOff)
		}
		if stored == fl {
			fmt.Printf("  Fletcher-64 over first %d bytes matches offset %d!\n", chkOff, chkOff)
		}
	}
}

func computeCRC64(data []byte) uint64 {
	const poly = uint64(0xC96C5795D7870F42)
	table := make([]uint64, 256)
	for i := range table {
		crc := uint64(i)
		for range 8 {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	crc := ^uint64(0)
	for _, b := range data {
		crc = table[byte(crc)^b] ^ (crc >> 8)
	}
	return ^crc
}

func computeFletcher64(data []byte) uint64 {
	var a, b uint64
	for i := 0; i+3 < len(data); i += 4 {
		w := uint64(binary.LittleEndian.Uint32(data[i : i+4]))
		a = (a + w) & 0xFFFFFFFF
		b = (b + a) & 0xFFFFFFFF
	}
	return (b << 32) | a
}

func computeXOR64(data []byte) uint64 {
	var r uint64
	for i := 0; i+7 < len(data); i += 8 {
		r ^= binary.LittleEndian.Uint64(data[i : i+8])
	}
	return r
}

func computeSum64(data []byte) uint64 {
	var s uint64
	for i := 0; i+7 < len(data); i += 8 {
		s += binary.LittleEndian.Uint64(data[i : i+8])
	}
	return s
}

func computeAdler32(data []byte) uint32 {
	const mod = uint32(65521)
	a, b := uint32(1), uint32(0)
	for _, v := range data {
		a = (a + uint32(v)) % mod
		b = (b + a) % mod
	}
	return (b << 16) | a
}

func computeCRC32(data []byte) uint32 {
	const poly = uint32(0xEDB88320)
	table := make([]uint32, 256)
	for i := range table {
		crc := uint32(i)
		for range 8 {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		table[i] = crc
	}
	crc := ^uint32(0)
	for _, b := range data {
		crc = table[byte(crc)^b] ^ (crc >> 8)
	}
	return ^crc
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	logDir := "/tmp/hana-express-data/log/mnt00001"
	for i, arg := range os.Args[1:] {
		if arg == "--log-dir" && i+1 < len(os.Args[1:]) {
			logDir = os.Args[i+2]
		}
	}

	fmt.Println("=== SAP HANA Block Type Decoder ===")
	fmt.Printf("HANA: %s | Log: %s\n\n", hanaVer, logDir)

	// Setup fresh probe table
	hdbsql("DROP TABLE BDC.T 2>/dev/null; DROP SCHEMA BDC 2>/dev/null")
	hdbsql("CREATE SCHEMA BDC")
	hdbsql("CREATE COLUMN TABLE BDC.T (ID INTEGER NOT NULL PRIMARY KEY, STR_VAL VARCHAR(32), INT_VAL INTEGER, BOOL_VAL BOOLEAN)")
	savepoint()

	_, baseData := readLatestSegment(logDir)
	baseFile, _ := filepath.Glob(filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat"))
	_ = baseFile

	// Capture each operation type
	fmt.Println("--- SAVEPOINT block ---")
	samples = append(samples, captureAndAnalyze(logDir, "SAVEPOINT", "ALTER SYSTEM SAVEPOINT", baseData, "")...)

	_, baseData = readLatestSegment(logDir)

	fmt.Println("--- INSERT block ---")
	insertSQL := "INSERT INTO BDC.T (ID, STR_VAL, INT_VAL, BOOL_VAL) VALUES (1, 'PROBE', 42, TRUE)"
	samples = append(samples, captureAndAnalyze(logDir, "INSERT", insertSQL, baseData, "")...)

	_, baseData = readLatestSegment(logDir)

	fmt.Println("--- UPDATE block ---")
	updateSQL := "UPDATE BDC.T SET STR_VAL = 'UPDATED', INT_VAL = 99 WHERE ID = 1"
	samples = append(samples, captureAndAnalyze(logDir, "UPDATE", updateSQL, baseData, "")...)

	_, baseData = readLatestSegment(logDir)

	fmt.Println("--- DELETE block ---")
	deleteSQL := "DELETE FROM BDC.T WHERE ID = 1"
	samples = append(samples, captureAndAnalyze(logDir, "DELETE", deleteSQL, baseData, "")...)

	_, baseData = readLatestSegment(logDir)

	fmt.Println("--- UPSERT block ---")
	upsertSQL := "UPSERT BDC.T (ID, STR_VAL, INT_VAL) VALUES (2, 'UPSERTED', 77) WITH PRIMARY KEY"
	samples = append(samples, captureAndAnalyze(logDir, "UPSERT", upsertSQL, baseData, "")...)

	_, baseData = readLatestSegment(logDir)

	fmt.Println("--- DDL (CREATE TABLE) block ---")
	ddlSQL := "CREATE COLUMN TABLE BDC.DDL_TEST (ID INTEGER PRIMARY KEY)"
	samples = append(samples, captureAndAnalyze(logDir, "DDL_CREATE", ddlSQL, baseData, "")...)

	// Try more checksum algorithms
	tryChecksums(logDir)

	// Write findings
	data, _ := json.MarshalIndent(samples, "", "  ")
	os.WriteFile("/tmp/block-samples.json", data, 0644)
	fmt.Printf("\n%d block samples written to /tmp/block-samples.json\n", len(samples))
}
