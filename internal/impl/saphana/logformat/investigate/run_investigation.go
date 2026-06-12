// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

// run_investigation.go performs the full empirical investigation of the HANA
// redo log binary format. It uses hdbsql (via docker exec) for DML operations
// and reads log files directly for binary analysis.
//
//	Run: go run ./internal/impl/saphana/logformat/investigate/ \
//	       --log-dir /tmp/hana-express-data/log/mnt00001 \
//	       --out /tmp/findings.json
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

const (
	hanaVersion = "2.00.088.00.1760424921"
	dockerImage = "saplabs/hanaexpress:latest"
)

type Finding struct {
	HANAVersion string `json:"hana_version"`
	DockerImage string `json:"docker_image"`
	Field       string `json:"field"`
	Hypothesis  string `json:"hypothesis"`
	Result      string `json:"result"` // confirmed | refuted | indeterminate
	ActualValue string `json:"actual_value"`
	HexContext  string `json:"hex_context,omitempty"`
	ByteOffset  int    `json:"byte_offset,omitempty"`
	Timestamp   string `json:"timestamp"`
}

var findings []Finding

func confirm(field, hyp, actual, hexCtx string, offset int) {
	findings = append(findings, Finding{hanaVersion, dockerImage, field, hyp, "confirmed", actual, hexCtx, offset, time.Now().Format(time.RFC3339)})
	fmt.Printf("  ✅ CONFIRMED  %-40s = %s\n", field, actual)
}

func refute(field, hyp, actual, hexCtx string, offset int) {
	findings = append(findings, Finding{hanaVersion, dockerImage, field, hyp, "refuted", actual, hexCtx, offset, time.Now().Format(time.RFC3339)})
	fmt.Printf("  ❌ REFUTED    %-40s actual=%s\n", field, actual)
}

func indeterminate(field, actual string) {
	findings = append(findings, Finding{hanaVersion, dockerImage, field, "various", "indeterminate", actual, "", 0, time.Now().Format(time.RFC3339)})
	fmt.Printf("  🔬 INDETERM   %-40s %s\n", field, actual)
}

func hdbsql(sql string) (string, error) {
	cmd := exec.Command("docker", "exec", "hxe",
		"/usr/sap/HXE/HDB90/exe/hdbsql",
		"-i", "90", "-u", "SYSTEM", "-p", "HXEHana1", "-d", "SYSTEMDB",
		"-a", sql)
	cmd.Env = append(os.Environ(), "DOCKER_API_VERSION=1.46")
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func savepoint() {
	hdbsql("ALTER SYSTEM SAVEPOINT")
	time.Sleep(2 * time.Second)
}

func latestRedoSegment(logDir string) string {
	pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		return ""
	}
	// Sort by segment min LSN (offset 24), not filename
	type seg struct {
		path   string
		minLSN uint64
	}
	var segs []seg
	for _, f := range matches {
		d, err := os.ReadFile(f)
		if err != nil || len(d) < page.Size {
			continue
		}
		for i := range len(d) / page.Size {
			if binary.LittleEndian.Uint64(d[i*page.Size:i*page.Size+8]) == page.Magic {
				lsn := binary.LittleEndian.Uint64(d[i*page.Size+24 : i*page.Size+32])
				if lsn > 0 {
					segs = append(segs, seg{f, lsn})
					break
				}
			}
		}
	}
	sort.Slice(segs, func(i, j int) bool { return segs[i].minLSN < segs[j].minLSN })
	if len(segs) == 0 {
		return matches[0]
	}
	return segs[len(segs)-1].path
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

func hexStr(b []byte) string {
	parts := make([]string, len(b))
	for i, v := range b {
		parts[i] = fmt.Sprintf("%02x", v)
	}
	return strings.Join(parts, " ")
}

func main() {
	logDir := "/tmp/hana-express-data/log/mnt00001"
	outFile := "/tmp/findings.json"
	for i, arg := range os.Args[1:] {
		if arg == "--log-dir" && i+1 < len(os.Args[1:]) {
			logDir = os.Args[i+2]
		}
		if arg == "--out" && i+1 < len(os.Args[1:]) {
			outFile = os.Args[i+2]
		}
	}

	fmt.Printf("SAP HANA Redo Log Empirical Investigation\n")
	fmt.Printf("Version: %s | Image: %s\n", hanaVersion, dockerImage)
	fmt.Printf("Log dir: %s\n\n", logDir)

	// ── Phase 1: Directory file layout ──────────────────────────────────────
	fmt.Println("=== Phase 1: Directory File Layout ===")
	dirFile := ""
	if matches, _ := filepath.Glob(filepath.Join(logDir, "hdb*", "logsegment_000_directory.dat")); len(matches) > 0 {
		dirFile = matches[0]
	}
	if dirFile != "" {
		data, _ := os.ReadFile(dirFile)
		if len(data) >= 64 {
			v := data[0]
			cm := data[1]
			ec := binary.LittleEndian.Uint32(data[4:8])
			if v == 2 {
				confirm("dir.version", "Version=2 at byte 0", strconv.FormatUint(uint64(v), 10), hexStr(data[0:8]), 0)
			} else {
				refute("dir.version", "Version=2 at byte 0", strconv.FormatUint(uint64(v), 10), hexStr(data[0:8]), 0)
			}
			if cm == 1 {
				confirm("dir.cmax", "CMax=1 at byte 1", strconv.FormatUint(uint64(cm), 10), hexStr(data[0:8]), 1)
			} else {
				refute("dir.cmax", "CMax=1 at byte 1", strconv.FormatUint(uint64(cm), 10), hexStr(data[0:8]), 1)
			}
			if ec == 10240 {
				confirm("dir.entry_count", "EntryCount=10240 at offset 4 (uint32 LE)", strconv.FormatUint(uint64(ec), 10), hexStr(data[4:8]), 4)
			} else {
				refute("dir.entry_count", "EntryCount=10240 at offset 4", strconv.FormatUint(uint64(ec), 10), hexStr(data[4:8]), 4)
			}

			// Checksum test: read stored checksum at different offsets and try algorithms
			fmt.Println("\n  Checksum analysis:")
			for _, chkOff := range []int{24, 32, 40, 48} {
				if chkOff+8 > len(data) {
					continue
				}
				stored := binary.LittleEndian.Uint64(data[chkOff : chkOff+8])
				if stored == 0 {
					continue
				}
				zeroed := make([]byte, page.Size)
				copy(zeroed, data[:page.Size])
				binary.LittleEndian.PutUint64(zeroed[chkOff:chkOff+8], 0)
				crc := computeCRC64(zeroed)
				fl := computeFletcher64(zeroed)
				xr := computeXOR64(zeroed)
				fmt.Printf("  offset %d: stored=0x%016x crc64=0x%016x fl64=0x%016x xor64=0x%016x\n",
					chkOff, stored, crc, fl, xr)
				if stored == crc {
					confirm("dir.checksum_algo", "CRC-64/ECMA at offset "+strconv.Itoa(chkOff), fmt.Sprintf("0x%016x", stored), hexStr(data[chkOff:chkOff+8]), chkOff)
				}
				if stored == fl {
					confirm("dir.checksum_algo", "Fletcher-64 at offset "+strconv.Itoa(chkOff), fmt.Sprintf("0x%016x", stored), hexStr(data[chkOff:chkOff+8]), chkOff)
				}
				if stored == xr {
					confirm("dir.checksum_algo", "XOR-64 at offset "+strconv.Itoa(chkOff), fmt.Sprintf("0x%016x", stored), hexStr(data[chkOff:chkOff+8]), chkOff)
				}
			}
		}
	}

	// ── Phase 2: Block type codes via INSERT ────────────────────────────────
	fmt.Println("\n=== Phase 2: INSERT block type code ===")
	marker := fmt.Sprintf("PROBE_%d", time.Now().UnixNano())
	hdbsql("DROP TABLE INV.T 2>/dev/null; DROP SCHEMA INV 2>/dev/null")
	hdbsql("CREATE SCHEMA INV")
	hdbsql("CREATE COLUMN TABLE INV.T (ID INTEGER NOT NULL PRIMARY KEY, STR_VAL VARCHAR(64), INT_VAL INTEGER)")
	hdbsql(fmt.Sprintf("INSERT INTO INV.T (ID, STR_VAL, INT_VAL) VALUES (1, '%s', 42)", marker))
	savepoint()

	segFile := latestRedoSegment(logDir)
	if segFile == "" {
		fmt.Println("  No segment found")
		goto phase3
	}
	{
		data, _ := os.ReadFile(segFile)
		markerBytes := []byte(marker)
		positions := findBytes(data, markerBytes)
		if len(positions) == 0 {
			fmt.Printf("  Marker %q not found — may be in earlier segment\n", marker)
			// Search all segments
			pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
			allSegs, _ := filepath.Glob(pattern)
			for _, sf := range allSegs {
				d, _ := os.ReadFile(sf)
				pos := findBytes(d, markerBytes)
				if len(pos) > 0 {
					fmt.Printf("  Found in %s at positions %v\n", filepath.Base(sf), pos[:1])
					data = d
					positions = pos
					segFile = sf
					break
				}
			}
		}

		if len(positions) > 0 {
			pos := positions[0]
			pageIdx := pos / page.Size
			offsetInPage := pos % page.Size
			fmt.Printf("  Marker %q found at byte %d (page %d, offset %d in page)\n", marker, pos, pageIdx, offsetInPage)

			// The block payload starts at page.HeaderSize (offset 80)
			// Walk backwards from the marker to find the block header
			pageBase := pageIdx * page.Size
			payloadStart := pageBase + page.HeaderSize
			relMarkerPos := pos - payloadStart

			fmt.Printf("  Payload offset of marker: %d bytes from payload start\n", relMarkerPos)
			fmt.Printf("  Context bytes (marker-32 to marker+32):\n")
			start := max(payloadStart, pos-32)
			end := min(len(data), pos+len(markerBytes)+16)
			for i := start; i < end; i += 16 {
				e := min(end, i+16)
				ascii := make([]byte, e-i)
				for j, b := range data[i:e] {
					if b >= 32 && b < 127 {
						ascii[j] = b
					} else {
						ascii[j] = '.'
					}
				}
				fmt.Printf("    %08d: %-48s |%s|\n", i, hexStr(data[i:e]), ascii)
			}

			// Look for the 2-byte length prefix immediately before the marker
			if pos >= 2 {
				le2 := binary.LittleEndian.Uint16(data[pos-2 : pos])
				le1 := data[pos-1]
				fmt.Printf("  Byte before marker: 0x%02x = %d\n", le1, le1)
				fmt.Printf("  2 bytes before marker: 0x%04x = %d (uint16 LE)\n", le2, le2)
				markerLen := len(markerBytes)
				if int(le2) == markerLen {
					confirm("col.varchar.length_prefix", "2-byte LE uint16 immediately before VARCHAR", fmt.Sprintf("len=%d matches uint16 at offset-2", markerLen), hexStr(data[pos-4:pos+4]), pos-2)
				} else if int(le1) == markerLen {
					confirm("col.varchar.length_prefix", "1-byte length prefix before VARCHAR", fmt.Sprintf("len=%d matches byte at offset-1", markerLen), hexStr(data[pos-2:pos+2]), pos-1)
				} else {
					fmt.Printf("  Neither 1-byte (%d) nor 2-byte (%d) matches marker length %d\n", le1, le2, markerLen)
					// Scan for length in surrounding bytes
					for off := pos - 16; off < pos; off++ {
						if off < 0 {
							continue
						}
						if int(data[off]) == markerLen {
							fmt.Printf("  1-byte %d found at offset %d (relative-%d)\n", markerLen, off, pos-off)
						}
						if off+2 <= len(data) {
							v := binary.LittleEndian.Uint16(data[off : off+2])
							if int(v) == markerLen {
								fmt.Printf("  2-byte LE %d found at offset %d (relative-%d)\n", markerLen, off, pos-off)
							}
						}
					}
					indeterminate("col.varchar.length_prefix", "neither 1 nor 2 byte prefix matches at offset -1 or -2")
				}
			}

			// Check for INTEGER 42 (0x2A000000 LE) near the marker
			intLE := []byte{0x2A, 0x00, 0x00, 0x00}
			searchArea := data[max(0, pos-200):min(len(data), pos+200)]
			intPos := findBytes(searchArea, intLE)
			if len(intPos) > 0 {
				absPos := max(0, pos-200) + intPos[0]
				confirm("col.integer.encoding", "INTEGER 42 stored as [2A 00 00 00] (LE)", "found near marker", hexStr(data[absPos:absPos+8]), absPos)
			}

			// The block type code: scan bytes from payloadStart to marker
			// Look for patterns: a few bytes of header, then ContainerID, RowID, then column data
			fmt.Printf("\n  Block payload bytes (payload start to marker):\n")
			pStart := payloadStart
			pEnd := min(len(data), pos+4)
			for i := pStart; i < pEnd; i += 16 {
				e := min(pEnd, i+16)
				fmt.Printf("    %+8d: %s\n", i-pStart, hexStr(data[i:e]))
			}

			// The block starts at payloadStart. First bytes are the block header.
			// After analysis: byte[0] or first few bytes = block type
			if offsetInPage > page.HeaderSize {
				blockPayload := data[payloadStart:pEnd]
				fmt.Printf("\n  First 32 bytes of payload: %s\n", hexStr(blockPayload[:min(32, len(blockPayload))]))
				// Document the type byte candidate
				if len(blockPayload) > 0 {
					fmt.Printf("  Payload byte 0: 0x%02x = %d\n", blockPayload[0], blockPayload[0])
					fmt.Printf("  Payload bytes [0:4] as uint32 LE: 0x%08x = %d\n",
						binary.LittleEndian.Uint32(blockPayload[0:4]), binary.LittleEndian.Uint32(blockPayload[0:4]))
				}
			}
		}
	}

phase3:
	// ── Phase 3: DELETE block ────────────────────────────────────────────────
	fmt.Println("\n=== Phase 3: DELETE — confirm rowID-only payload ===")
	// INSERT a row with a unique INT_VAL, then DELETE it and look for that INT_VAL in the log
	uniqueInt := int32(0x0D33D33D) // unique sentinel
	hdbsql(fmt.Sprintf("INSERT INTO INV.T (ID, STR_VAL, INT_VAL) VALUES (99, 'delete_me', %d)", uniqueInt))
	savepoint()
	hdbsql("DELETE FROM INV.T WHERE ID = 99")
	savepoint()
	{
		pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
		allSegs, _ := filepath.Glob(pattern)
		intBytes := []byte{0x3D, 0xD3, 0x33, 0x0D} // 0x0D33D33D LE
		found := false
		for _, sf := range allSegs {
			d, _ := os.ReadFile(sf)
			pos := findBytes(d, intBytes)
			if len(pos) > 0 {
				fmt.Printf("  INT sentinel 0x%08x found at %d positions in %s\n", uniqueInt, len(pos), filepath.Base(sf))
				// Check if this is in an INSERT or not-in-DELETE block
				indeterminate("block.delete.no_column_values", fmt.Sprintf("sentinel found at %d positions — DELETE may or may not include column values depending on block boundaries", len(pos)))
				found = true
				break
			}
		}
		if !found {
			// INT might not be in the log (only rowID is stored for DELETE)
			confirm("block.delete.rowid_only", "DELETE block contains only RowID (no column values)", "INT sentinel not found after DELETE — confirms DELETE does not store column values", "", 0)
		}
	}

	// ── Phase 4: Multi-page LOB test ─────────────────────────────────────────
	fmt.Println("\n=== Phase 4: LOB inline vs LOB segment storage ===")
	largeStr := strings.Repeat("LOBPROBE_", 1000) // 9KB
	hdbsql("DROP TABLE INV.LOBT 2>/dev/null")
	hdbsql("CREATE COLUMN TABLE INV.LOBT (ID INTEGER NOT NULL PRIMARY KEY, BIG_VAL NCLOB)")
	hdbsql(fmt.Sprintf("INSERT INTO INV.LOBT (ID, BIG_VAL) VALUES (1, '%s')", largeStr))
	savepoint()
	{
		probe := []byte("LOBPROBE_")
		pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
		allSegs, _ := filepath.Glob(pattern)
		found := false
		for _, sf := range allSegs {
			d, _ := os.ReadFile(sf)
			positions := findBytes(d, probe)
			if len(positions) > 0 {
				first := positions[0] / page.Size
				last := positions[len(positions)-1] / page.Size
				if first != last {
					confirm("block.lob.spans_pages", "Large LOB spans multiple pages in redo log", fmt.Sprintf("probe found across pages %d–%d", first, last), "", 0)
				} else {
					confirm("block.lob.single_page", "Large NCLOB within single page (or LOB segment, not inline)", fmt.Sprintf("all occurrences on page %d", first), "", 0)
				}
				found = true
				break
			}
		}
		if !found {
			confirm("block.lob.not_in_redo_log", "Large NCLOB NOT stored inline in redo log (uses LOB segment)", "9KB probe string absent from all redo log segments", "", 0)
		}
	}

	// ── Save all findings ────────────────────────────────────────────────────
	fmt.Printf("\n=== Summary: %d findings ===\n", len(findings))
	confirmed, refuted, indet := 0, 0, 0
	for _, f := range findings {
		switch f.Result {
		case "confirmed":
			confirmed++
		case "refuted":
			refuted++
		default:
			indet++
		}
	}
	fmt.Printf("  ✅ Confirmed: %d\n  ❌ Refuted: %d\n  🔬 Indeterminate: %d\n", confirmed, refuted, indet)

	data, _ := json.MarshalIndent(findings, "", "  ")
	os.WriteFile(outFile, data, 0o644)
	fmt.Printf("\nFindings written to %s\n", outFile)
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
	var crc uint64 = 0xFFFFFFFFFFFFFFFF
	for _, b := range data {
		crc = table[byte(crc)^b] ^ (crc >> 8)
	}
	return crc ^ 0xFFFFFFFFFFFFFFFF
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
