//go:build ignore

// biased_workload.go — systematic log format analysis using biased (known-content) workloads.
//
// Strategy (per user guidance 2026-06-08):
//  1. Insert N rows with a known sentinel value (e.g. 0xDEADBEEF)
//  2. Measure actual bytes consumed in redo log
//  3. Known data bytes = N × (sum of column sizes)
//  4. Overhead = total_bytes - known_data_bytes
//  5. Overhead / N = per-record overhead (block header + RowID + delimiters)
//  6. Vary N to confirm linearity (proves the per-row overhead model)
//  7. Add/remove columns and measure deltas to isolate per-column overhead
//  8. Vary column ORDER to detect ordering effects on metadata
//
// This approach builds up from known inputs, giving precise measurements instead of
// guessing from inspecting individual byte sequences.
//
// Run:
//
//	go run ./internal/impl/saphana/logformat/investigate/biased_workload.go \
//	  --log-dir /tmp/hana-express-data/log/mnt00001
package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logformat/page"
)

// Measurement records bytes used in the log for one experiment.
type Measurement struct {
	Label        string  `json:"label"`
	HANAVersion  string  `json:"hana_version"`
	RowCount     int     `json:"row_count"`
	KnownDataB   int     `json:"known_data_bytes"` // bytes we know we wrote (column values)
	TotalLogB    int     `json:"total_log_bytes"`  // actual bytes used in log
	OverheadB    int     `json:"overhead_bytes"`   // TotalLogB - KnownDataB
	PerRowOverB  float64 `json:"per_row_overhead"` // OverheadB / RowCount
	ColumnSchema string  `json:"column_schema"`
	Notes        string  `json:"notes"`
}

var measurements []Measurement

func hdbsql(sql string) string {
	cmd := exec.Command("docker", "exec", "hxe",
		"/usr/sap/HXE/HDB90/exe/hdbsql",
		"-i", "90", "-u", "SYSTEM", "-p", "HXEHana1", "-d", "SYSTEMDB",
		"-a", sql)
	cmd.Env = append(os.Environ(), "DOCKER_API_VERSION=1.46")
	out, _ := cmd.CombinedOutput()
	return strings.TrimSpace(string(out))
}

func savepoint() { hdbsql("ALTER SYSTEM SAVEPOINT"); time.Sleep(2 * time.Second) }

// bytesUsedInLog returns total payload bytes written to redo log segments
// since the given min LSN watermark.
func bytesUsedInLog(logDir string, sinceMaxLSN uint64) int {
	pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
	files, _ := filepath.Glob(pattern)

	total := 0
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		for i := range len(data) / page.Size {
			base := i * page.Size
			if binary.LittleEndian.Uint64(data[base:base+8]) != page.Magic {
				continue
			}
			lsn := binary.LittleEndian.Uint64(data[base+16 : base+24])
			if lsn <= sinceMaxLSN {
				continue
			}
			used := int(binary.LittleEndian.Uint32(data[base+76 : base+80]))
			if used > 0 && used <= page.Size-page.HeaderSize {
				total += used
			}
		}
	}
	return total
}

func currentMaxLSN(logDir string) uint64 {
	pattern := filepath.Join(logDir, "hdb*", "logsegment_000_0*.dat")
	files, _ := filepath.Glob(pattern)
	var maxLSN uint64
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		for i := range len(data) / page.Size {
			base := i * page.Size
			if binary.LittleEndian.Uint64(data[base:base+8]) != page.Magic {
				continue
			}
			lsn := binary.LittleEndian.Uint64(data[base+16 : base+24])
			if lsn > maxLSN {
				maxLSN = lsn
			}
		}
	}
	return maxLSN
}

func insertN(schema, table string, rows int, idOffset int, sentinel uint32) {
	// Batch insert in chunks of 500 to avoid SQL length limits
	const batch = 500
	for start := 0; start < rows; start += batch {
		end := start + batch
		if end > rows {
			end = rows
		}
		var vals []string
		for i := start; i < end; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d)", idOffset+i, sentinel))
		}
		hdbsql(fmt.Sprintf("INSERT INTO %s.%s (ID, VAL) VALUES %s",
			schema, table, strings.Join(vals, ",")))
	}
}

func insertNWithString(schema, table string, rows int, idOffset int, sentinel uint32, str string) {
	const batch = 200
	for start := 0; start < rows; start += batch {
		end := start + batch
		if end > rows {
			end = rows
		}
		var vals []string
		for i := start; i < end; i++ {
			vals = append(vals, fmt.Sprintf("(%d, %d, '%s')", idOffset+i, sentinel, str))
		}
		hdbsql(fmt.Sprintf("INSERT INTO %s.%s (ID, VAL, STR) VALUES %s",
			schema, table, strings.Join(vals, ",")))
	}
}

func experimentUnused(label, logDir2, schema, table, createSQL2 string, insertFn func() int, knownBytesPerRow int, rowCount int) {
	// unused but kept for reference
	hdbsql(fmt.Sprintf("DROP TABLE %s.%s", schema, table))
	hdbsql(fmt.Sprintf("DROP SCHEMA %s", schema))
	hdbsql(fmt.Sprintf("CREATE SCHEMA %s", schema))
	hdbsql(createSQL2)
	savepoint()

	watermark := currentMaxLSN(logDir2)
	n := insertFn()
	savepoint()

	totalBytes := bytesUsedInLog(logDir2, watermark)
	knownBytes := knownBytesPerRow * rowCount
	overhead := totalBytes - knownBytes
	perRow := float64(overhead) / float64(n)

	m := Measurement{
		Label:        label,
		HANAVersion:  "2.00.088.00.1760424921",
		RowCount:     n,
		KnownDataB:   knownBytes,
		TotalLogB:    totalBytes,
		OverheadB:    overhead,
		PerRowOverB:  perRow,
		ColumnSchema: createSQL2,
	}
	measurements = append(measurements, m)
	fmt.Printf("  %-40s rows=%d known=%d total=%d overhead=%d per-row=%.1f\n",
		label, n, knownBytes, totalBytes, overhead, perRow)
}

func main() {
	logDir := "/tmp/hana-express-data/log/mnt00001"
	for i, arg := range os.Args[1:] {
		if arg == "--log-dir" && i+1 < len(os.Args[1:]) {
			logDir = os.Args[i+2]
		}
	}

	const SENTINEL = uint32(0xDEADBEEF) // 3735928559 = [EF BE AD DE] LE

	fmt.Println("=== Biased Workload Analysis ===")
	s := uint32(SENTINEL)
	fmt.Printf("Sentinel: 0x%08X = %d = [%02x %02x %02x %02x] LE\n\n",
		s, s, s&0xFF, (s>>8)&0xFF, (s>>16)&0xFF, (s>>24)&0xFF)

	// ── Experiment A: Single INTEGER column, varying N ──────────────────────
	fmt.Println("--- A: Single INTEGER column (vary N to confirm linearity) ---")
	for _, n := range []int{100, 500, 1000, 2000} {
		schema := fmt.Sprintf("BW%d", n)
		hdbsql("DROP TABLE " + schema + ".T 2>/dev/null; DROP SCHEMA " + schema + " 2>/dev/null")
		hdbsql("CREATE SCHEMA " + schema)
		hdbsql("CREATE COLUMN TABLE " + schema + ".T (ID INTEGER NOT NULL PRIMARY KEY, VAL INTEGER)")
		savepoint()
		wm := currentMaxLSN(logDir)
		insertN(schema, "T", n, 0, SENTINEL)
		savepoint()
		total := bytesUsedInLog(logDir, wm)
		known := n * 4 // VAL is 4 bytes; ID is also 4 bytes = 8 bytes known data per row
		knownFull := n * 8
		overhead := total - knownFull
		fmt.Printf("  N=%-5d total=%6d known(VAL only)=%5d known(ID+VAL)=%5d overhead=%5d per-row=%.2f\n",
			n, total, known, knownFull, overhead, float64(overhead)/float64(n))
		measurements = append(measurements, Measurement{
			Label:        fmt.Sprintf("single_int_N=%d", n),
			HANAVersion:  "2.00.088.00.1760424921",
			RowCount:     n,
			KnownDataB:   knownFull,
			TotalLogB:    total,
			OverheadB:    overhead,
			PerRowOverB:  float64(overhead) / float64(n),
			ColumnSchema: "ID INTEGER PK, VAL INTEGER",
		})
	}

	// ── Experiment B: Add VARCHAR column, measure delta ──────────────────────
	fmt.Println("\n--- B: Two columns: INTEGER + VARCHAR('DEADBEEF' = 8 chars) ---")
	{
		str := "DEADBEEF" // 8 bytes UTF-8
		for _, n := range []int{500, 1000} {
			schema := fmt.Sprintf("BW2C%d", n)
			hdbsql("DROP TABLE " + schema + ".T 2>/dev/null; DROP SCHEMA " + schema + " 2>/dev/null")
			hdbsql("CREATE SCHEMA " + schema)
			hdbsql("CREATE COLUMN TABLE " + schema + ".T (ID INTEGER NOT NULL PRIMARY KEY, VAL INTEGER, STR VARCHAR(32))")
			savepoint()
			wm := currentMaxLSN(logDir)
			insertNWithString(schema, "T", n, 0, SENTINEL, str)
			savepoint()
			total := bytesUsedInLog(logDir, wm)
			// known: ID(4) + VAL(4) + VARCHAR prefix(1) + string bytes(8) = 17 bytes per row
			known := n * (4 + 4 + 1 + len(str))
			overhead := total - known
			fmt.Printf("  N=%-5d total=%6d known=%5d overhead=%5d per-row=%.2f\n",
				n, total, known, overhead, float64(overhead)/float64(n))
			measurements = append(measurements, Measurement{
				Label:        fmt.Sprintf("int_varchar_N=%d", n),
				HANAVersion:  "2.00.088.00.1760424921",
				RowCount:     n,
				KnownDataB:   known,
				TotalLogB:    total,
				OverheadB:    overhead,
				PerRowOverB:  float64(overhead) / float64(n),
				ColumnSchema: "ID INTEGER PK, VAL INTEGER, STR VARCHAR(32)",
				Notes:        fmt.Sprintf("str='%s' (%d bytes)", str, len(str)),
			})
		}
	}

	// ── Experiment C: BIGINT instead of INTEGER for VAL ──────────────────────
	fmt.Println("\n--- C: BIGINT column (8 bytes) vs INTEGER (4 bytes) ---")
	{
		n := 1000
		schema := "BWBIG"
		hdbsql("DROP TABLE " + schema + ".T 2>/dev/null; DROP SCHEMA " + schema + " 2>/dev/null")
		hdbsql("CREATE SCHEMA " + schema)
		hdbsql("CREATE COLUMN TABLE " + schema + ".T (ID INTEGER NOT NULL PRIMARY KEY, BIGVAL BIGINT)")
		savepoint()
		wm := currentMaxLSN(logDir)
		// Insert BIGINT sentinel: 0xDEADBEEFDEADBEEF
		bigSentinel := int64(-2401053088876216593) // 0xDEADBEEFDEADBEEF signed
		const batchBig = 500
		for start := 0; start < n; start += batchBig {
			end := start + batchBig
			if end > n {
				end = n
			}
			var vals []string
			for i := start; i < end; i++ {
				vals = append(vals, fmt.Sprintf("(%d, %d)", i, bigSentinel))
			}
			hdbsql(fmt.Sprintf("INSERT INTO %s.T (ID, BIGVAL) VALUES %s", schema, strings.Join(vals, ",")))
		}
		savepoint()
		total := bytesUsedInLog(logDir, wm)
		known := n * (4 + 8) // ID(4) + BIGINT(8)
		overhead := total - known
		fmt.Printf("  N=%-5d total=%6d known=%5d overhead=%5d per-row=%.2f\n",
			n, total, known, overhead, float64(overhead)/float64(n))
		measurements = append(measurements, Measurement{
			Label:        "bigint_N=1000",
			HANAVersion:  "2.00.088.00.1760424921",
			RowCount:     n,
			KnownDataB:   known,
			TotalLogB:    total,
			OverheadB:    overhead,
			PerRowOverB:  float64(overhead) / float64(n),
			ColumnSchema: "ID INTEGER PK, BIGVAL BIGINT",
		})
	}

	// ── Experiment D: Column order variation ─────────────────────────────────
	fmt.Println("\n--- D: Column ORDER variation (does order affect overhead?) ---")
	str := "DEADBEEF"
	knownPerRow := 4 + 4 + 1 + len(str) // ID + INT + prefix + str
	for i, createSQL := range []string{
		"CREATE COLUMN TABLE BWCO1.T (ID INTEGER NOT NULL PRIMARY KEY, VAL INTEGER, STR VARCHAR(32))",
		"CREATE COLUMN TABLE BWCO2.T (ID INTEGER NOT NULL PRIMARY KEY, STR VARCHAR(32), VAL INTEGER)",
	} {
		schema := fmt.Sprintf("BWCO%d", i+1)
		hdbsql("DROP TABLE " + schema + ".T 2>/dev/null; DROP SCHEMA " + schema + " 2>/dev/null")
		hdbsql("CREATE SCHEMA " + schema)
		hdbsql(createSQL)
		savepoint()
		n := 500
		wm := currentMaxLSN(logDir)
		insertNWithString(schema, "T", n, 0, SENTINEL, str)
		savepoint()
		total := bytesUsedInLog(logDir, wm)
		overhead := total - knownPerRow*n
		fmt.Printf("  Schema %d: total=%6d overhead=%5d per-row=%.2f  [%s]\n",
			i+1, total, overhead, float64(overhead)/float64(n),
			strings.ReplaceAll(createSQL, "CREATE COLUMN TABLE "+schema+".T ", ""))
		measurements = append(measurements, Measurement{
			Label:        fmt.Sprintf("col_order_%d_N=500", i+1),
			HANAVersion:  "2.00.088.00.1760424921",
			RowCount:     n,
			KnownDataB:   knownPerRow * n,
			TotalLogB:    total,
			OverheadB:    overhead,
			PerRowOverB:  float64(overhead) / float64(n),
			ColumnSchema: createSQL,
		})
	}

	// ── Analysis: derive per-row overhead ────────────────────────────────────
	fmt.Println("\n=== Analysis ===")
	var singleIntOverheads []float64
	for _, m := range measurements {
		if strings.HasPrefix(m.Label, "single_int") {
			singleIntOverheads = append(singleIntOverheads, m.PerRowOverB)
		}
	}
	if len(singleIntOverheads) > 0 {
		sum := 0.0
		for _, v := range singleIntOverheads {
			sum += v
		}
		avg := sum / float64(len(singleIntOverheads))
		// Compute stddev
		var varSum float64
		for _, v := range singleIntOverheads {
			varSum += (v - avg) * (v - avg)
		}
		std := math.Sqrt(varSum / float64(len(singleIntOverheads)))
		fmt.Printf("  Per-row overhead (single INT col): avg=%.2f bytes stddev=%.2f\n", avg, std)
		fmt.Printf("  This overhead = block header + RowID + column descriptors + padding\n")
		fmt.Printf("  Known: RowID = 8 bytes, so remaining overhead = %.2f bytes\n", avg-8)
	}

	// Check column order effect
	var co1, co2 float64
	for _, m := range measurements {
		if m.Label == "col_order_1_N=500" {
			co1 = m.PerRowOverB
		}
		if m.Label == "col_order_2_N=500" {
			co2 = m.PerRowOverB
		}
	}
	if co1 > 0 && co2 > 0 {
		fmt.Printf("  Column order effect: schema1=%.2f schema2=%.2f diff=%.2f bytes per row\n",
			co1, co2, math.Abs(co1-co2))
		if math.Abs(co1-co2) < 1.0 {
			fmt.Println("  → Column ORDER has NO effect on per-row overhead (confirmed)")
		} else {
			fmt.Printf("  → Column order CHANGES overhead by %.2f bytes per row\n", math.Abs(co1-co2))
		}
	}

	// Sort and print
	sort.Slice(measurements, func(i, j int) bool { return measurements[i].Label < measurements[j].Label })
	fmt.Printf("\n%-40s %6s %8s %8s %8s %8s\n",
		"EXPERIMENT", "ROWS", "KNOWN_B", "TOTAL_B", "OVER_B", "PER_ROW")
	fmt.Println(strings.Repeat("-", 95))
	for _, m := range measurements {
		fmt.Printf("%-40s %6d %8d %8d %8d %8.1f\n",
			m.Label, m.RowCount, m.KnownDataB, m.TotalLogB, m.OverheadB, m.PerRowOverB)
	}

	// Save
	data, _ := json.MarshalIndent(measurements, "", "  ")
	os.WriteFile("/tmp/biased-workload-results.json", data, 0644)
	fmt.Printf("\nResults written to /tmp/biased-workload-results.json\n")
}
