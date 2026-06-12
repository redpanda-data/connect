//go:build ignore

// hexdump_main.go — hex-dump raw SAP HANA redo log pages for format analysis.
//
// Run: go run hexdump_main.go --dir=/tmp/hana-express-data/log/HXE/mnt00001 --pages=5
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logscanner"
)

func main() {
	dir := flag.String("dir", ".", "HANA log partition directory (parent of hdb* subdirs)")
	pages := flag.Int("pages", 5, "Pages to dump per segment")
	archive := flag.Bool("archive", false, "Skip 4KB archive header before pages")
	flag.Parse()

	pattern := filepath.Join(*dir, "hdb*", "logsegment_000_*.dat")
	matches, err := filepath.Glob(pattern)
	if err != nil || len(matches) == 0 {
		fmt.Fprintf(os.Stderr, "No log segments found at %s\n", pattern)
		os.Exit(1)
	}
	sort.Strings(matches)
	target := matches[len(matches)-1]
	fmt.Printf("Dumping: %s\n\n", target)

	f, err := os.Open(target)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Opening %s: %v\n", target, err)
		os.Exit(1)
	}
	defer f.Close()

	s := logscanner.NewScanner(f, logscanner.Options{
		MaxPages:          *pages,
		SkipArchiveHeader: *archive,
	})
	pgs, err := s.ScanAll()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Scanning: %v\n", err)
	}
	if len(pgs) == 0 {
		fmt.Println("(no pages found)")
		return
	}
	for _, pg := range pgs {
		fmt.Println(logscanner.HexDumpPage(pg.Raw, pg.PageIndex))
	}
	fmt.Printf("Total: %d page(s) dumped from %s\n", len(pgs), target)
}
