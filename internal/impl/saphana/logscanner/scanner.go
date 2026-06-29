// Package logscanner provides tooling to read and hexdump raw SAP HANA
// redo log segment files. This is a RESEARCH TOOL for binary format
// reverse-engineering — see ../logformat/doc/ for the known format specification.
//
// The block type numeric codes are currently UNKNOWN. This scanner reads
// 4 KB pages verbatim so you can observe real data patterns after performing
// known DML operations with the dev script (../scripts/dev.sh).
package logscanner

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// PageSize is the fixed redo log page size (4096 bytes).
// Source: rtdi.io Transaction Log Reader analysis.
const PageSize = 4096

// ArchiveHeaderSize is the extra prefix on archive (backup) log files.
// Source: rtdi.io — "An archive log is a transaction log with an additional
// 4KB of data at the beginning."
const ArchiveHeaderSize = 4096

// Options controls scanner behaviour.
type Options struct {
	// SkipArchiveHeader skips the leading ArchiveHeaderSize bytes when true.
	SkipArchiveHeader bool
	// MaxPages limits pages read (0 = unlimited).
	MaxPages int
}

// Page holds the raw bytes of one 4 KB page plus position metadata.
type Page struct {
	PageIndex int    // 0-based index in the segment file
	Offset    int64  // byte offset within the file
	Raw       []byte // up to PageSize bytes
	IsFull    bool   // true if exactly PageSize bytes were read
}

// LittleEndianU32 reads a 4-byte little-endian uint32 at offset.
// Returns 0 if offset is out of bounds.
func (p *Page) LittleEndianU32(offset int) uint32 {
	if offset+4 > len(p.Raw) {
		return 0
	}
	return binary.LittleEndian.Uint32(p.Raw[offset:])
}

// LittleEndianU64 reads an 8-byte little-endian uint64 at offset.
func (p *Page) LittleEndianU64(offset int) uint64 {
	if offset+8 > len(p.Raw) {
		return 0
	}
	return binary.LittleEndian.Uint64(p.Raw[offset:])
}

// Scanner reads raw redo log pages from an io.Reader.
type Scanner struct {
	r    io.Reader
	opts Options
}

// NewScanner creates a Scanner over the given reader.
func NewScanner(r io.Reader, opts Options) *Scanner {
	return &Scanner{r: r, opts: opts}
}

// ScanAll reads all pages and returns them in order.
func (s *Scanner) ScanAll() ([]Page, error) {
	var startOffset int64
	if s.opts.SkipArchiveHeader {
		skip := make([]byte, ArchiveHeaderSize)
		n, err := io.ReadFull(s.r, skip)
		if n == 0 {
			return nil, nil
		}
		if err != nil && err != io.ErrUnexpectedEOF {
			return nil, fmt.Errorf("skipping archive header: %w", err)
		}
		startOffset = int64(n)
	}

	var pages []Page
	offset := startOffset
	for i := 0; s.opts.MaxPages == 0 || i < s.opts.MaxPages; i++ {
		buf := make([]byte, PageSize)
		n, err := io.ReadFull(s.r, buf)
		if n > 0 {
			pages = append(pages, Page{
				PageIndex: i,
				Offset:    offset,
				Raw:       buf[:n],
				IsFull:    n == PageSize,
			})
			offset += int64(n)
		}
		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break
			}
			return pages, fmt.Errorf("reading page %d at offset %d: %w", i, offset, err)
		}
	}
	return pages, nil
}

// DetectArchiveLog reads up to PageSize bytes and attempts to heuristically
// determine if this is an archive log file.
// NOTE: Without a confirmed archive log header magic-byte sequence, this
// returns (false, 0) until the format is confirmed via live HANA inspection.
func DetectArchiveLog(_ io.Reader) (isArchive bool, headerSize int) {
	// UNKNOWN: No public documentation of the archive log header magic bytes.
	// To determine: compare a known online segment with its archive backup via
	// hexdump; the first 4096 bytes of the archive file are the extra header.
	return false, 0
}

// HexDumpPage formats a page's raw bytes as a canonical hex dump for analysis.
func HexDumpPage(data []byte, pageIndex int) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "=== Page %d (%d bytes) ===\n", pageIndex, len(data))
	for i := 0; i < len(data); i += 16 {
		end := min(i+16, len(data))
		chunk := data[i:end]
		hexBytes := hex.EncodeToString(chunk)
		var spaced strings.Builder
		for j := 0; j < len(hexBytes); j += 2 {
			if j > 0 {
				spaced.WriteByte(' ')
			}
			spaced.WriteString(hexBytes[j : j+2])
		}
		var ascii strings.Builder
		for _, b := range chunk {
			if b >= 32 && b < 127 {
				ascii.WriteByte(b)
			} else {
				ascii.WriteByte('.')
			}
		}
		fmt.Fprintf(&sb, "%08x  %-47s  |%s|\n", i, spaced.String(), ascii.String())
	}
	return sb.String()
}
