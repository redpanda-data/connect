package logscanner_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redpanda-data/connect/v4/internal/impl/saphana/logscanner"
)

func TestScannerPageSize(t *testing.T) {
	assert.Equal(t, 4096, logscanner.PageSize)
}

func TestScannerArchiveHeaderSize(t *testing.T) {
	assert.Equal(t, 4096, logscanner.ArchiveHeaderSize)
}

func TestScannerReadsTwoFullPages(t *testing.T) {
	data := make([]byte, 2*logscanner.PageSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	s := logscanner.NewScanner(bytes.NewReader(data), logscanner.Options{})
	pages, err := s.ScanAll()
	require.NoError(t, err)
	require.Len(t, pages, 2)
	assert.Equal(t, 0, pages[0].PageIndex)
	assert.Equal(t, int64(0), pages[0].Offset)
	assert.True(t, pages[0].IsFull)
	assert.Equal(t, 1, pages[1].PageIndex)
	assert.Equal(t, int64(logscanner.PageSize), pages[1].Offset)
	assert.True(t, pages[1].IsFull)
	assert.Len(t, pages[0].Raw, logscanner.PageSize)
}

func TestScannerHandlesEmptyReader(t *testing.T) {
	s := logscanner.NewScanner(bytes.NewReader(nil), logscanner.Options{})
	pages, err := s.ScanAll()
	require.NoError(t, err)
	assert.Empty(t, pages)
}

func TestScannerHandlesPartialPage(t *testing.T) {
	data := make([]byte, logscanner.PageSize+100)
	s := logscanner.NewScanner(bytes.NewReader(data), logscanner.Options{})
	pages, err := s.ScanAll()
	require.NoError(t, err)
	require.Len(t, pages, 2)
	assert.True(t, pages[0].IsFull)
	assert.False(t, pages[1].IsFull)
	assert.Len(t, pages[1].Raw, 100)
}

func TestScannerMaxPages(t *testing.T) {
	data := make([]byte, 5*logscanner.PageSize)
	s := logscanner.NewScanner(bytes.NewReader(data), logscanner.Options{MaxPages: 2})
	pages, err := s.ScanAll()
	require.NoError(t, err)
	assert.Len(t, pages, 2)
}

func TestScannerSkipsArchiveHeader(t *testing.T) {
	// Archive = 4KB header + 1 data page
	data := make([]byte, 2*logscanner.PageSize)
	// Mark header bytes distinctively
	for i := range logscanner.ArchiveHeaderSize {
		data[i] = 0xAB
	}
	// Mark first data page
	for i := logscanner.ArchiveHeaderSize; i < 2*logscanner.PageSize; i++ {
		data[i] = 0xCD
	}
	s := logscanner.NewScanner(bytes.NewReader(data), logscanner.Options{SkipArchiveHeader: true})
	pages, err := s.ScanAll()
	require.NoError(t, err)
	require.Len(t, pages, 1)
	// Offset should be after the header
	assert.Equal(t, int64(logscanner.ArchiveHeaderSize), pages[0].Offset)
	// Data should be the CD bytes, not AB
	assert.Equal(t, byte(0xCD), pages[0].Raw[0])
}

func TestScannerPageLittleEndianU32(t *testing.T) {
	raw := make([]byte, 8)
	raw[0], raw[1], raw[2], raw[3] = 0x01, 0x02, 0x03, 0x04
	p := logscanner.Page{Raw: raw}
	assert.Equal(t, uint32(0x04030201), p.LittleEndianU32(0))
}

func TestScannerPageLittleEndianU32OutOfBounds(t *testing.T) {
	p := logscanner.Page{Raw: make([]byte, 3)}
	assert.Equal(t, uint32(0), p.LittleEndianU32(0)) // needs 4 bytes, only 3 available
}

func TestScannerPageLittleEndianU64(t *testing.T) {
	raw := make([]byte, 8)
	raw[0] = 0xFF
	p := logscanner.Page{Raw: raw}
	assert.Equal(t, uint64(0xFF), p.LittleEndianU64(0))
}

func TestDetectArchiveLog(t *testing.T) {
	// Currently a stub — returns false, 0 until format is confirmed.
	isArchive, headerSize := logscanner.DetectArchiveLog(bytes.NewReader(make([]byte, logscanner.PageSize)))
	assert.False(t, isArchive, "stub should return false until format confirmed")
	assert.Equal(t, 0, headerSize)
}

func TestHexDumpPageContainsHeader(t *testing.T) {
	page := make([]byte, 64)
	for i := range page {
		page[i] = byte(i)
	}
	dump := logscanner.HexDumpPage(page, 3)
	assert.Contains(t, dump, "=== Page 3 (64 bytes) ===")
	assert.Contains(t, dump, "00000000")
	assert.Contains(t, dump, "00 01 02 03")
}

func TestHexDumpPagePrintableASCII(t *testing.T) {
	page := []byte("Hello, HANA!")
	dump := logscanner.HexDumpPage(page, 0)
	assert.Contains(t, dump, "Hello, HANA!")
}

func TestHexDumpPageNonPrintableShowsDot(t *testing.T) {
	page := []byte{0x00, 0x01, 0xFF}
	dump := logscanner.HexDumpPage(page, 0)
	assert.Contains(t, dump, "...")
}

func TestHexDumpEmptyPage(t *testing.T) {
	dump := logscanner.HexDumpPage(nil, 0)
	assert.Contains(t, dump, "=== Page 0 (0 bytes) ===")
}

func TestScannerPageIndexSequential(t *testing.T) {
	data := make([]byte, 4*logscanner.PageSize)
	s := logscanner.NewScanner(bytes.NewReader(data), logscanner.Options{})
	pages, err := s.ScanAll()
	require.NoError(t, err)
	for i, p := range pages {
		assert.Equal(t, i, p.PageIndex, "page index must be sequential")
		assert.Equal(t, int64(i*logscanner.PageSize), p.Offset)
	}
}

func TestHexDumpPageMultipleLines(t *testing.T) {
	page := make([]byte, 32) // 2 lines of 16
	dump := logscanner.HexDumpPage(page, 0)
	lines := strings.Split(strings.TrimSpace(dump), "\n")
	// Header + 2 data lines
	assert.GreaterOrEqual(t, len(lines), 3)
}
