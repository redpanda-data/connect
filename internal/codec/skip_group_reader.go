package codec

import (
	"io"
	"sort"
)

func skipBOM(r io.Reader) io.Reader {
	return skipGroup(r,
		[]byte{0x00, 0x00, 0xFE, 0xFF}, // UTF32BigEndianBOM4
		[]byte{0xFF, 0xFE, 0x00, 0x00}, // UTF32LittleEndianBOM4
		[]byte{0xEF, 0xBB, 0xBF},       // UTF8BOM3
		[]byte{0xFE, 0xFF},             // UTF16BigEndianBOM2
		[]byte{0xFF, 0xFE},             // UTF16LittleEndianBOM2
	)
}

func skipGroup(rd io.Reader, groups ...[]byte) io.Reader {
	if len(groups) == 0 {
		return rd
	}

	sort.Slice(groups, func(i, j int) bool {
		return len(groups[i]) > len(groups[j])
	})

	buf, err := readUpToMax(rd, len(groups[0]))

groupLoop:
	for _, g := range groups {
		if len(buf) < len(g) {
			continue
		}
		for i, b := range g {
			if buf[i] != b {
				continue groupLoop
			}
		}
		if buf = buf[len(g):]; len(buf) == 0 {
			buf = nil
		}
		break
	}

	return &bufPriorityReader{
		rd:  rd,
		buf: buf,
		err: err,
	}
}

func readUpToMax(r io.Reader, max int) (buf []byte, err error) {
	if max == 0 {
		return
	}

	buf = make([]byte, max)

	var readLen int
	for err == nil && readLen < max {
		var n int
		n, err = r.Read(buf[readLen:])
		readLen += n
	}
	buf = buf[:readLen]
	return
}

//------------------------------------------------------------------------------

// Reads from a buf and err as priority over the underlying io.Reader.
type bufPriorityReader struct {
	rd  io.Reader
	buf []byte
	err error
}

func (r *bufPriorityReader) Read(p []byte) (n int, err error) {
	if len(p) == 0 {
		return
	}

	if r.buf == nil {
		if err = r.err; err != nil {
			r.err = nil
			return
		}
		return r.rd.Read(p)
	}

	n = copy(p, r.buf)
	if r.buf = r.buf[n:]; len(r.buf) == 0 {
		r.buf = nil
	}
	return
}
