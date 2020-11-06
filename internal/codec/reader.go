package codec

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// ReaderDocs is a static field documentation for input codecs.
var ReaderDocs = docs.FieldCommon(
	"codec", "The way in which the bytes of consumed files are converted into messages, codecs are useful for specifying how large files might be processed in small chunks rather than loading it all in memory. It's possible to consume lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.", "lines", "delim:\t", "delim:foobar",
).HasAnnotatedOptions(
	"all-bytes", "Consume the entire file as a single binary message.",
	"lines", "Consume the file in segments divided by linebreaks.",
	"delim:x", "Consume the file in segments divided by a custom delimter.",
	"tar", "Parse the file as a tar archive, and consume each file of the archive as a message.",
	"tar-gzip", "Parse the file as a gzip compressed tar archive, and consume each file of the archive as a message.",
)

//------------------------------------------------------------------------------

// ReaderConfig is a general configuration struct that covers all reader codecs.
type ReaderConfig struct {
	MaxScanTokenSize int
}

// NewReaderConfig creates a reader configuration with default values.
func NewReaderConfig() ReaderConfig {
	return ReaderConfig{
		MaxScanTokenSize: bufio.MaxScanTokenSize,
	}
}

//------------------------------------------------------------------------------

// ReaderAckFn is a function provided to a reader codec that it should call once
// the underlying io.ReadCloser is fully consumed.
type ReaderAckFn func(context.Context, error) error

// Reader is a codec type that reads message parts from a source.
type Reader interface {
	Next(context.Context) (types.Part, ReaderAckFn, error)
	Close(context.Context) error
}

// ReaderConstructor creates a reader from an io.ReadCloser and an ack func
// which is called by the reader once the io.ReadCloser is finished with.
type ReaderConstructor func(io.ReadCloser, ReaderAckFn) (Reader, error)

// GetReader returns a constructor that creates reader codecs.
func GetReader(codec string, conf ReaderConfig) (ReaderConstructor, error) {
	switch codec {
	case "all-bytes":
		return func(r io.ReadCloser, fn ReaderAckFn) (Reader, error) {
			return &allBytesReader{r, fn, false}, nil
		}, nil
	case "lines":
		return func(r io.ReadCloser, fn ReaderAckFn) (Reader, error) {
			return newLinesReader(conf, r, fn)
		}, nil
	case "tar":
		return newTarReader, nil
	case "tar-gzip":
		return func(r io.ReadCloser, fn ReaderAckFn) (Reader, error) {
			g, err := gzip.NewReader(r)
			if err != nil {
				r.Close()
				return nil, err
			}
			return newTarReader(g, fn)
		}, nil
	}
	if strings.HasPrefix(codec, "delim:") {
		by := strings.TrimPrefix(codec, "delim:")
		if len(by) == 0 {
			return nil, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return func(r io.ReadCloser, fn ReaderAckFn) (Reader, error) {
			return newCustomDelimReader(conf, r, by, fn)
		}, nil
	}
	return nil, fmt.Errorf("codec was not recognised: %v", codec)
}

//------------------------------------------------------------------------------

type allBytesReader struct {
	i        io.ReadCloser
	ack      ReaderAckFn
	consumed bool
}

func (a *allBytesReader) Next(ctx context.Context) (types.Part, ReaderAckFn, error) {
	if a.consumed {
		return nil, nil, io.EOF
	}
	a.consumed = true
	b, err := ioutil.ReadAll(a.i)
	if err != nil {
		a.ack(ctx, err)
		return nil, nil, err
	}
	p := message.NewPart(b)
	return p, a.ack, nil
}

func (a *allBytesReader) Close(ctx context.Context) error {
	if !a.consumed {
		a.ack(ctx, errors.New("service shutting down"))
	}
	return a.i.Close()
}

//------------------------------------------------------------------------------

type linesReader struct {
	buf       *bufio.Scanner
	r         io.ReadCloser
	sourceAck ReaderAckFn

	mut      sync.Mutex
	finished bool
	total    int32
	pending  int32
}

func newLinesReader(conf ReaderConfig, r io.ReadCloser, ackFn ReaderAckFn) (Reader, error) {
	scanner := bufio.NewScanner(r)
	if conf.MaxScanTokenSize != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, conf.MaxScanTokenSize)
	}
	return &linesReader{
		buf:       scanner,
		r:         r,
		sourceAck: ackFn,
	}, nil
}

func (a *linesReader) ack(ctx context.Context, err error) error {
	a.mut.Lock()
	a.pending--
	doAck := a.pending == 0 && a.finished
	a.mut.Unlock()

	if err != nil {
		return a.sourceAck(ctx, err)
	}
	if doAck {
		return a.sourceAck(ctx, nil)
	}
	return nil
}

func (a *linesReader) Next(ctx context.Context) (types.Part, ReaderAckFn, error) {
	a.mut.Lock()
	defer a.mut.Unlock()

	if a.buf.Scan() {
		a.pending++
		a.total++
		return message.NewPart(a.buf.Bytes()), a.ack, nil
	}
	err := a.buf.Err()
	if err == nil {
		err = io.EOF
	}
	a.finished = true
	return nil, nil, err
}

func (a *linesReader) Close(ctx context.Context) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	if !a.finished {
		a.sourceAck(ctx, errors.New("service shutting down"))
	}
	if a.pending == 0 && a.total > 0 {
		a.sourceAck(ctx, nil)
	}
	return a.r.Close()
}

//------------------------------------------------------------------------------

type customDelimReader struct {
	buf       *bufio.Scanner
	r         io.ReadCloser
	sourceAck ReaderAckFn

	mut      sync.Mutex
	finished bool
	total    int32
	pending  int32
}

func newCustomDelimReader(conf ReaderConfig, r io.ReadCloser, delim string, ackFn ReaderAckFn) (Reader, error) {
	scanner := bufio.NewScanner(r)
	if conf.MaxScanTokenSize != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, conf.MaxScanTokenSize)
	}

	delimBytes := []byte(delim)

	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		if i := bytes.Index(data, delimBytes); i >= 0 {
			// We have a full terminated line.
			return i + len(delimBytes), data[0:i], nil
		}

		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}

		// Request more data.
		return 0, nil, nil
	})

	return &customDelimReader{
		buf:       scanner,
		r:         r,
		sourceAck: ackFn,
	}, nil
}

func (a *customDelimReader) ack(ctx context.Context, err error) error {
	a.mut.Lock()
	a.pending--
	doAck := a.pending == 0 && a.finished
	a.mut.Unlock()

	if err != nil {
		return a.sourceAck(ctx, err)
	}
	if doAck {
		return a.sourceAck(ctx, nil)
	}
	return nil
}

func (a *customDelimReader) Next(ctx context.Context) (types.Part, ReaderAckFn, error) {
	a.mut.Lock()
	defer a.mut.Unlock()

	if a.buf.Scan() {
		a.pending++
		a.total++
		return message.NewPart(a.buf.Bytes()), a.ack, nil
	}
	err := a.buf.Err()
	if err == nil {
		err = io.EOF
	}
	a.finished = true
	return nil, nil, err
}

func (a *customDelimReader) Close(ctx context.Context) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	if !a.finished {
		a.sourceAck(ctx, errors.New("service shutting down"))
	}
	if a.pending == 0 && a.total > 0 {
		a.sourceAck(ctx, nil)
	}
	return a.r.Close()
}

//------------------------------------------------------------------------------

type tarReader struct {
	buf       *tar.Reader
	r         io.ReadCloser
	sourceAck ReaderAckFn

	mut      sync.Mutex
	finished bool
	total    int32
	pending  int32
}

func newTarReader(r io.ReadCloser, ackFn ReaderAckFn) (Reader, error) {
	return &tarReader{
		buf:       tar.NewReader(r),
		r:         r,
		sourceAck: ackFn,
	}, nil
}

func (a *tarReader) ack(ctx context.Context, err error) error {
	a.mut.Lock()
	a.pending--
	doAck := a.pending == 0 && a.finished
	a.mut.Unlock()

	if err != nil {
		return a.sourceAck(ctx, err)
	}
	if doAck {
		return a.sourceAck(ctx, nil)
	}
	return nil
}

func (a *tarReader) Next(ctx context.Context) (types.Part, ReaderAckFn, error) {
	a.mut.Lock()
	defer a.mut.Unlock()

	_, err := a.buf.Next()
	if err == nil {
		fileBuf := bytes.Buffer{}
		_, err = fileBuf.ReadFrom(a.buf)
		if err != nil {
			return nil, nil, err
		}
		a.pending++
		a.total++
		return message.NewPart(fileBuf.Bytes()), a.ack, nil
	}

	if err == io.EOF {
		a.finished = true
	}
	return nil, nil, err
}

func (a *tarReader) Close(ctx context.Context) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	if !a.finished {
		a.sourceAck(ctx, errors.New("service shutting down"))
	}
	if a.pending == 0 && a.total > 0 {
		a.sourceAck(ctx, nil)
	}
	return a.r.Close()
}
