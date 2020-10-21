package input

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

var codecDocs = docs.FieldCommon(
	"codec", "The way in which the bytes of consumed files are converted into messages, codecs are useful for specifying how large files might be processed in small chunks rather than loading it all in memory. It's possible to consume lines using a custom delimiter with the `delim:x` codec, where x is the character sequence custom delimiter.", "lines", "delim:\t", "delim:foobar",
).HasOptions("all-bytes", "lines", "delim:x", "tar", "tar-gzip")

//------------------------------------------------------------------------------

type codecConfig struct {
	MaxScanTokenSize int
}

func newCodecConfig() codecConfig {
	return codecConfig{
		MaxScanTokenSize: bufio.MaxScanTokenSize,
	}
}

//------------------------------------------------------------------------------

type codecAckFn func(context.Context, error) error

type partCodec interface {
	Next(ctx context.Context) (types.Part, codecAckFn, error)
	Close(context.Context) error
}

type partCodecCtor func(io.ReadCloser, codecAckFn) (partCodec, error)

func getPartCodec(codec string, conf codecConfig) (partCodecCtor, error) {
	switch codec {
	case "all-bytes":
		return func(r io.ReadCloser, fn codecAckFn) (partCodec, error) {
			return &allBytesCodec{r, fn, false}, nil
		}, nil
	case "lines":
		return func(r io.ReadCloser, fn codecAckFn) (partCodec, error) {
			return newLinesCodec(conf, r, fn)
		}, nil
	case "tar":
		return newTarCodec, nil
	case "tar-gzip":
		return func(r io.ReadCloser, fn codecAckFn) (partCodec, error) {
			g, err := gzip.NewReader(r)
			if err != nil {
				r.Close()
				return nil, err
			}
			return newTarCodec(g, fn)
		}, nil
	}
	if strings.HasPrefix(codec, "delim:") {
		by := strings.TrimPrefix(codec, "delim:")
		if len(by) == 0 {
			return nil, errors.New("custom delimiter codec requires a non-empty delimiter")
		}
		return func(r io.ReadCloser, fn codecAckFn) (partCodec, error) {
			return newCustomDelimCodec(conf, r, by, fn)
		}, nil
	}
	return nil, fmt.Errorf("codec was not recognised: %v", codec)
}

//------------------------------------------------------------------------------

type allBytesCodec struct {
	i        io.ReadCloser
	ack      codecAckFn
	consumed bool
}

func (a *allBytesCodec) Next(ctx context.Context) (types.Part, codecAckFn, error) {
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

func (a *allBytesCodec) Close(ctx context.Context) error {
	if !a.consumed {
		a.ack(ctx, errors.New("service shutting down"))
	}
	return a.i.Close()
}

//------------------------------------------------------------------------------

type linesCodec struct {
	buf       *bufio.Scanner
	r         io.ReadCloser
	sourceAck codecAckFn

	mut      sync.Mutex
	finished bool
	total    int32
	pending  int32
}

func newLinesCodec(conf codecConfig, r io.ReadCloser, ackFn codecAckFn) (partCodec, error) {
	scanner := bufio.NewScanner(r)
	if conf.MaxScanTokenSize != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, conf.MaxScanTokenSize)
	}
	return &linesCodec{
		buf:       scanner,
		r:         r,
		sourceAck: ackFn,
	}, nil
}

func (a *linesCodec) ack(ctx context.Context, err error) error {
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

func (a *linesCodec) Next(ctx context.Context) (types.Part, codecAckFn, error) {
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

func (a *linesCodec) Close(ctx context.Context) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	if !a.finished {
		a.sourceAck(ctx, errors.New("service shutting down"))
	}
	if a.pending == 0 && a.total == 0 {
		a.sourceAck(ctx, nil)
	}
	return a.r.Close()
}

//------------------------------------------------------------------------------

type customDelimCodec struct {
	buf       *bufio.Scanner
	r         io.ReadCloser
	sourceAck codecAckFn

	mut      sync.Mutex
	finished bool
	total    int32
	pending  int32
}

func newCustomDelimCodec(conf codecConfig, r io.ReadCloser, delim string, ackFn codecAckFn) (partCodec, error) {
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

	return &customDelimCodec{
		buf:       scanner,
		r:         r,
		sourceAck: ackFn,
	}, nil
}

func (a *customDelimCodec) ack(ctx context.Context, err error) error {
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

func (a *customDelimCodec) Next(ctx context.Context) (types.Part, codecAckFn, error) {
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

func (a *customDelimCodec) Close(ctx context.Context) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	if !a.finished {
		a.sourceAck(ctx, errors.New("service shutting down"))
	}
	if a.pending == 0 && a.total == 0 {
		a.sourceAck(ctx, nil)
	}
	return a.r.Close()
}

//------------------------------------------------------------------------------

type tarCodec struct {
	buf       *tar.Reader
	r         io.ReadCloser
	sourceAck codecAckFn

	mut      sync.Mutex
	finished bool
	total    int32
	pending  int32
}

func newTarCodec(r io.ReadCloser, ackFn codecAckFn) (partCodec, error) {
	return &tarCodec{
		buf:       tar.NewReader(r),
		r:         r,
		sourceAck: ackFn,
	}, nil
}

func (a *tarCodec) ack(ctx context.Context, err error) error {
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

func (a *tarCodec) Next(ctx context.Context) (types.Part, codecAckFn, error) {
	a.mut.Lock()
	defer a.mut.Unlock()

	_, err := a.buf.Next()
	if err == nil {
		b, err := ioutil.ReadAll(a.buf)
		if err != nil {
			return nil, nil, err
		}
		a.pending++
		a.total++
		return message.NewPart(b), a.ack, nil
	}

	if err == io.EOF {
		a.finished = true
	}
	return nil, nil, err
}

func (a *tarCodec) Close(ctx context.Context) error {
	a.mut.Lock()
	defer a.mut.Unlock()

	if !a.finished {
		a.sourceAck(ctx, errors.New("service shutting down"))
	}
	if a.pending == 0 && a.total == 0 {
		a.sourceAck(ctx, nil)
	}
	return a.r.Close()
}
