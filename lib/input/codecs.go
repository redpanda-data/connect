package input

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"sync"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

var codecDocs = docs.FieldCommon(
	"codec", "The way in which the bytes of consumed files are converted into messages, codecs are useful for specifying how large files might be processed in small chunks rather than loading it all in memory.",
).HasOptions("all-bytes", "lines", "tar", "tar-gzip")

//------------------------------------------------------------------------------

type codecAckFn func(context.Context, error) error

type partCodec interface {
	Next(ctx context.Context) (types.Part, codecAckFn, error)
	Close(context.Context) error
}

type partCodecCtor func(io.ReadCloser, codecAckFn) (partCodec, error)

func getPartCodec(codec string) (partCodecCtor, error) {
	switch codec {
	case "all-bytes":
		return func(r io.ReadCloser, fn codecAckFn) (partCodec, error) {
			return &allBytesCodec{r, fn, false}, nil
		}, nil
	case "lines":
		return newLinesCodec, nil
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

func newLinesCodec(r io.ReadCloser, ackFn codecAckFn) (partCodec, error) {
	return &linesCodec{
		buf:       bufio.NewScanner(r),
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
