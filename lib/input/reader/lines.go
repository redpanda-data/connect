// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package reader

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// Lines is a reader implementation that continuously reads line delimited
// messages from an io.Reader type.
type Lines struct {
	handleCtor func(ctx context.Context) (io.Reader, error)
	onClose    func(ctx context.Context)

	mut        sync.Mutex
	handle     io.Reader
	shutdownFn func()
	errChan    chan error
	msgChan    chan types.Message

	maxBuffer int
	multipart bool
	delimiter []byte
}

// NewLines creates a new reader input type able to create a feed of line
// delimited messages from an io.Reader.
//
// Callers must provide a constructor function for the target io.Reader, which
// is called on start up and again each time a reader is exhausted. If the
// constructor is called but there is no more content to create a Reader for
// then the error `io.EOF` should be returned and the Lines will close.
//
// Callers must also provide an onClose function, which will be called if the
// Lines has been instructed to shut down. This function should unblock any
// blocked Read calls.
func NewLines(
	handleCtor func() (io.Reader, error),
	onClose func(),
	options ...func(r *Lines),
) (*Lines, error) {
	r := Lines{
		handleCtor: func(ctx context.Context) (io.Reader, error) {
			return handleCtor()
		},
		onClose: func(ctx context.Context) {
			onClose()
		},
		maxBuffer: bufio.MaxScanTokenSize,
		multipart: false,
		delimiter: []byte("\n"),
	}

	for _, opt := range options {
		opt(&r)
	}

	r.shutdownFn = func() {}
	return &r, nil
}

// NewLinesWithContext expands NewLines by requiring context.Context arguments
// in the provided closures.
func NewLinesWithContext(
	handleCtor func(ctx context.Context) (io.Reader, error),
	onClose func(ctx context.Context),
	options ...func(r *Lines),
) (*Lines, error) {
	r := Lines{
		handleCtor: handleCtor,
		onClose:    onClose,
		maxBuffer:  bufio.MaxScanTokenSize,
		multipart:  false,
		delimiter:  []byte("\n"),
	}

	for _, opt := range options {
		opt(&r)
	}

	r.shutdownFn = func() {}
	return &r, nil
}

//------------------------------------------------------------------------------

// OptLinesSetMaxBuffer is a option func that sets the maximum size of the
// line parsing buffers.
func OptLinesSetMaxBuffer(maxBuffer int) func(r *Lines) {
	return func(r *Lines) {
		r.maxBuffer = maxBuffer
	}
}

// OptLinesSetMultipart is a option func that sets the boolean flag
// indicating whether lines should be parsed as multipart or not.
func OptLinesSetMultipart(multipart bool) func(r *Lines) {
	return func(r *Lines) {
		r.multipart = multipart
	}
}

// OptLinesSetDelimiter is a option func that sets the delimiter (default
// '\n') used to divide lines (message parts) in the stream of data.
func OptLinesSetDelimiter(delimiter string) func(r *Lines) {
	return func(r *Lines) {
		r.delimiter = []byte(delimiter)
	}
}

//------------------------------------------------------------------------------

func (r *Lines) closeHandle() {
	if r.handle != nil {
		if closer, ok := r.handle.(io.ReadCloser); ok {
			closer.Close()
		}
		r.handle = nil
	}
	r.shutdownFn()
}

//------------------------------------------------------------------------------

// Connect attempts to establish a new scanner for an io.Reader.
func (r *Lines) Connect() error {
	return r.ConnectWithContext(context.Background())
}

// ConnectWithContext attempts to establish a new scanner for an io.Reader.
func (r *Lines) ConnectWithContext(ctx context.Context) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	r.closeHandle()

	handle, err := r.handleCtor(ctx)
	if err != nil {
		if err == io.EOF {
			return types.ErrTypeClosed
		}
		return err
	}

	scanner := bufio.NewScanner(handle)
	if r.maxBuffer != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, r.maxBuffer)
	}

	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		if i := bytes.Index(data, r.delimiter); i >= 0 {
			// We have a full terminated line.
			return i + len(r.delimiter), data[0:i], nil
		}

		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}

		// Request more data.
		return 0, nil, nil
	})

	scannerCtx, shutdownFn := context.WithCancel(context.Background())
	msgChan := make(chan types.Message)
	errChan := make(chan error)

	go func() {
		defer func() {
			shutdownFn()
			close(errChan)
			close(msgChan)
		}()

		msg := message.New(nil)
		for scanner.Scan() {
			partBytes := make([]byte, len(scanner.Bytes()))
			partSize := copy(partBytes, scanner.Bytes())

			if partSize > 0 {
				msg.Append(message.NewPart(partBytes))
				if !r.multipart {
					select {
					case msgChan <- msg:
					case <-scannerCtx.Done():
						return
					}
					msg = message.New(nil)
				}
			} else if r.multipart && msg.Len() > 0 {
				// Empty line means we're finished reading parts for this
				// message.
				select {
				case msgChan <- msg:
				case <-scannerCtx.Done():
					return
				}
				msg = message.New(nil)
			}
		}
		if msg.Len() > 0 {
			select {
			case msgChan <- msg:
			case <-scannerCtx.Done():
				return
			}
		}
		if serr := scanner.Err(); serr != nil {
			select {
			case errChan <- serr:
			case <-scannerCtx.Done():
				return
			}
		}
	}()

	r.handle = handle
	r.msgChan = msgChan
	r.errChan = errChan
	r.shutdownFn = shutdownFn
	return nil
}

// ReadWithContext attempts to read a new line from the io.Reader.
func (r *Lines) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	r.mut.Lock()
	msgChan := r.msgChan
	errChan := r.errChan
	r.mut.Unlock()

	select {
	case msg, open := <-msgChan:
		if !open {
			return nil, nil, types.ErrNotConnected
		}
		return msg, noopAsyncAckFn, nil
	case err, open := <-errChan:
		if !open {
			return nil, nil, types.ErrNotConnected
		}
		return nil, nil, err
	case <-ctx.Done():
	}
	return nil, nil, types.ErrTimeout
}

// Read attempts to read a new line from the io.Reader.
func (r *Lines) Read() (types.Message, error) {
	r.mut.Lock()
	msgChan := r.msgChan
	errChan := r.errChan
	r.mut.Unlock()

	select {
	case msg, open := <-msgChan:
		if !open {
			return nil, types.ErrNotConnected
		}
		return msg, nil
	case err, open := <-errChan:
		if !open {
			return nil, types.ErrNotConnected
		}
		return nil, err
	}
}

// Acknowledge confirms whether or not our unacknowledged messages have been
// successfully propagated or not.
func (r *Lines) Acknowledge(err error) error {
	return nil
}

// CloseAsync shuts down the reader input and stops processing requests.
func (r *Lines) CloseAsync() {
	go func() {
		r.mut.Lock()
		r.onClose(context.Background())
		r.closeHandle()
		r.mut.Unlock()
	}()
}

// WaitForClose blocks until the reader input has closed down.
func (r *Lines) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
