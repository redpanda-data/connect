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

package log

import (
	"io"
	"sync"
)

//------------------------------------------------------------------------------

// FanOutWriter is an io.Writer that can add and remove other writers, there is
// a primary writer that blocks, all other writers are lossy and will be cut off
// and closed if they return errors.
type FanOutWriter struct {
	writer io.Writer

	lossyWriters map[io.Writer]struct{}
	mut          sync.Mutex
}

// NewFanOutWriter creates a new fanned out writer.
func NewFanOutWriter(w io.Writer) *FanOutWriter {
	fow := &FanOutWriter{
		writer:       w,
		lossyWriters: map[io.Writer]struct{}{},
	}
	return fow
}

//------------------------------------------------------------------------------

// Add a new lossy writer.
func (w *FanOutWriter) Add(lw io.Writer) {
	w.mut.Lock()
	w.lossyWriters[lw] = struct{}{}
	w.mut.Unlock()
}

// Remove an existing lossy writer. Writers that are removed and implement
// io.Closer will be closed as they are removed.
func (w *FanOutWriter) Remove(lw io.Writer) {
	w.mut.Lock()
	if _, exists := w.lossyWriters[lw]; exists {
		delete(w.lossyWriters, lw)
		if clw, ok := lw.(io.Closer); ok {
			clw.Close()
		}
	}
	w.mut.Unlock()
}

// Write to all registered writers, if a writer blocks this call will also
// block.
func (w *FanOutWriter) Write(p []byte) (n int, err error) {
	if n, err = w.writer.Write(p); err != nil {
		return
	}

	w.mut.Lock()
	for tw := range w.lossyWriters {
		// If an error occurs we remove and close the writer.
		if _, err := tw.Write(p[:n]); err != nil {
			if clw, ok := tw.(io.Closer); ok {
				clw.Close()
			}
			delete(w.lossyWriters, tw)
		}
	}
	w.mut.Unlock()

	return
}

// Close all lossy writers. This call does NOT close the primary writer.
func (w *FanOutWriter) Close() error {
	w.mut.Lock()
	for tw := range w.lossyWriters {
		if clw, ok := tw.(io.Closer); ok {
			clw.Close()
		}
		delete(w.lossyWriters, tw)
	}
	w.mut.Unlock()
	return nil
}

//------------------------------------------------------------------------------
