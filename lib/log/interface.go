// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

import "io"

//------------------------------------------------------------------------------

// Modular is a log printer that allows you to branch new modules.
type Modular interface {
	NewModule(prefix string) Modular

	// AddWriter adds a new writer to the logger which receives the same log
	// data as the primary writer. If this new writer returns an error it is
	// removed. The logger becomes the owner of this writer and under any
	// circumstance whereby the writer is removed it will also be closed by the
	// logger.
	AddWriter(w io.Writer)

	// RemoveWriter removes writer from the logger.
	RemoveWriter(w io.Writer)

	Fatalf(message string, other ...interface{})
	Errorf(message string, other ...interface{})
	Warnf(message string, other ...interface{})
	Infof(message string, other ...interface{})
	Debugf(message string, other ...interface{})
	Tracef(message string, other ...interface{})

	Fatalln(message string)
	Errorln(message string)
	Warnln(message string)
	Infoln(message string)
	Debugln(message string)
	Traceln(message string)

	Output(calldepth int, s string) error

	// Close the logger, including the underlying io.Writer if it implements the
	// io.Closer interface.
	Close() error
}

//------------------------------------------------------------------------------
