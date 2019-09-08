// Copyright (c) 2019 Ashley Jeffs
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

package writer

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// DropConfig contains configuration fields for the drop output type.
type DropConfig struct{}

// NewDropConfig creates a new DropConfig with default values.
func NewDropConfig() DropConfig {
	return DropConfig{}
}

//------------------------------------------------------------------------------

// Drop is a benthos writer.Type implementation that writes message parts to no
// where.
type Drop struct {
	log log.Modular
}

// NewDrop creates a new file based writer.Type.
func NewDrop(
	conf DropConfig,
	log log.Modular,
	stats metrics.Type,
) *Drop {
	return &Drop{
		log: log,
	}
}

// Connect is a noop.
func (d *Drop) Connect() error {
	d.log.Infoln("Dropping messages.")
	return nil
}

// Write does nothing.
func (d *Drop) Write(msg types.Message) error {
	return nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (d *Drop) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (d *Drop) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
