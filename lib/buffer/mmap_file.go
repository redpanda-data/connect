// Copyright (c) 2014 Ashley Jeffs
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

package buffer

import (
	"github.com/Jeffail/benthos/lib/buffer/single"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["mmap_file"] = TypeSpec{
		constructor: NewMmapFile,
		description: `
The mmap file buffer type uses memory mapped files to perform low-latency,
file-persisted buffering of messages.

To configure the mmap file buffer you need to designate a writeable directory
for storing the mapped files. Benthos will create multiple files in this
directory as it fills them.

When files are fully read from they will be deleted. You can disable this
feature if you wish to preserve the data indefinitely, but the directory will
fill up as fast as data passes through.`,
	}
}

//------------------------------------------------------------------------------

// NewMmapFile creates a buffer held in memory and persisted to file through
// memory map.
func NewMmapFile(config Config, log log.Modular, stats metrics.Type) (Type, error) {
	b, err := single.NewMmapBuffer(config.Mmap, log.NewModule(".buffer.mmap_file"), stats)
	if err != nil {
		return nil, err
	}
	return NewSingleWrapper(config, b, log, stats), nil
}

//------------------------------------------------------------------------------
