// Copyright (c) 2017 Ashley Jeffs
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

package processor

import (
	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["multi_to_blob"] = typeSpec{
		constructor: NewMultiToBlob,
		description: `
If an input supports multiple part messages but your output does not you will
end up with each part being sent as a unique message. This can cause confusion
and complexity regarding delivery guarantees.

You can instead use this processor to encode multiple part messages into a
binary single part message, which can be converted back further down the
platform pipeline using the blob to multi processor.

E.g. ZMQ => Benthos(multi to blob) => Kafka => Benthos(blob to multi)`,
	}
}

//------------------------------------------------------------------------------

// MultiToBlob is a processor that takes messages with potentially multiple
// parts and converts them into a single part message using the benthos binary
// format. This message can be converted back to multiple parts using the
// BlobToMulti processor.
type MultiToBlob struct {
	stats metrics.Type
}

// NewMultiToBlob returns a MultiToBlob processor.
func NewMultiToBlob(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return MultiToBlob{
		stats: stats,
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message of > 0 parts and returns a single part message
// that can be later converted back to the original parts.
func (m MultiToBlob) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	newMsg := types.NewMessage()
	newMsg.Parts = append(newMsg.Parts, msg.Bytes())
	m.stats.Incr("processor.multi_to_blob.count", 1)
	return &newMsg, nil, true
}

//------------------------------------------------------------------------------
