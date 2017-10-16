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
	constructors["blob_to_multi"] = typeSpec{
		constructor: NewBlobToMulti,
		description: `
If a multiple part message has been encoded into a single part message using the
multi_to_blob processor then this processor is able to convert it back into a
multiple part message.

You can therefore use this processor when multiple Benthos instances are
bridging between message queues that don't support multiple parts.

E.g. ZMQ => Benthos(multi_to_blob) => Kafka => Benthos(blob_to_multi)`,
	}
}

//------------------------------------------------------------------------------

// BlobToMulti is a processor that takes messages with a single part in a
// benthos multiple part blob format and decodes them into multiple part
// messages.
type BlobToMulti struct {
	log log.Modular
}

// NewBlobToMulti returns a BlobToMulti processor.
func NewBlobToMulti(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	return &BlobToMulti{
		log: log.NewModule(".processor.blob_to_multi"),
	}, nil
}

//------------------------------------------------------------------------------

// ProcessMessage takes a message with 1 part in multiple part blob format and
// returns a multiple part message by decoding it.
func (m *BlobToMulti) ProcessMessage(msg *types.Message) (*types.Message, types.Response, bool) {
	if len(msg.Parts) != 1 {
		m.log.Errorf("Cannot decode message into mutiple parts due to parts count: %v != 1\n", len(msg.Parts))
		return nil, types.NewSimpleResponse(nil), false
	}
	newMsg, err := types.FromBytes(msg.Parts[0])
	if err != nil {
		m.log.Errorf("Failed to decode message into multiple parts: %v\n", err)
		return nil, types.NewSimpleResponse(nil), false
	}
	return &newMsg, nil, true
}

//------------------------------------------------------------------------------
