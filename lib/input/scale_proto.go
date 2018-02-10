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

package input

import (
	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["scalability_protocols"] = typeSpec{
		constructor: NewScaleProto,
		description: `
The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This input type should be compatible with any implementation of these protocols,
but nanomsg (http://nanomsg.org/index.html) is the specific target of this type.

Since scale proto messages are only single part we would need a binary format
for interpretting multi part messages. If the input is receiving messages from a
benthos output you can set both to use the benthos binary multipart format with
the 'benthos_multi' flag. Note, however, that this format may appear to be
gibberish to other services, and the input will be unable to read normal
messages with this setting.

Currently only PULL, SUB, and REP sockets are supported.

When using REP sockets Benthos will respond to each request with a success or
error message. The content of these messages are set with the 'reply_success'
and 'reply_error' config options respectively. The 'reply_timeout_ms' option
decides how long Benthos will wait before giving up on the reply, which can
result in duplicate messages when triggered.`,
	}
}

//------------------------------------------------------------------------------

// NewScaleProto creates a new ScaleProto input type.
func NewScaleProto(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	s, err := reader.NewScaleProto(conf.ScaleProto, log, stats)
	if err != nil {
		return nil, err
	}
	return NewReader("scalability_protocols", s, log, stats)
}

//------------------------------------------------------------------------------
