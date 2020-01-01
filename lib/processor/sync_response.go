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

package processor

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSyncResponse] = TypeSpec{
		constructor: NewSyncResponse,
		Description: `
Adds the payload in its current state as a synchronous response to the input
source, where it is dealt with according to that specific input type.

For most inputs this mechanism is ignored entirely, in which case the sync
response is dropped without penalty. It is therefore safe to use this processor
even when combining input types that might not have support for sync responses.
An example of an input able to utilise this is the ` + "`http_server`" + `.

For more information please read [Synchronous Responses](../sync_responses.md).`,
	}
}

//------------------------------------------------------------------------------

// SyncResponseConfig contains configuration fields for the SyncResponse
// processor.
type SyncResponseConfig struct{}

// NewSyncResponseConfig returns a SyncResponseConfig with default values.
func NewSyncResponseConfig() SyncResponseConfig {
	return SyncResponseConfig{}
}

//------------------------------------------------------------------------------

// SyncResponse is a processor that prints a log event each time it processes a message.
type SyncResponse struct {
	log log.Modular
}

// NewSyncResponse returns a SyncResponse processor.
func NewSyncResponse(
	conf Config, mgr types.Manager, logger log.Modular, stats metrics.Type,
) (Type, error) {
	s := &SyncResponse{
		log: logger,
	}
	return s, nil
}

//------------------------------------------------------------------------------

// ProcessMessage logs an event and returns the message unchanged.
func (s *SyncResponse) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	if err := roundtrip.SetAsResponse(msg); err != nil {
		s.log.Debugf("Failed to store message as a sync response: %v\n", err)
	}
	return []types.Message{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (s *SyncResponse) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (s *SyncResponse) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
