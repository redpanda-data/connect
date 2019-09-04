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

package writer

import (
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/roundtrip"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

//------------------------------------------------------------------------------

// HTTPClientConfig contains configuration fields for the HTTPClient output
// type.
type HTTPClientConfig struct {
	client.Config     `json:",inline" yaml:",inline"`
	PropagateResponse bool `json:"propagate_response" yaml:"propagate_response"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		Config:            client.NewConfig(),
		PropagateResponse: false,
	}
}

//------------------------------------------------------------------------------

// HTTPClient is an output type that sends messages as HTTP requests to a target
// server endpoint.
type HTTPClient struct {
	client *client.Type

	stats metrics.Type
	log   log.Modular

	conf      HTTPClientConfig
	closeChan chan struct{}
}

// NewHTTPClient creates a new HTTPClient writer type.
func NewHTTPClient(
	conf HTTPClientConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*HTTPClient, error) {
	h := HTTPClient{
		stats:     stats,
		log:       log,
		conf:      conf,
		closeChan: make(chan struct{}),
	}
	var err error
	if h.client, err = client.New(
		conf.Config,
		client.OptSetCloseChan(h.closeChan),
		client.OptSetLogger(h.log),
		client.OptSetManager(mgr),
		client.OptSetStats(metrics.Namespaced(h.stats, "client")),
	); err != nil {
		return nil, err
	}
	return &h, nil
}

//------------------------------------------------------------------------------

// Connect does nothing.
func (h *HTTPClient) Connect() error {
	h.log.Infof("Sending messages via HTTP requests to: %s\n", h.conf.URL)
	return nil
}

// Write attempts to send a message to an HTTP server, this attempt may include
// retries, and if all retries fail an error is returned.
func (h *HTTPClient) Write(msg types.Message) error {
	resultMsg, err := h.client.Send(msg)
	if err == nil && h.conf.PropagateResponse {
		msgCopy := msg.Copy()
		parts := make([]types.Part, resultMsg.Len())
		resultMsg.Iter(func(i int, p types.Part) error {
			if i < msgCopy.Len() {
				parts[i] = msgCopy.Get(i)
			} else {
				parts[i] = msgCopy.Get(0)
			}
			parts[i].Set(p.Get())
			return nil
		})
		msgCopy.SetAll(parts)
		roundtrip.SetAsResponse(msgCopy)
	}
	return err
}

// CloseAsync shuts down the HTTPClient output and stops processing messages.
func (h *HTTPClient) CloseAsync() {
	close(h.closeChan)
}

// WaitForClose blocks until the HTTPClient output has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
