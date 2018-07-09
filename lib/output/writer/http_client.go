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

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/client"
)

//------------------------------------------------------------------------------

// HTTPClientConfig is configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	client.Config `json:",inline" yaml:",inline"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		Config: client.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// HTTPClient is an output type that pushes messages to HTTPClient.
type HTTPClient struct {
	client *client.Type

	stats metrics.Type
	log   log.Modular

	conf      HTTPClientConfig
	closeChan chan struct{}
}

// NewHTTPClient creates a new HTTPClient writer type.
func NewHTTPClient(conf HTTPClientConfig, log log.Modular, stats metrics.Type) *HTTPClient {
	h := HTTPClient{
		stats:     stats,
		log:       log.NewModule(".output.http_client"),
		conf:      conf,
		closeChan: make(chan struct{}),
	}
	h.client = client.New(
		conf.Config,
		client.OptSetCloseChan(h.closeChan),
		client.OptSetLogger(h.log),
		client.OptSetStats(metrics.Namespaced(h.stats, "output.http_client")),
	)
	return &h
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
	_, err := h.client.Send(msg)
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
