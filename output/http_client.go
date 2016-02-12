/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package output

import (
	"bytes"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["http_client"] = NewHTTPClient
}

//--------------------------------------------------------------------------------------------------

// HTTPClientConfig - Configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	URL            string `json:"url" yaml:"url"`
	TimeoutMS      int64  `json:"timeout_ms" yaml:"timeout_ms"`
	RetryMS        int64  `json:"retry_period" yaml:"retry_period"`
	FullForwarding bool   `json:"full_contents_forwarding" yaml:"full_contents_forwarding"`
}

// NewHTTPClientConfig - Creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		URL:            "localhost:8081/post",
		TimeoutMS:      5000,
		RetryMS:        1000,
		FullForwarding: true,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTPClient - An output type that pushes messages to HTTPClient.
type HTTPClient struct {
	running int32

	stats metrics.Aggregator
	log   *log.Logger

	conf Config

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
}

// NewHTTPClient - Create a new HTTPClient output type.
func NewHTTPClient(conf Config, log *log.Logger, stats metrics.Aggregator) (Type, error) {
	h := HTTPClient{
		running:      1,
		stats:        stats,
		log:          log.NewModule(".output.http"),
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
	}

	return &h, nil
}

//--------------------------------------------------------------------------------------------------

// loop - Internal loop brokers incoming messages to output pipe through POST requests.
func (h *HTTPClient) loop() {
	h.log.Infof("Sending HTTP Post messages to: %s\n", h.conf.HTTPClient.URL)

	for atomic.LoadInt32(&h.running) == 1 {
		msg, open := <-h.messages
		if !open {
			atomic.StoreInt32(&h.running, 0)
			h.messages = nil
			break
		}

		// POST message
		var res *http.Response
		var err error

		if len(msg.Parts) == 1 {
			res, err = http.Post(
				h.conf.HTTPClient.URL,
				"application/octet-stream",
				bytes.NewBuffer(msg.Parts[0]),
			)
		} else {
			res, err = http.Post(
				h.conf.HTTPClient.URL,
				"application/x-benthos-multipart",
				bytes.NewBuffer(msg.Bytes()),
			)
		}

		// Check status code
		if err == nil && res.StatusCode != 200 {
			err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
		}

		h.responseChan <- types.NewSimpleResponse(err)
		if err != nil {
			h.log.Errorf("POST request failed: %v\n", err)
			h.stats.Incr("output.http_client.post.error", 1)

			// If we failed then wait the configured retry period.
			if h.conf.HTTPClient.RetryMS > 0 {
				<-time.After(time.Duration(h.conf.HTTPClient.RetryMS) * time.Millisecond)
			}
		}
	}

	close(h.responseChan)
	close(h.closedChan)
}

// StartReceiving - Assigns a messages channel for the output to read.
func (h *HTTPClient) StartReceiving(msgs <-chan types.Message) error {
	if h.messages != nil {
		return types.ErrAlreadyStarted
	}
	h.messages = msgs
	go h.loop()
	return nil
}

// ResponseChan - Returns the errors channel.
func (h *HTTPClient) ResponseChan() <-chan types.Response {
	return h.responseChan
}

// CloseAsync - Shuts down the HTTPClient output and stops processing messages.
func (h *HTTPClient) CloseAsync() {
	atomic.StoreInt32(&h.running, 0)
}

// WaitForClose - Blocks until the HTTPClient output has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
