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

package input

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["http_client"] = TypeSpec{
		constructor: NewHTTPClient,
		description: `
The HTTP client input type connects to a server and continuously performs
requests for a single message.

You should set a sensible retry period and max backoff so as to not flood your
target server.

### Streaming

If you enable streaming then Benthos will consume the body of the response as a
line delimited list of message parts. Each part is read as an individual message
unless multipart is set to true, in which case an empty line indicates the end
of a message.`,
	}
}

//------------------------------------------------------------------------------

// StreamConfig contains fields for specifiying stream consumption behaviour.
type StreamConfig struct {
	Enabled   bool   `json:"enabled" yaml:"enabled"`
	Reconnect bool   `json:"reconnect" yaml:"reconnect"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// HTTPClientConfig is configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	client.Config `json:",inline" yaml:",inline"`
	Payload       string       `json:"payload" yaml:"payload"`
	Stream        StreamConfig `json:"stream" yaml:"stream"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	cConf := client.NewConfig()
	cConf.Verb = "GET"
	cConf.URL = "http://localhost:4195/get"
	return HTTPClientConfig{
		Config:  cConf,
		Payload: "",
		Stream: StreamConfig{
			Enabled:   false,
			Reconnect: true,
			Multipart: false,
			MaxBuffer: 1000000,
			Delim:     "",
		},
	}
}

//------------------------------------------------------------------------------

// HTTPClient is an output type that pushes messages to HTTPClient.
type HTTPClient struct {
	running int32

	stats metrics.Type
	log   log.Modular

	conf Config

	buffer *bytes.Buffer
	client *client.Type

	payload types.Message

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewHTTPClient creates a new HTTPClient output type.
func NewHTTPClient(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	h := HTTPClient{
		running:      1,
		stats:        stats,
		log:          log.NewModule(".input.http_client"),
		conf:         conf,
		buffer:       &bytes.Buffer{},
		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	if h.conf.HTTPClient.Stream.Enabled {
		// Timeout should be left at zero if we are streaming.
		h.conf.HTTPClient.TimeoutMS = 0
	}
	if len(h.conf.HTTPClient.Payload) > 0 {
		h.payload = types.NewMessage([][]byte{[]byte(h.conf.HTTPClient.Payload)})
	}

	h.client = client.New(
		h.conf.HTTPClient.Config,
		client.OptSetCloseChan(h.closeChan),
		client.OptSetLogger(h.log),
		client.OptSetStats(metrics.Namespaced(h.stats, "input.http_client")),
	)

	if !h.conf.HTTPClient.Stream.Enabled {
		go h.loop()
		return &h, nil
	}

	delim := conf.HTTPClient.Stream.Delim
	if len(delim) == 0 {
		delim = "\n"
	}

	var resMux sync.Mutex
	var closed bool
	var res *http.Response

	conn := false

	var (
		mStrmConstructor = h.stats.GetCounter("input.http_client.stream.constructor")
		mStrmReqErr      = h.stats.GetCounter("input.http_client.stream.request.error")
		mStrnOnClose     = h.stats.GetCounter("input.http_client.stream.on_close")
	)

	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			mStrmConstructor.Incr(1)

			resMux.Lock()
			defer resMux.Unlock()

			if conn && !conf.HTTPClient.Stream.Reconnect {
				return nil, io.EOF
			}

			if res != nil {
				res.Body.Close()
			}

			var err error
			res, err = h.doRequest()
			for err != nil && !closed {
				h.log.Errorf("HTTP stream request failed: %v\n", err)
				mStrmReqErr.Incr(1)

				resMux.Unlock()
				<-time.After(time.Second)
				resMux.Lock()

				res, err = h.doRequest()
			}

			if closed {
				return nil, io.EOF
			}

			conn = true
			return res.Body, nil
		},
		func() {
			mStrnOnClose.Incr(1)

			resMux.Lock()
			defer resMux.Unlock()

			closed = true

			// On shutdown we close the response body, this should end any
			// blocked Read calls.
			if res != nil {
				res.Body.Close()
				res = nil
			}
		},
		reader.OptLinesSetDelimiter(delim),
		reader.OptLinesSetMaxBuffer(conf.HTTPClient.Stream.MaxBuffer),
		reader.OptLinesSetMultipart(conf.HTTPClient.Stream.Multipart),
	)
	if err != nil {
		return nil, err
	}

	return NewReader(
		"http_client",
		reader.NewPreserver(rdr),
		log, stats,
	)
}

//------------------------------------------------------------------------------

func (h *HTTPClient) doRequest() (*http.Response, error) {
	return h.client.Do(h.payload)
}

func (h *HTTPClient) parseResponse(res *http.Response) ([][]byte, error) {
	msg, err := h.client.ParseResponse(res)
	if err != nil {
		return nil, err
	}
	return msg.GetAll(), nil
}

//------------------------------------------------------------------------------

// loop is an internal loop brokers incoming messages to output pipe through
// POST requests.
func (h *HTTPClient) loop() {
	var (
		mRunning     = h.stats.GetCounter("input.http_client.running")
		mReqTimedOut = h.stats.GetCounter("input.http_client.request.timed_out")
		mReqErr      = h.stats.GetCounter("input.http_client.request.error")
		mReqParseErr = h.stats.GetCounter("input.http_client.request.parse.error")
		mReqSucc     = h.stats.GetCounter("input.http_client.request.success")
		mCount       = h.stats.GetCounter("input.http_client.count")
		mSendErr     = h.stats.GetCounter("input.http_client.send.error")
		mSendSucc    = h.stats.GetCounter("input.http_client.send.success")
	)

	defer func() {
		atomic.StoreInt32(&h.running, 0)
		mRunning.Decr(1)

		close(h.transactions)
		close(h.closedChan)
	}()

	mRunning.Incr(1)
	h.log.Infof("Polling for HTTP messages from: %s\n", h.conf.HTTPClient.URL)

	resOut := make(chan types.Response)

	var msgOut types.Message
	for atomic.LoadInt32(&h.running) == 1 {
		if msgOut == nil {
			var res *http.Response
			var err error

			if res, err = h.doRequest(); err != nil {
				if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
					// Hate this ^
					mReqTimedOut.Incr(1)
				} else {
					h.log.Errorf("Request failed: %v\n", err)
					mReqErr.Incr(1)
				}
			} else {
				var parts [][]byte
				if parts, err = h.parseResponse(res); err != nil {
					mReqParseErr.Incr(1)
					h.log.Errorf("Failed to decode response: %v\n", err)
				} else {
					mReqSucc.Incr(1)
					msgOut = types.NewMessage(parts)
				}
				res.Body.Close()
			}
			mCount.Incr(1)
		}

		if msgOut != nil {
			select {
			case h.transactions <- types.NewTransaction(msgOut, resOut):
			case <-h.closeChan:
				return
			}
			select {
			case res, open := <-resOut:
				if !open {
					return
				}
				if res.Error() != nil {
					mSendErr.Incr(1)
				} else {
					msgOut = nil
					mSendSucc.Incr(1)
				}
			case <-h.closeChan:
				return
			}
		}
	}
}

// TransactionChan returns the transactions channel.
func (h *HTTPClient) TransactionChan() <-chan types.Transaction {
	return h.transactions
}

// CloseAsync shuts down the HTTPClient output and stops processing messages.
func (h *HTTPClient) CloseAsync() {
	if atomic.CompareAndSwapInt32(&h.running, 1, 0) {
		close(h.closeChan)
	}
}

// WaitForClose blocks until the HTTPClient output has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
