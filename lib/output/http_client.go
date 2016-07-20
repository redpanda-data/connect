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
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["http_client"] = typeSpec{
		constructor: NewHTTPClient,
		description: `
The HTTP client output type connects to a server and sends POST requests for
each message. The body of the request is the raw message contents. The output
will apply back pressure until a 2XX response has been returned from the server.

For more information about sending HTTP messages, including details on sending
multipart, please read the 'docs/using_http.md' document.`,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTPClientConfig - Configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	URL       string `json:"url" yaml:"url"`
	TimeoutMS int64  `json:"timeout_ms" yaml:"timeout_ms"`
	RetryMS   int64  `json:"retry_period_ms" yaml:"retry_period_ms"`
}

// NewHTTPClientConfig - Creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		URL:       "localhost:8081/post",
		TimeoutMS: 5000,
		RetryMS:   1000,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTPClient - An output type that pushes messages to HTTPClient.
type HTTPClient struct {
	running int32

	stats metrics.Aggregator
	log   log.Modular

	conf Config

	messages     <-chan types.Message
	responseChan chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewHTTPClient - Create a new HTTPClient output type.
func NewHTTPClient(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	h := HTTPClient{
		running:      1,
		stats:        stats,
		log:          log.NewModule(".output.http"),
		conf:         conf,
		messages:     nil,
		responseChan: make(chan types.Response),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	return &h, nil
}

//--------------------------------------------------------------------------------------------------

// createMultiPartRequest - Creates an HTTP multipart request out of a multipart message.
func createMultiPartRequest(url string, msg *types.Message) (req *http.Request, err error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for i := 0; i < len(msg.Parts) && err == nil; i++ {
		var part io.Writer
		if part, err = writer.CreatePart(textproto.MIMEHeader{
			"Content-Type": []string{"application/octet-stream"},
		}); err == nil {
			_, err = io.Copy(part, bytes.NewReader(msg.Parts[i]))
		}
	}

	writer.Close()
	if req, err = http.NewRequest("POST", url, body); err == nil {
		req.Header.Add("Content-Type", writer.FormDataContentType())
	}

	return
}

// loop - Internal loop brokers incoming messages to output pipe through POST requests.
func (h *HTTPClient) loop() {
	defer func() {
		atomic.StoreInt32(&h.running, 0)

		close(h.responseChan)
		close(h.closedChan)
	}()

	h.log.Infof("Sending HTTP Post messages to: %s\n", h.conf.HTTPClient.URL)

	var open bool
	for atomic.LoadInt32(&h.running) == 1 {
		var msg types.Message
		if msg, open = <-h.messages; !open {
			return
		}

		// POST message
		var client http.Client
		client.Timeout = time.Duration(h.conf.HTTPClient.TimeoutMS) * time.Millisecond

		var res *http.Response
		var err error

		if len(msg.Parts) == 1 {
			res, err = client.Post(
				h.conf.HTTPClient.URL,
				"application/octet-stream",
				bytes.NewBuffer(msg.Parts[0]),
			)
		} else {
			var req *http.Request
			if req, err = createMultiPartRequest(h.conf.HTTPClient.URL, &msg); err == nil {
				res, err = client.Do(req)
			}
		}

		// Check status code
		if err == nil && res.StatusCode != 200 {
			err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
		}

		if err != nil {
			h.log.Errorf("POST request failed: %v\n", err)
			h.stats.Incr("output.http_client.post.error", 1)
		}
		select {
		case h.responseChan <- types.NewSimpleResponse(err):
		case <-h.closeChan:
			return
		}

		if err != nil && h.conf.HTTPClient.RetryMS > 0 {
			// If we failed then wait the configured retry period.
			select {
			case <-time.After(time.Duration(h.conf.HTTPClient.RetryMS) * time.Millisecond):
			case <-h.closeChan:
				return
			}
		}
	}
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
	if atomic.CompareAndSwapInt32(&h.running, 1, 0) {
		close(h.closeChan)
	}
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
