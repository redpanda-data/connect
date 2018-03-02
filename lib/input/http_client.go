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
	"bufio"
	"bytes"
	"crypto/tls"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/input/reader"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/http/auth"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["http_client"] = typeSpec{
		constructor: NewHTTPClient,
		description: `
The HTTP client input type connects to a server and continuously performs
requests for a single message.

You should set a sensible number of max retries and retry delays so as to not
stress your target server.

### Streaming

If you enable streaming then Benthos will consume the body of the response as a
line delimited list of message parts. Each part is read as an individual message
unless multipart is set to true, in which case an empty line indicates the end
of a message.

For more information about sending HTTP messages, including details on sending
multipart, please read the 'docs/using_http.md' document.`,
	}
}

//------------------------------------------------------------------------------

// StreamConfig contains fields for specifiying stream consumption behaviour.
type StreamConfig struct {
	Enabled     bool   `json:"enabled" yaml:"enabled"`
	Multipart   bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer   int    `json:"max_buffer" yaml:"max_buffer"`
	CustomDelim string `json:"custom_delimiter" yaml:"custom_delimiter"`
}

// HTTPClientConfig is configuration for the HTTPClient output type.
type HTTPClientConfig struct {
	URL            string       `json:"url" yaml:"url"`
	Verb           string       `json:"verb" yaml:"verb"`
	Payload        string       `json:"payload" yaml:"payload"`
	ContentType    string       `json:"content_type" yaml:"content_type"`
	Stream         StreamConfig `json:"stream" yaml:"stream"`
	TimeoutMS      int64        `json:"timeout_ms" yaml:"timeout_ms"`
	RetryMS        int64        `json:"retry_period_ms" yaml:"retry_period_ms"`
	NumRetries     int          `json:"retries" yaml:"retries"`
	SkipCertVerify bool         `json:"skip_cert_verify" yaml:"skip_cert_verify"`
	auth.Config    `json:",inline" yaml:",inline"`
}

// NewHTTPClientConfig creates a new HTTPClientConfig with default values.
func NewHTTPClientConfig() HTTPClientConfig {
	return HTTPClientConfig{
		URL:         "http://localhost:4195/get/stream",
		Verb:        "GET",
		Payload:     "",
		ContentType: "application/octet-stream",
		Stream: StreamConfig{
			Enabled:     false,
			Multipart:   false,
			MaxBuffer:   bufio.MaxScanTokenSize,
			CustomDelim: "",
		},
		TimeoutMS:      5000,
		RetryMS:        1000,
		NumRetries:     3,
		SkipCertVerify: false,
		Config:         auth.NewConfig(),
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
	client http.Client

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

	if h.conf.HTTPClient.SkipCertVerify {
		h.client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}

	if !h.conf.HTTPClient.Stream.Enabled {
		// Timeout should be left at zero if we are streaming.
		h.client.Timeout = time.Duration(h.conf.HTTPClient.TimeoutMS) * time.Millisecond
		go h.loop()
		return &h, nil
	}

	delim := "\n"
	if len(conf.HTTPClient.Stream.CustomDelim) > 0 {
		delim = conf.HTTPClient.Stream.CustomDelim
	}

	var resMux sync.Mutex
	var closed bool
	var res *http.Response

	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			h.stats.Incr("input.http_client.stream.constructor", 1)

			resMux.Lock()
			defer resMux.Unlock()

			if res != nil {
				res.Body.Close()
			}

			var err error
			res, err = h.doRequest()
			for err != nil && !closed {
				h.log.Errorf("HTTP stream request failed: %v\n", err)
				h.stats.Incr("input.http_client.stream.request.error", 1)

				resMux.Unlock()
				<-time.After(time.Second)
				resMux.Lock()

				res, err = h.doRequest()
			}

			if closed {
				return nil, io.EOF
			}

			return res.Body, nil
		},
		func() {
			h.stats.Incr("input.http_client.stream.on_close", 1)

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

// createRequest creates an HTTP request out of a single message.
func (h *HTTPClient) createRequest() (req *http.Request, err error) {
	var body io.Reader
	if len(h.conf.HTTPClient.Payload) > 0 {
		body = bytes.NewBufferString(h.conf.HTTPClient.Payload)
	}

	req, err = http.NewRequest(
		h.conf.HTTPClient.Verb,
		h.conf.HTTPClient.URL,
		body,
	)
	if err != nil {
		return
	}

	if contentType := h.conf.HTTPClient.ContentType; len(contentType) > 0 {
		req.Header.Add("Content-Type", contentType)
	}

	err = h.conf.HTTPClient.Config.Sign(req)
	return
}

func (h *HTTPClient) doRequest() (*http.Response, error) {
	var req *http.Request
	var res *http.Response
	var err error

	if req, err = h.createRequest(); err != nil {
		return nil, err
	}

	if res, err = h.client.Do(req); err == nil {
		if res.StatusCode < 200 || res.StatusCode > 299 {
			err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
			res.Body.Close()
		}
	}

	i, j := 0, h.conf.HTTPClient.NumRetries
	for i < j && err != nil && atomic.LoadInt32(&h.running) == 1 {
		req, _ = h.createRequest()
		select {
		case <-time.After(time.Duration(h.conf.HTTPClient.RetryMS) * time.Millisecond):
		case <-h.closeChan:
			return nil, err
		}
		if res, err = h.client.Do(req); err == nil {
			if res.StatusCode < 200 || res.StatusCode > 299 {
				err = types.ErrUnexpectedHTTPRes{Code: res.StatusCode, S: res.Status}
				res.Body.Close()
			}
		}
		i++
	}

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (h *HTTPClient) parseResponse(res *http.Response) (parts [][]byte, err error) {
	var mediaType string
	var params map[string]string
	if mediaType, params, err = mime.ParseMediaType(res.Header.Get("Content-Type")); err != nil {
		return
	}

	// Caveat: We assume this is only ever called after we have received
	// acknowledgement from any prior messages. Otherwise we would need a second
	// buffer that we rotate.
	h.buffer.Reset()

	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(res.Body, params["boundary"])
		var bufferIndex int64
		for {
			var p *multipart.Part
			if p, err = mr.NextPart(); err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return
			}

			var bytesRead int64
			if bytesRead, err = h.buffer.ReadFrom(p); err != nil {
				return
			}
			parts = append(parts, h.buffer.Bytes()[bufferIndex:bufferIndex+bytesRead])
			bufferIndex += bytesRead
		}
	} else {
		var bytesRead int64
		if bytesRead, err = h.buffer.ReadFrom(res.Body); err != nil {
			return
		}
		parts = [][]byte{h.buffer.Bytes()[:bytesRead]}
	}

	return
}

//------------------------------------------------------------------------------

// loop is an internal loop brokers incoming messages to output pipe through
// POST requests.
func (h *HTTPClient) loop() {
	defer func() {
		atomic.StoreInt32(&h.running, 0)
		h.stats.Decr("input.http_client.running", 1)

		close(h.transactions)
		close(h.closedChan)
	}()

	h.stats.Incr("input.http_client.running", 1)
	h.log.Infof("Polling for HTTP messages from: %s\n", h.conf.HTTPClient.URL)

	resOut := make(chan types.Response)

	var msgOut types.Message
	for atomic.LoadInt32(&h.running) == 1 {
		if len(msgOut.Parts) == 0 {
			var res *http.Response
			var err error

			if res, err = h.doRequest(); err != nil {
				if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
					// Hate this ^
					h.stats.Incr("input.http_client.request.timed_out", 1)
				} else {
					h.log.Errorf("Request failed: %v\n", err)
					h.stats.Incr("input.http_client.request.error", 1)
				}
			} else {
				var parts [][]byte
				if parts, err = h.parseResponse(res); err != nil {
					h.stats.Incr("input.http_client.request.parse.error", 1)
					h.log.Errorf("Failed to decode response: %v\n", err)
				} else {
					h.stats.Incr("input.http_client.request.success", 1)
					msgOut = types.Message{
						Parts: parts,
					}
				}
				res.Body.Close()
			}
			h.stats.Incr("input.http_client.count", 1)
		}

		if len(msgOut.Parts) > 0 {
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
					h.stats.Incr("input.http_client.send.error", 1)
				} else {
					msgOut = types.Message{}
					h.stats.Incr("input.http_client.send.success", 1)
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
