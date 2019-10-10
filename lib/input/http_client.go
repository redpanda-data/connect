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
	"context"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPClient] = TypeSpec{
		constructor: NewHTTPClient,
		description: `
The HTTP client input type connects to a server and continuously performs
requests for a single message.

You should set a sensible retry period and max backoff so as to not flood your
target server.

The URL and header values of this type can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

### Streaming

If you enable streaming then Benthos will consume the body of the response as a
line delimited list of message parts. Each part is read as an individual message
unless multipart is set to true, in which case an empty line indicates the end
of a message.`,
	}
}

//------------------------------------------------------------------------------

// StreamConfig contains fields for specifying consumption behaviour when the
// body of a request is a constant stream of bytes.
type StreamConfig struct {
	Enabled   bool   `json:"enabled" yaml:"enabled"`
	Reconnect bool   `json:"reconnect" yaml:"reconnect"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// HTTPClientConfig contains configuration for the HTTPClient output type.
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

// HTTPClient is an input type that continuously makes HTTP requests and reads
// the response bodies as message payloads.
type HTTPClient struct {
	conf Config

	client  *client.Type
	payload types.Message
}

// NewHTTPClient creates a new HTTPClient input type.
func NewHTTPClient(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	h := HTTPClient{
		conf: conf,
	}

	if h.conf.HTTPClient.Stream.Enabled {
		// Timeout should be left at zero if we are streaming.
		h.conf.HTTPClient.Timeout = ""
	}
	if len(h.conf.HTTPClient.Payload) > 0 {
		h.payload = message.New([][]byte{[]byte(h.conf.HTTPClient.Payload)})
	}

	var err error
	if h.client, err = client.New(
		h.conf.HTTPClient.Config,
		client.OptSetLogger(log.NewModule(".client")),
		client.OptSetManager(mgr),
		client.OptSetStats(metrics.Namespaced(stats, "client")),
	); err != nil {
		return nil, err
	}

	if !h.conf.HTTPClient.Stream.Enabled {
		hc, err := reader.NewHTTPClient(h.payload, h.client)
		if err != nil {
			return nil, err
		}
		return NewAsyncReader(TypeHTTPClient, true, reader.NewAsyncPreserver(hc), log, stats)
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
		mStrmConstructor = stats.GetCounter("stream.constructor")
		mStrmReqErr      = stats.GetCounter("stream.request.error")
		mStrnOnClose     = stats.GetCounter("stream.on_close")
	)

	rdr, err := reader.NewLinesWithContext(
		func(ctx context.Context) (io.Reader, error) {
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
				log.Errorf("HTTP stream request failed: %v\n", err)
				mStrmReqErr.Incr(1)

				resMux.Unlock()
				select {
				case <-time.After(time.Second):
				case <-ctx.Done():
					resMux.Lock()
					return nil, types.ErrTimeout
				}
				resMux.Lock()

				res, err = h.doRequest()
			}

			if closed {
				return nil, io.EOF
			}

			conn = true
			return res.Body, nil
		},
		func(ctx context.Context) {
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

	return NewAsyncReader(
		TypeHTTPClient,
		true,
		reader.NewAsyncPreserver(rdr),
		log, stats,
	)
}

//------------------------------------------------------------------------------

func (h *HTTPClient) doRequest() (*http.Response, error) {
	return h.client.Do(h.payload)
}

func (h *HTTPClient) parseResponse(res *http.Response) (types.Message, error) {
	msg, err := h.client.ParseResponse(res)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

//------------------------------------------------------------------------------
