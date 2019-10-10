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

package reader

import (
	"context"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/client"
)

//------------------------------------------------------------------------------

// HTTPClient is a reader that continuously polls an HTTP endpoint, providing an
// optional payload each time.
type HTTPClient struct {
	payload types.Message
	client  *client.Type
}

// NewHTTPClient creates a new HTTPClient reader type.
func NewHTTPClient(payload types.Message, httpClient *client.Type) (*HTTPClient, error) {
	return &HTTPClient{
		payload: payload,
		client:  httpClient,
	}, nil
}

//------------------------------------------------------------------------------

// Connect establishes a connection.
func (h *HTTPClient) Connect() (err error) {
	return nil
}

// ConnectWithContext establishes a connection.
func (h *HTTPClient) ConnectWithContext(ctx context.Context) (err error) {
	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new HTTPClient message.
func (h *HTTPClient) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	res, err := h.client.Do(h.payload)
	if err != nil {
		if strings.Contains(err.Error(), "(Client.Timeout exceeded while awaiting headers)") {
			err = types.ErrTimeout
		}
		return nil, nil, err
	}

	var msg types.Message
	if msg, err = h.client.ParseResponse(res); err != nil {
		return nil, nil, err
	}

	return msg, noopAsyncAckFn, nil
}

// CloseAsync shuts down the HTTPClient input and stops processing requests.
func (h *HTTPClient) CloseAsync() {
}

// WaitForClose blocks until the HTTPClient input has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
