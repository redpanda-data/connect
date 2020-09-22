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

	dropEmptyBodies bool
}

// HTTPClientOptFunc changes the behaviour of an HTTPClient reader.
type HTTPClientOptFunc func(*HTTPClient)

// HTTPClientOptSetDropEmpty determines whether payloads received that are empty
// should be dropped rather than propagated as an empty benthos message.
func HTTPClientOptSetDropEmpty(dropEmpty bool) HTTPClientOptFunc {
	return func(h *HTTPClient) {
		h.dropEmptyBodies = dropEmpty
	}
}

// NewHTTPClient creates a new HTTPClient reader type.
func NewHTTPClient(payload types.Message, httpClient *client.Type, opts ...HTTPClientOptFunc) (*HTTPClient, error) {
	h := &HTTPClient{
		payload:         payload,
		client:          httpClient,
		dropEmptyBodies: true,
	}

	for _, opt := range opts {
		opt(h)
	}

	return h, nil
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

	if msg.Len() == 0 {
		return nil, nil, types.ErrTimeout
	}
	if msg.Len() == 1 && msg.Get(0).IsEmpty() && h.dropEmptyBodies {
		return nil, nil, types.ErrTimeout
	}

	return msg, noopAsyncAckFn, nil
}

// CloseAsync shuts down the HTTPClient input and stops processing requests.
func (h *HTTPClient) CloseAsync() {
	h.client.CloseAsync()
}

// WaitForClose blocks until the HTTPClient input has closed down.
func (h *HTTPClient) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
