package httpclient

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"

	"github.com/benthosdev/benthos/v4/internal/log"

	"github.com/stretchr/testify/require"
)

func TestNewRequestLog(t *testing.T) {
	t.Run("nil base", func(t *testing.T) {
		httpRoundTrip, err := newRequestLog(nil, log.Noop(), "")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)
	})

	t.Run("nil log", func(t *testing.T) {
		httpRoundTrip, err := newRequestLog(http.DefaultTransport, nil, "INFO")
		require.Nil(t, httpRoundTrip)
		require.Error(t, err)
	})

	t.Run("enable with log", func(t *testing.T) {
		httpRoundTrip, err := newRequestLog(http.DefaultTransport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)
	})
}

func TestToSimpleMap(t *testing.T) {
	t.Run("empty header", func(t *testing.T) {
		m := toSimpleMap(http.Header{})
		require.EqualValues(t, map[string]string{}, m)
	})

	t.Run("values header", func(t *testing.T) {
		m := toSimpleMap(http.Header{
			"Content Type": []string{"application/json", "charset=utf-8"},
		})
		require.EqualValues(t, map[string]string{
			"Content Type": "application/json charset=utf-8",
		}, m)
	})
}

type mockHTTPRoundTrip struct {
	Error         error
	CallRoundTrip func(request *http.Request) (*http.Response, error)
}

var _ http.RoundTripper = (*mockHTTPRoundTrip)(nil)

func newMockHTTPRoundTripper() *mockHTTPRoundTrip {
	return &mockHTTPRoundTrip{}
}

func newMockHTTPRoundTripperWithErr(err error) *mockHTTPRoundTrip {
	return &mockHTTPRoundTrip{
		Error: err,
	}
}

func (m *mockHTTPRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	if m.Error != nil {
		return nil, m.Error
	}

	if request == nil {
		return nil, fmt.Errorf("nil *http.Request")
	}

	// by default, non-nil error returned with empty http.Response
	if m.CallRoundTrip == nil {
		return &http.Response{}, nil
	}

	return m.CallRoundTrip(request)
}

func TestRoundTripper_RoundTrip(t *testing.T) {
	t.Run("nil request", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		resp, err := httpRoundTrip.RoundTrip(nil)
		require.Nil(t, resp)
		require.Error(t, err)
	})

	t.Run("nil request body", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("non-empty request body", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("non-empty request body but error read", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(&Buf{err: fmt.Errorf("mock error buffer")}),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("non-empty request body but error close", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: &Closer{
				buf: bytes.NewBufferString(`{"foo":"bar"}`),
				err: fmt.Errorf("mock error buffer"),
			},
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("non-empty request body no valid json", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`<html></html>`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("failed http.RoundTripper", func(t *testing.T) {
		expectedErr := fmt.Errorf("failed to fetch data")
		transport := newMockHTTPRoundTripperWithErr(expectedErr)

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, respErr := httpRoundTrip.RoundTrip(req)
		require.Nil(t, resp)
		require.ErrorIs(t, respErr, expectedErr)
	})

	t.Run("not-nil response with empty resp body", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		transport.CallRoundTrip = func(request *http.Request) (*http.Response, error) {
			return &http.Response{}, nil
		}

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("not-nil response non-empty resp body", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		transport.CallRoundTrip = func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Body: io.NopCloser(bytes.NewBufferString(`{"FOO":"BAR"}`)),
			}, nil
		}

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("not-nil response body fail on close", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		transport.CallRoundTrip = func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Body: &Closer{
					buf: bytes.NewBufferString(`{"FOO":"BAR"}`),
					err: fmt.Errorf("mock error close resp body"),
				},
			}, nil
		}

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("not-nil response non-empty resp body with non-valid json", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		transport.CallRoundTrip = func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Body: io.NopCloser(bytes.NewBufferString(`<FOO>BAR</FOO>`)),
			}, nil
		}

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("not-nil response non-empty resp body but failed to read", func(t *testing.T) {
		transport := newMockHTTPRoundTripper()

		transport.CallRoundTrip = func(request *http.Request) (*http.Response, error) {
			return &http.Response{
				Body: io.NopCloser(&Buf{err: fmt.Errorf("failed to read response body buffer")}),
			}, nil
		}

		httpRoundTrip, err := newRequestLog(transport, log.Noop(), "INFO")
		require.NotNil(t, httpRoundTrip)
		require.NoError(t, err)

		req := &http.Request{
			Body: io.NopCloser(bytes.NewBufferString(`{"foo":"bar"}`)),
		}

		resp, err := httpRoundTrip.RoundTrip(req)
		require.NotNil(t, resp)
		require.NoError(t, err)
	})

	t.Run("test all log level", func(t *testing.T) {
		// cannot test FATAL because it really os.Exit(1) which make unit test error
		levels := []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR"}

		for _, level := range levels {
			t.Run(level, func(t *testing.T) {
				transport := newMockHTTPRoundTripper()
				httpRoundTrip, err := newRequestLog(transport, log.Noop(), level)
				require.NotNil(t, httpRoundTrip)
				require.NoError(t, err)

				resp, err := httpRoundTrip.RoundTrip(nil)
				require.Nil(t, resp)
				require.Error(t, err)
			})
		}
	})
}

type Buf struct {
	err error
}

func (b *Buf) Read(p []byte) (n int, err error) {
	if b.err != nil {
		return 0, b.err
	}

	return len(p), nil
}

type Closer struct {
	buf io.Reader
	err error
}

var _ io.ReadCloser = (*Closer)(nil)

func (c *Closer) Read(p []byte) (n int, err error) {
	return c.buf.Read(p)
}

func (c *Closer) Close() error {
	return c.err
}
