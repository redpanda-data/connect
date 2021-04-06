package input

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPtServerBasic(t *testing.T) {
	// Ask the OS for a random port. There's a small chance that the OS might
	// allocate this port to another process before it gets locked by the HTTP
	// server, but it should be OK just for a test.
	ln, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	err = ln.Close()
	require.NoError(t, err)

	conf := NewConfig()
	conf.HTTPServer.Address = ln.Addr().String()
	conf.HTTPServer.Path = "/"

	server, err := NewHTTPServer(conf, nil, log.Noop(), metrics.Noop())
	require.NoError(t, err)

	defer func() {
		server.CloseAsync()
		assert.NoError(t, server.WaitForClose(time.Second))
	}()

	dummyPath := "/across/the/rainbow/bridge"
	dummyQuery := url.Values{"foo": []string{"bar"}}
	serverURL := url.URL{
		Scheme:   "http",
		Host:     server.(*HTTPServer).server.Addr,
		Path:     dummyPath,
		RawQuery: dummyQuery.Encode(),
	}
	dummyData := []byte("a bunch of jolly leprechauns await")
	go func() {
		resp, cerr := http.Post(serverURL.String(), "text/plain", bytes.NewReader(dummyData))
		require.NoError(t, cerr)
		defer resp.Body.Close()
	}()

	readNextMsg := func() (types.Message, error) {
		var tran types.Transaction
		select {
		case tran = <-server.TransactionChan():
			select {
			case tran.ResponseChan <- response.NewAck():
			case <-time.After(time.Second):
				return nil, errors.New("timed out")
			}
		case <-time.After(time.Second):
			return nil, errors.New("timed out")
		}
		return tran.Payload, nil
	}

	msg, err := readNextMsg()
	require.NoError(t, err)
	assert.Equal(t, dummyData, message.GetAllBytes(msg)[0])

	meta := msg.Get(0).Metadata()
	assert.Equal(t, dummyPath, meta.Get("http_server_request_path"))
	assert.Regexp(t, "^Go-http-client/", meta.Get("http_server_user_agent"))
	// Make sure query params are set in the metadata
	assert.Contains(t, "bar", meta.Get("foo"))
}
