package stream_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"

	_ "github.com/benthosdev/benthos/v4/public/components/all"
)

func TestTypeConstruction(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "nanomsg"
	conf.Input.Nanomsg.PollTimeout = "100ms"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "nanomsg"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)

	assert.NoError(t, strm.Stop(time.Minute))

	newMgr, err = manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)

	require.NoError(t, strm.Stop(time.Minute))
}

func TestTypeCloseGracefully(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "http_server"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "http_server"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(time.Minute))
}

func TestTypeCloseOrdered(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "http_server"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "http_server"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopOrdered(time.Minute))
}

func TestTypeCloseUnordered(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "http_server"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "http_server"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(time.Minute))
}

type mockAPIReg struct {
	server *httptest.Server
}

func (ar mockAPIReg) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	ar.server.Config.Handler = h
}

func (ar mockAPIReg) Close() {
	ar.server.Close()
}

func newMockAPIReg() mockAPIReg {
	return mockAPIReg{
		server: httptest.NewServer(nil),
	}
}

func validateHealthCheckResponse(t *testing.T, serverURL, expectedResponse string) {
	t.Helper()

	res, err := http.Get(serverURL + "/ready")
	require.NoError(t, err)
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	require.Equal(t, expectedResponse, string(data))
}

func TestHealthCheck(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "http_client"
	conf.Output.Type = "drop"

	mockAPIReg := newMockAPIReg()
	defer mockAPIReg.Close()

	newMgr, err := manager.New(manager.NewResourceConfig(), manager.OptSetAPIReg(&mockAPIReg))
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer done()
	for !strm.IsReady() {
		select {
		case <-ctx.Done():
			t.Fatalf("Failed to start stream")
		case <-time.After(10 * time.Millisecond):
		}
	}

	validateHealthCheckResponse(t, mockAPIReg.server.URL, "OK")

	assert.NoError(t, strm.StopUnordered(time.Minute))

	validateHealthCheckResponse(t, mockAPIReg.server.URL, "input not connected\n")
}
