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
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/stream"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestTypeConstruction(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "generate"
	conf.Input.Generate.Mapping = "root = {}"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "drop"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	assert.NoError(t, strm.Stop(ctx))

	newMgr, err = manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)

	require.NoError(t, strm.Stop(ctx))
}

func TestStreamCloseUngraceful(t *testing.T) {
	t.Parallel()

	conf := stream.NewConfig()
	conf.Input.Type = "generate"
	conf.Input.Generate.Mapping = `root = "hello world"`
	conf.Input.Generate.Interval = ""
	conf.Output.Type = "inproc"
	conf.Output.Inproc = "foo"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)

	tChan, err := newMgr.GetPipe("foo")
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()

	var tTmp message.Transaction
	select {
	case tTmp = <-tChan:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Len(t, tTmp.Payload, 1)

	pBytes := tTmp.Payload[0].AsBytes()
	assert.Equal(t, "hello world", string(pBytes))

	assert.Error(t, strm.Stop(ctx))
}

func TestTypeCloseGracefully(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "generate"
	conf.Input.Generate.Mapping = "root = {}"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "drop"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(ctx))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(ctx))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopGracefully(ctx))
}

func TestTypeCloseUnordered(t *testing.T) {
	conf := stream.NewConfig()
	conf.Input.Type = "generate"
	conf.Input.Generate.Mapping = "root = {}"
	conf.Buffer.Type = "memory"
	conf.Output.Type = "drop"

	newMgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	ctx, done := context.WithTimeout(context.Background(), time.Minute)
	defer done()

	strm, err := stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(ctx))

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(ctx))

	conf.Pipeline.Processors = []processor.Config{
		processor.NewConfig(),
	}

	strm, err = stream.New(conf, newMgr)
	require.NoError(t, err)
	assert.NoError(t, strm.StopUnordered(ctx))
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
	conf.Input.Type = "generate"
	conf.Input.Generate.Mapping = "root = {}"
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

	stopCtx, stopDone := context.WithTimeout(context.Background(), time.Minute)
	defer stopDone()

	assert.NoError(t, strm.StopUnordered(stopCtx))

	validateHealthCheckResponse(t, mockAPIReg.server.URL, "Stream terminated\n")
}
