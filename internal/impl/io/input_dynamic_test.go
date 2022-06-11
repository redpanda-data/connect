package io

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	bmock "github.com/benthosdev/benthos/v4/internal/manager/mock"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestDynamicInputAPI(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*10)
	defer done()

	gMux := mux.NewRouter()

	mgr := bmock.NewManager()
	mgr.OnRegisterEndpoint = func(path string, h http.HandlerFunc) {
		gMux.HandleFunc(path, h)
	}

	conf := input.NewConfig()
	conf.Type = "dynamic"

	i, err := mgr.NewInput(conf)
	require.NoError(t, err)

	req := httptest.NewRequest("GET", "/inputs", nil)
	res := httptest.NewRecorder()
	gMux.ServeHTTP(res, req)

	assert.Equal(t, 200, res.Code)
	assert.Equal(t, `{}`, res.Body.String())

	fooConf := `
generate:
  interval: 100ms
  mapping: 'root.source = "foo"'
`
	req = httptest.NewRequest("POST", "/inputs/foo", bytes.NewBuffer([]byte(fooConf)))
	res = httptest.NewRecorder()
	gMux.ServeHTTP(res, req)

	assert.Equal(t, 200, res.Code)

	select {
	case ts, open := <-i.TransactionChan():
		require.True(t, open)
		assert.Equal(t, `{"source":"foo"}`, string(ts.Payload.Get(0).Get()))
		require.NoError(t, ts.Ack(ctx, nil))
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	req = httptest.NewRequest("GET", "/inputs/foo", nil)
	res = httptest.NewRecorder()
	gMux.ServeHTTP(res, req)

	assert.Equal(t, 200, res.Code)
	assert.Equal(t, `label: ""
generate:
    mapping: root.source = "foo"
    interval: 100ms
    count: 0
`, res.Body.String())

	i.CloseAsync()
	require.NoError(t, i.WaitForClose(time.Second))
}
