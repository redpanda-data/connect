package io

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestWebsocketOutputBasic(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	expMsgs := []string{
		"foo",
		"bar",
		"baz",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		var ws *websocket.Conn
		var err error
		if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
			return
		}

		defer ws.Close()

		var actBytes []byte
		for _, exp := range expMsgs {
			if _, actBytes, err = ws.ReadMessage(); err != nil {
				t.Error(err)
			} else if act := string(actBytes); act != exp {
				t.Errorf("Wrong msg contents: %v != %v", act, exp)
			}
		}
	}))

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	wsURL.Scheme = "ws"

	conf := parseYAMLOutputConf(t, `
websocket:
  url: %v
`, wsURL.String())

	m, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	tChan := make(chan message.Transaction)
	require.NoError(t, m.Consume(tChan))

	for _, msg := range expMsgs {
		require.NoError(t, writeBatchToChan(ctx, t, message.QuickBatch([][]byte{[]byte(msg)}), tChan))
	}

	m.TriggerCloseNow()
	require.NoError(t, m.WaitForClose(ctx))
}

func TestWebsocketOutputClose(t *testing.T) {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		var ws *websocket.Conn
		var err error
		if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
			return
		}

		ws.Close()
	}))

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	wsURL.Scheme = "ws"

	conf := parseYAMLOutputConf(t, `
websocket:
  url: %v
`, wsURL.String())

	m, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	tChan := make(chan message.Transaction)
	require.NoError(t, m.Consume(tChan))

	m.TriggerCloseNow()
	require.NoError(t, m.WaitForClose(ctx))
}
