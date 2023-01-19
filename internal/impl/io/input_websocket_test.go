package io

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestWebsocketBasic(t *testing.T) {
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

		for _, msg := range expMsgs {
			if err = ws.WriteMessage(websocket.BinaryMessage, []byte(msg)); err != nil {
				t.Error(err)
			}
		}
	}))

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	wsURL.Scheme = "ws"

	pConf, err := websocketInputSpec().ParseYAML(fmt.Sprintf(`
url: %v
`, wsURL.String()), nil)
	require.NoError(t, err)

	m, err := newWebsocketReaderFromParsed(pConf, mock.NewManager())
	require.NoError(t, err)

	ctx := context.Background()

	if err = m.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	for _, exp := range expMsgs {
		var actMsg message.Batch
		if actMsg, _, err = m.ReadBatch(ctx); err != nil {
			t.Error(err)
		} else if act := string(actMsg.Get(0).AsBytes()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
	}

	require.NoError(t, m.Close(ctx))
}

func TestWebsocketOpenMsg(t *testing.T) {
	expMsgs := []string{
		"foo",
		"bar",
		"baz",
	}

	testHandler := func(expMsgType int, w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		var ws *websocket.Conn
		var err error
		if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
			return
		}

		defer ws.Close()

		msgType, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := "hello world", string(data); exp != act {
			t.Errorf("Wrong open message: %v != %v", act, exp)
		}
		if msgType != expMsgType {
			t.Errorf("Wrong open message type: %v != %v", msgType, expMsgType)
		}

		for _, msg := range expMsgs {
			if err = ws.WriteMessage(websocket.BinaryMessage, []byte(msg)); err != nil {
				t.Error(err)
			}
		}
	}

	tests := []struct {
		handler       func(expMsgType int, w http.ResponseWriter, r *http.Request)
		openMsgType   wsOpenMsgType
		wsOpenMsgType int
		errStr        string
	}{
		{
			handler:       testHandler,
			openMsgType:   wsOpenMsgTypeBinary,
			wsOpenMsgType: websocket.BinaryMessage,
		},
		{
			handler:       testHandler,
			openMsgType:   wsOpenMsgTypeText,
			wsOpenMsgType: websocket.TextMessage,
		},
		{
			// Use a simplified handler to avoid the blocking call to `ws.ReadMessage()` when no OpenMsg gets sent
			handler: func(_ int, w http.ResponseWriter, r *http.Request) {
				upgrader := websocket.Upgrader{}

				var ws *websocket.Conn
				var err error
				if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
					return
				}

				ws.Close()
			},
			openMsgType: "foobar",
			errStr:      "unrecognised open_message_type: foobar",
		},
	}

	for id, test := range tests {
		t.Run(strconv.Itoa(id), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { test.handler(test.wsOpenMsgType, w, r) }))
			t.Cleanup(server.Close)

			wsURL, err := url.Parse(server.URL)
			require.NoError(t, err)

			wsURL.Scheme = "ws"

			pConf, err := websocketInputSpec().ParseYAML(fmt.Sprintf(`
url: %v
open_message: "hello world"
open_message_type: %v
`, wsURL.String(), test.openMsgType), nil)
			require.NoError(t, err)

			m, err := newWebsocketReaderFromParsed(pConf, mock.NewManager())
			require.NoError(t, err)

			ctx, done := context.WithTimeout(context.Background(), 100*time.Millisecond)
			t.Cleanup(func() { require.NoError(t, m.Close(ctx)) })
			t.Cleanup(done)

			if err = m.Connect(ctx); err != nil {
				if test.errStr != "" {
					require.ErrorContains(t, err, test.errStr)
					return
				}

				t.Fatal(err)
			}

			for _, exp := range expMsgs {
				var actMsg message.Batch
				if actMsg, _, err = m.ReadBatch(ctx); err != nil {
					t.Error(err)
				} else if act := string(actMsg.Get(0).AsBytes()); act != exp {
					t.Errorf("Wrong result: %v != %v", act, exp)
				}
			}

			require.NoError(t, m.Close(ctx))
		})
	}
}

func TestWebsocketClose(t *testing.T) {
	closeChan := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		var ws *websocket.Conn
		var err error
		if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
			return
		}

		defer ws.Close()
		<-closeChan
	}))

	wsURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	wsURL.Scheme = "ws"

	pConf, err := websocketInputSpec().ParseYAML(fmt.Sprintf(`
url: %v
`, wsURL.String()), nil)
	require.NoError(t, err)

	m, err := newWebsocketReaderFromParsed(pConf, mock.NewManager())
	require.NoError(t, err)

	ctx := context.Background()

	if err = m.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		require.NoError(t, m.Close(ctx))
		wg.Done()
	}()

	if _, _, err = m.ReadBatch(ctx); err != component.ErrTypeClosed && err != component.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, component.ErrTypeClosed)
	}

	wg.Wait()
	close(closeChan)
}
