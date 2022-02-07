package writer

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/gorilla/websocket"
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

		var actBytes []byte
		for _, exp := range expMsgs {
			if _, actBytes, err = ws.ReadMessage(); err != nil {
				t.Error(err)
			} else if act := string(actBytes); act != exp {
				t.Errorf("Wrong msg contents: %v != %v", act, exp)
			}
		}
	}))

	conf := NewWebsocketConfig()
	if wsURL, err := url.Parse(server.URL); err != nil {
		t.Fatal(err)
	} else {
		wsURL.Scheme = "ws"
		conf.URL = wsURL.String()
	}

	m, err := NewWebsocket(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.ConnectWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	for _, msg := range expMsgs {
		if err = m.WriteWithContext(context.Background(), message.QuickBatch([][]byte{[]byte(msg)})); err != nil {
			t.Error(err)
		}
	}

	m.CloseAsync()
	if err = m.WaitForClose(time.Second); err != nil {
		t.Error(err)
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

		ws.Close()
	}))

	conf := NewWebsocketConfig()
	if wsURL, err := url.Parse(server.URL); err != nil {
		t.Fatal(err)
	} else {
		wsURL.Scheme = "ws"
		conf.URL = wsURL.String()
	}

	m, err := NewWebsocket(conf, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	if err = m.ConnectWithContext(context.Background()); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		m.CloseAsync()
		if cErr := m.WaitForClose(time.Second); cErr != nil {
			t.Error(cErr)
		}
		wg.Done()
	}()

	wg.Wait()
	close(closeChan)
}
