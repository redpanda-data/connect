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
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
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

		for _, msg := range expMsgs {
			if err = ws.WriteMessage(websocket.BinaryMessage, []byte(msg)); err != nil {
				t.Error(err)
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

	m, err := NewWebsocket(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, exp := range expMsgs {
		var actMsg types.Message
		if actMsg, err = m.Read(); err != nil {
			t.Error(err)
		} else if act := string(actMsg.Get(0).Get()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		if err = m.Acknowledge(nil); err != nil {
			t.Error(err)
		}
	}

	m.CloseAsync()
	if err = m.WaitForClose(time.Second); err != nil {
		t.Error(err)
	}
}

func TestWebsocketOpenMsg(t *testing.T) {
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

		_, data, err := ws.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if exp, act := "hello world", string(data); exp != act {
			t.Errorf("Wrong open message: %v != %v", act, exp)
		}

		for _, msg := range expMsgs {
			if err = ws.WriteMessage(websocket.BinaryMessage, []byte(msg)); err != nil {
				t.Error(err)
			}
		}
	}))

	conf := NewWebsocketConfig()
	conf.OpenMsg = "hello world"
	if wsURL, err := url.Parse(server.URL); err != nil {
		t.Fatal(err)
	} else {
		wsURL.Scheme = "ws"
		conf.URL = wsURL.String()
	}

	m, err := NewWebsocket(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
		t.Fatal(err)
	}

	for _, exp := range expMsgs {
		var actMsg types.Message
		if actMsg, err = m.Read(); err != nil {
			t.Error(err)
		} else if act := string(actMsg.Get(0).Get()); act != exp {
			t.Errorf("Wrong result: %v != %v", act, exp)
		}
		if err = m.Acknowledge(nil); err != nil {
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

		defer ws.Close()
		<-closeChan
	}))

	conf := NewWebsocketConfig()
	if wsURL, err := url.Parse(server.URL); err != nil {
		t.Fatal(err)
	} else {
		wsURL.Scheme = "ws"
		conf.URL = wsURL.String()
	}

	m, err := NewWebsocket(conf, log.New(os.Stdout, log.Config{LogLevel: "NONE"}), metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = m.Connect(); err != nil {
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

	if _, err = m.Read(); err != types.ErrTypeClosed && err != types.ErrNotConnected {
		t.Errorf("Wrong error: %v != %v", err, types.ErrTypeClosed)
	}

	wg.Wait()
	close(closeChan)
}
