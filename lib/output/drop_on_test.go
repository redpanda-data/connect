package output

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDropOnNothing(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test error", http.StatusForbidden)
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	childConf := NewConfig()
	childConf.Type = TypeHTTPClient
	childConf.HTTPClient.URL = ts.URL
	childConf.HTTPClient.DropOn = []int{http.StatusForbidden}

	child, err := New(childConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		child.CloseAsync()
		assert.NoError(t, child.WaitForClose(time.Second*5))
	})

	dropConf := NewDropOnConfig()
	dropConf.Error = false

	d, err := newDropOn(dropConf.DropOnConditions, child, log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		d.CloseAsync()
		assert.NoError(t, d.WaitForClose(time.Second*5))
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan response.Error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res response.Error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.EqualError(t, res.AckError(), fmt.Sprintf("%s: HTTP request returned unexpected response code (403): 403 Forbidden, Error: test error", ts.URL))
}

func TestDropOnError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test error", http.StatusForbidden)
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	childConf := NewConfig()
	childConf.Type = TypeHTTPClient
	childConf.HTTPClient.URL = ts.URL
	childConf.HTTPClient.DropOn = []int{http.StatusForbidden}

	child, err := New(childConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		child.CloseAsync()
		assert.NoError(t, child.WaitForClose(time.Second*5))
	})

	dropConf := NewDropOnConfig()
	dropConf.Error = true

	d, err := newDropOn(dropConf.DropOnConditions, child, log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		d.CloseAsync()
		assert.NoError(t, d.WaitForClose(time.Second*5))
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan response.Error)

	require.NoError(t, d.Consume(tChan))

	select {
	case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte("foobar")}), rChan):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var res response.Error
	select {
	case res = <-rChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	assert.NoError(t, res.AckError())
}

func TestDropOnBackpressureWithErrors(t *testing.T) {
	// Skip this test in most runs as it relies on awkward timers.
	t.Skip()

	var wsMut sync.Mutex
	var wsReceived []string
	var wsAllow bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsMut.Lock()
		allow := wsAllow
		wsMut.Unlock()
		if !allow {
			http.Error(w, "nope", http.StatusForbidden)
			return
		}

		upgrader := websocket.Upgrader{}

		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer ws.Close()

		for {
			_, actBytes, err := ws.ReadMessage()
			if err != nil {
				return
			}
			wsMut.Lock()
			wsReceived = append(wsReceived, string(actBytes))
			wsMut.Unlock()
		}
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	childConf := NewConfig()
	childConf.Type = TypeWebsocket
	childConf.Websocket.URL = "ws://" + strings.TrimPrefix(ts.URL, "http://")

	child, err := New(childConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		child.CloseAsync()
		assert.NoError(t, child.WaitForClose(time.Second*5))
	})

	dropConf := NewDropOnConfig()
	dropConf.BackPressure = "100ms"

	d, err := newDropOn(dropConf.DropOnConditions, child, log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		d.CloseAsync()
		assert.NoError(t, d.WaitForClose(time.Second*5))
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan response.Error)

	require.NoError(t, d.Consume(tChan))

	sendAndGet := func(msg string, expErr string) {
		t.Helper()

		select {
		case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(msg)}), rChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		var res response.Error
		select {
		case res = <-rChan:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if expErr == "" {
			assert.NoError(t, res.AckError())
		} else {
			assert.EqualError(t, res.AckError(), expErr)
		}
	}

	sendAndGet("first", "experienced back pressure beyond: 100ms")
	sendAndGet("second", "experienced back pressure beyond: 100ms")
	wsMut.Lock()
	wsAllow = true
	wsMut.Unlock()
	<-time.After(time.Second)

	sendAndGet("third", "")
	sendAndGet("fourth", "")

	<-time.After(time.Second)
	wsMut.Lock()
	assert.Equal(t, []string{"third", "fourth"}, wsReceived)
	wsMut.Unlock()
}

func TestDropOnDisconnectBackpressureNoErrors(t *testing.T) {
	// Skip this test in most runs as it relies on awkward timers.
	t.Skip()

	var wsReceived []string
	var ws *websocket.Conn
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}

		var err error
		if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
			return
		}
		defer ws.Close()

		for {
			_, actBytes, err := ws.ReadMessage()
			if err != nil {
				return
			}
			wsReceived = append(wsReceived, string(actBytes))
		}
	}))
	t.Cleanup(func() {
		ts.Close()
	})

	childConf := NewConfig()
	childConf.Type = TypeWebsocket
	childConf.Websocket.URL = "ws://" + strings.TrimPrefix(ts.URL, "http://")

	child, err := New(childConf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		child.CloseAsync()
		assert.NoError(t, child.WaitForClose(time.Second*5))
	})

	dropConf := NewDropOnConfig()
	dropConf.Error = true
	dropConf.BackPressure = "100ms"

	d, err := newDropOn(dropConf.DropOnConditions, child, log.Noop(), metrics.Noop())
	require.NoError(t, err)
	t.Cleanup(func() {
		d.CloseAsync()
		assert.NoError(t, d.WaitForClose(time.Second*5))
	})

	tChan := make(chan message.Transaction)
	rChan := make(chan response.Error)

	require.NoError(t, d.Consume(tChan))

	sendAndGet := func(msg string, expErr string) {
		t.Helper()

		select {
		case tChan <- message.NewTransaction(message.QuickBatch([][]byte{[]byte(msg)}), rChan):
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		var res response.Error
		select {
		case res = <-rChan:
		case <-time.After(time.Second):
			t.Fatal("timed out")
		}

		if expErr == "" {
			assert.NoError(t, res.AckError())
		} else {
			assert.EqualError(t, res.AckError(), expErr)
		}
	}

	sendAndGet("first", "")
	sendAndGet("second", "")

	ts.Close()
	ws.Close()
	<-time.After(time.Second)

	sendAndGet("third", "")
	sendAndGet("fourth", "")

	<-time.After(time.Second)

	assert.Equal(t, []string{"first", "second"}, wsReceived)
}
