package io

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/http/docs/auth"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(c input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newWebsocketInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:        "websocket",
		Summary:     `Connects to a websocket server and continuously receives messages.`,
		Description: `It is possible to configure an ` + "`open_message`" + `, which when set to a non-empty string will be sent to the websocket server each time a connection is first established.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("url", "The URL to connect to.", "ws://localhost:4195/get/ws"),
			docs.FieldString("open_message", "An optional message to send to the server upon connection.").Advanced(),
			btls.FieldSpec(),
		).WithChildren(auth.FieldSpecs()...).ChildDefaultAndTypesFromStruct(input.NewWebsocketConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newWebsocketInput(conf input.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	ws, err := newWebsocketReader(conf.Websocket, log)
	if err != nil {
		return nil, err
	}
	return input.NewAsyncReader("websocket", true, input.NewAsyncPreserver(ws), mgr)
}

type websocketReader struct {
	log log.Modular

	lock *sync.Mutex

	conf    input.WebsocketConfig
	client  *websocket.Conn
	tlsConf *tls.Config
}

func newWebsocketReader(conf input.WebsocketConfig, log log.Modular) (*websocketReader, error) {
	ws := &websocketReader{
		log:  log,
		lock: &sync.Mutex{},
		conf: conf,
	}
	if conf.TLS.Enabled {
		var err error
		if ws.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return ws, nil
}

func (w *websocketReader) getWS() *websocket.Conn {
	w.lock.Lock()
	ws := w.client
	w.lock.Unlock()
	return ws
}

func (w *websocketReader) ConnectWithContext(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.client != nil {
		return nil
	}

	headers := http.Header{}

	purl, err := url.Parse(w.conf.URL)
	if err != nil {
		return err
	}

	if err := w.conf.Sign(&http.Request{
		URL:    purl,
		Header: headers,
	}); err != nil {
		return err
	}
	var client *websocket.Conn
	if w.conf.TLS.Enabled {
		dialer := websocket.Dialer{
			TLSClientConfig: w.tlsConf,
		}
		if client, _, err = dialer.Dial(w.conf.URL, headers); err != nil {
			return err

		}
	} else if client, _, err = websocket.DefaultDialer.Dial(w.conf.URL, headers); err != nil {
		return err
	}

	if len(w.conf.OpenMsg) > 0 {
		if err := client.WriteMessage(
			websocket.BinaryMessage, []byte(w.conf.OpenMsg),
		); err != nil {
			return err
		}
	}

	w.client = client
	return nil
}

func (w *websocketReader) ReadWithContext(ctx context.Context) (*message.Batch, input.AsyncAckFn, error) {
	client := w.getWS()
	if client == nil {
		return nil, nil, component.ErrNotConnected
	}

	_, data, err := client.ReadMessage()
	if err != nil {
		w.lock.Lock()
		w.client = nil
		w.lock.Unlock()
		err = component.ErrNotConnected
		return nil, nil, err
	}

	return message.QuickBatch([][]byte{data}), func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (w *websocketReader) CloseAsync() {
	w.lock.Lock()
	if w.client != nil {
		w.client.Close()
		w.client = nil
	}
	w.lock.Unlock()
}

func (w *websocketReader) WaitForClose(timeout time.Duration) error {
	return nil
}
