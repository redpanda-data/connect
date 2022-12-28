package io

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newWebsocketInput), docs.ComponentSpec{
		Name:        "websocket",
		Summary:     `Connects to a websocket server and continuously receives messages.`,
		Description: `It is possible to configure an ` + "`open_message`" + `, which when set to a non-empty string will be sent to the websocket server each time a connection is first established.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url", "The URL to connect to.", "ws://localhost:4195/get/ws"),
			docs.FieldString("open_message", "An optional message to send to the server upon connection.").Advanced(),
			btls.FieldSpec(),
		).WithChildren(httpclient.OldAuthFieldSpecs()...).ChildDefaultAndTypesFromStruct(input.NewWebsocketConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newWebsocketInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	ws, err := newWebsocketReader(conf.Websocket, mgr)
	if err != nil {
		return nil, err
	}
	return input.NewAsyncReader("websocket", input.NewAsyncPreserver(ws), mgr)
}

type websocketReader struct {
	log log.Modular
	mgr bundle.NewManagement

	lock *sync.Mutex

	conf    input.WebsocketConfig
	client  *websocket.Conn
	tlsConf *tls.Config
}

func newWebsocketReader(conf input.WebsocketConfig, mgr bundle.NewManagement) (*websocketReader, error) {
	ws := &websocketReader{
		log:  mgr.Logger(),
		mgr:  mgr,
		lock: &sync.Mutex{},
		conf: conf,
	}
	if conf.TLS.Enabled {
		var err error
		if ws.tlsConf, err = conf.TLS.Get(mgr.FS()); err != nil {
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

func (w *websocketReader) Connect(ctx context.Context) error {
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

	if err := w.conf.Sign(w.mgr.FS(), &http.Request{
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

func (w *websocketReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
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

func (w *websocketReader) Close(ctx context.Context) (err error) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.client != nil {
		err = w.client.Close()
		w.client = nil
	}
	return
}
