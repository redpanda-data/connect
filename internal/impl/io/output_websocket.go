package io

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	btls "github.com/benthosdev/benthos/v4/internal/tls"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newWebsocketOutput), docs.ComponentSpec{
		Name:    "websocket",
		Summary: `Sends messages to an HTTP server via a websocket connection.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url", "The URL to connect to."),
			btls.FieldSpec(),
		).WithChildren(httpclient.OldAuthFieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewWebsocketConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newWebsocketOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newWebsocketWriter(conf.Websocket, mgr)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("websocket", 1, w, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(a), nil
}

type websocketWriter struct {
	log log.Modular
	mgr bundle.NewManagement

	lock *sync.Mutex

	conf    output.WebsocketConfig
	client  *websocket.Conn
	tlsConf *tls.Config
}

func newWebsocketWriter(conf output.WebsocketConfig, mgr bundle.NewManagement) (*websocketWriter, error) {
	ws := &websocketWriter{
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

func (w *websocketWriter) getWS() *websocket.Conn {
	w.lock.Lock()
	ws := w.client
	w.lock.Unlock()
	return ws
}

func (w *websocketWriter) Connect(ctx context.Context) error {
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

	go func(c *websocket.Conn) {
		for {
			if _, _, cerr := c.NextReader(); cerr != nil {
				c.Close()
				break
			}
		}
	}(client)

	w.client = client
	return nil
}

func (w *websocketWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	client := w.getWS()
	if client == nil {
		return component.ErrNotConnected
	}

	err := msg.Iter(func(i int, p *message.Part) error {
		return client.WriteMessage(websocket.BinaryMessage, p.AsBytes())
	})
	if err != nil {
		w.lock.Lock()
		w.client = nil
		w.lock.Unlock()
		if errors.Is(err, websocket.ErrCloseSent) {
			return component.ErrNotConnected
		}
		return err
	}
	return nil
}

func (w *websocketWriter) Close(ctx context.Context) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	var err error
	if w.client != nil {
		err = w.client.Close()
		w.client = nil
	}
	return err
}
