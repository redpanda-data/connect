package reader

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/http/auth"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
	"github.com/gorilla/websocket"
)

//------------------------------------------------------------------------------

// WebsocketConfig contains configuration fields for the Websocket input type.
type WebsocketConfig struct {
	URL         string `json:"url" yaml:"url"`
	OpenMsg     string `json:"open_message" yaml:"open_message"`
	auth.Config `json:",inline" yaml:",inline"`
	TLS         btls.Config `json:"tls" yaml:"tls"`
}

// NewWebsocketConfig creates a new WebsocketConfig with default values.
func NewWebsocketConfig() WebsocketConfig {
	return WebsocketConfig{
		URL:     "ws://localhost:4195/get/ws",
		OpenMsg: "",
		Config:  auth.NewConfig(),
		TLS:     btls.NewConfig(),
	}
}

//------------------------------------------------------------------------------

// Websocket is an input type that reads Websocket messages.
type Websocket struct {
	log   log.Modular
	stats metrics.Type

	lock *sync.Mutex

	conf    WebsocketConfig
	client  *websocket.Conn
	tlsConf *tls.Config
}

// NewWebsocket creates a new Websocket input type.
func NewWebsocket(
	conf WebsocketConfig,
	log log.Modular,
	stats metrics.Type,
) (*Websocket, error) {
	ws := &Websocket{
		log:   log,
		stats: stats,
		lock:  &sync.Mutex{},
		conf:  conf,
	}
	if conf.TLS.Enabled {
		var err error
		if ws.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return ws, nil
}

//------------------------------------------------------------------------------

func (w *Websocket) getWS() *websocket.Conn {
	w.lock.Lock()
	ws := w.client
	w.lock.Unlock()
	return ws
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to a Websocket server.
func (w *Websocket) ConnectWithContext(ctx context.Context) error {
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

//------------------------------------------------------------------------------

// ReadWithContext attempts to read a new message from the websocket.
func (w *Websocket) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	client := w.getWS()
	if client == nil {
		return nil, nil, types.ErrNotConnected
	}

	_, data, err := client.ReadMessage()
	if err != nil {
		w.lock.Lock()
		w.client = nil
		w.lock.Unlock()
		err = types.ErrNotConnected
		return nil, nil, err
	}

	return message.QuickBatch([][]byte{data}), noopAsyncAckFn, nil
}

// CloseAsync shuts down the Websocket input and stops reading messages.
func (w *Websocket) CloseAsync() {
	w.lock.Lock()
	if w.client != nil {
		w.client.Close()
		w.client = nil
	}
	w.lock.Unlock()
}

// WaitForClose blocks until the Websocket input has closed down.
func (w *Websocket) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
