package io

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"net/url"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/config"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/httpclient"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

type wsOpenMsgType string

const (
	// wsOpenMsgTypeBinary sets the type of open_message to binary.
	wsOpenMsgTypeBinary wsOpenMsgType = "binary"
	// wsOpenMsgTypeText sets the type of open_message to text (UTF-8 encoded text data).
	wsOpenMsgTypeText wsOpenMsgType = "text"
)

func websocketInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Connects to a websocket server and continuously receives messages.").
		Description(`It is possible to configure an `+"`open_message`"+`, which when set to a non-empty string will be sent to the websocket server each time a connection is first established.`).
		Fields(
			service.NewURLField("url").
				Description("The URL to connect to.").
				Example("ws://localhost:4195/get/ws"),
			service.NewStringField("open_message").
				Description("An optional message to send to the server upon connection.").
				Advanced().Optional(),
			service.NewStringAnnotatedEnumField("open_message_type", map[string]string{
				string(wsOpenMsgTypeBinary): "Binary data open_message.",
				string(wsOpenMsgTypeText):   "Text data open_message. The text message payload is interpreted as UTF-8 encoded text data.",
			}).Description("An optional flag to indicate the data type of open_message.").
				Advanced().Default(string(wsOpenMsgTypeBinary)),
			service.NewAutoRetryNacksToggleField(),
			service.NewTLSToggledField("tls"),
		).
		Fields(config.AsyncOptsFields()...).
		Fields(httpclient.AuthFieldSpecs()...)
}

func init() {
	err := service.RegisterBatchInput(
		"websocket", websocketInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (in service.BatchInput, err error) {
			oldMgr := interop.UnwrapManagement(mgr)
			var r input.Async
			if r, err = newWebsocketReaderFromParsed(conf, oldMgr); err != nil {
				return
			}

			var opts []func(*input.AsyncReader)
			if opts, err = config.AsyncOptsFromParsed(conf); err != nil {
				return
			}

			if autoRetry, _ := conf.FieldBool(service.AutoRetryNacksToggleFieldName); autoRetry {
				r = input.NewAsyncPreserver(r)
			}

			var i input.Streamed
			if i, err = input.NewAsyncReader("websocket", r, oldMgr, opts...); err != nil {
				return
			}
			in = interop.NewUnwrapInternalInput(i)
			return
		})
	if err != nil {
		panic(err)
	}
}

type websocketReader struct {
	log log.Modular
	mgr bundle.NewManagement

	lock *sync.Mutex

	client     *websocket.Conn
	urlParsed  *url.URL
	urlStr     string
	tlsEnabled bool
	tlsConf    *tls.Config
	reqSigner  httpclient.RequestSigner

	openMsgType wsOpenMsgType
	openMsg     []byte
}

func newWebsocketReaderFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (*websocketReader, error) {
	ws := &websocketReader{
		log:  mgr.Logger(),
		mgr:  mgr,
		lock: &sync.Mutex{},
	}
	var err error
	if ws.urlParsed, err = conf.FieldURL("url"); err != nil {
		return nil, err
	}
	if ws.urlStr, err = conf.FieldString("url"); err != nil {
		return nil, err
	}
	if ws.tlsConf, ws.tlsEnabled, err = conf.FieldTLSToggled("tls"); err != nil {
		return nil, err
	}
	if ws.reqSigner, err = httpclient.AuthSignerFromParsed(conf); err != nil {
		return nil, err
	}
	var openMsgStr, openMsgTypeStr string
	if openMsgTypeStr, err = conf.FieldString("open_message_type"); err != nil {
		return nil, err
	}
	ws.openMsgType = wsOpenMsgType(openMsgTypeStr)
	if openMsgStr, _ = conf.FieldString("open_message"); openMsgStr != "" {
		ws.openMsg = []byte(openMsgStr)
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

	err := w.reqSigner(w.mgr.FS(), &http.Request{
		URL:    w.urlParsed,
		Header: headers,
	})
	if err != nil {
		return err
	}

	var (
		client *websocket.Conn
		res    *http.Response
	)

	defer func() {
		if res != nil {
			res.Body.Close()
		}
	}()

	if w.tlsEnabled {
		dialer := websocket.Dialer{
			TLSClientConfig: w.tlsConf,
		}
		if client, res, err = dialer.Dial(w.urlStr, headers); err != nil {
			return err
		}
	} else if client, res, err = websocket.DefaultDialer.Dial(w.urlStr, headers); err != nil {
		return err
	}

	var openMsgType int
	switch w.openMsgType {
	case wsOpenMsgTypeBinary:
		openMsgType = websocket.BinaryMessage
	case wsOpenMsgTypeText:
		openMsgType = websocket.TextMessage
	default:
		return fmt.Errorf("unrecognised open_message_type: %s", w.openMsgType)
	}

	if len(w.openMsg) > 0 {
		if err := client.WriteMessage(openMsgType, w.openMsg); err != nil {
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
