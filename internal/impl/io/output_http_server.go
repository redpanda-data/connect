package io

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/httpserver"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	hsoFieldAddress            = "address"
	hsoFieldPath               = "path"
	hsoFieldStreamPath         = "stream_path"
	hsoFieldWSPath             = "ws_path"
	hsoFieldAllowedVerbs       = "allowed_verbs"
	hsoFieldTimeout            = "timeout"
	hsoFieldCertFile           = "cert_file"
	hsoFieldKeyFile            = "key_file"
	hsoFieldCORS               = "cors"
	hsoFieldCORSEnabled        = "enabled"
	hsoFieldCORSAllowedOrigins = "allowed_origins"
)

type hsoConfig struct {
	Address      string
	Path         string
	StreamPath   string
	WSPath       string
	AllowedVerbs map[string]struct{}
	Timeout      time.Duration
	CertFile     string
	KeyFile      string
	CORS         httpserver.CORSConfig
}

func hsoConfigFromParsed(pConf *service.ParsedConfig) (conf hsoConfig, err error) {
	if conf.Address, err = pConf.FieldString(hsoFieldAddress); err != nil {
		return
	}
	if conf.Path, err = pConf.FieldString(hsoFieldPath); err != nil {
		return
	}
	if conf.StreamPath, err = pConf.FieldString(hsoFieldStreamPath); err != nil {
		return
	}
	if conf.WSPath, err = pConf.FieldString(hsoFieldWSPath); err != nil {
		return
	}
	{
		var verbsList []string
		if verbsList, err = pConf.FieldStringList(hsoFieldAllowedVerbs); err != nil {
			return
		}
		if len(verbsList) == 0 {
			err = errors.New("must specify at least one allowed verb")
			return
		}
		conf.AllowedVerbs = map[string]struct{}{}
		for _, v := range verbsList {
			conf.AllowedVerbs[v] = struct{}{}
		}
	}
	if conf.Timeout, err = pConf.FieldDuration(hsoFieldTimeout); err != nil {
		return
	}
	if conf.CertFile, err = pConf.FieldString(hsoFieldCertFile); err != nil {
		return
	}
	if conf.KeyFile, err = pConf.FieldString(hsoFieldKeyFile); err != nil {
		return
	}
	if conf.CORS, err = corsConfigFromParsed(pConf.Namespace(hsoFieldCORS)); err != nil {
		return
	}
	return
}

func hsoSpec() *service.ConfigSpec {
	corsSpec := httpserver.ServerCORSFieldSpec()
	corsSpec.Description += " Only valid with a custom `address`."

	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary(`Sets up an HTTP server that will send messages over HTTP(S) GET requests. HTTP 2.0 is supported when using TLS, which is enabled when key and cert files are specified.`).
		Description(`Sets up an HTTP server that will send messages over HTTP(S) GET requests. If the `+"`address`"+` config field is left blank the [service-wide HTTP server](/docs/components/http/about) will be used.

Three endpoints will be registered at the paths specified by the fields `+"`path`, `stream_path` and `ws_path`"+`. Which allow you to consume a single message batch, a continuous stream of line delimited messages, or a websocket of messages for each request respectively.

When messages are batched the `+"`path`"+` endpoint encodes the batch according to [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). This behaviour can be overridden by [archiving your batches](/docs/configuration/batching#post-batch-processing).

Please note, messages are considered delivered as soon as the data is written to the client. There is no concept of at least once delivery on this output.

`+api.EndpointCaveats()+`
`).
		Fields(
			service.NewStringField(hsoFieldAddress).
				Description("An alternative address to host from. If left empty the service wide address is used.").
				Default(""),
			service.NewStringField(hsoFieldPath).
				Description("The path from which discrete messages can be consumed.").
				Default("/get"),
			service.NewStringField(hsoFieldStreamPath).
				Description("The path from which a continuous stream of messages can be consumed.").
				Default("/get/stream"),
			service.NewStringField(hsoFieldWSPath).
				Description("The path from which websocket connections can be established.").
				Default("/get/ws"),
			service.NewStringListField(hsoFieldAllowedVerbs).
				Description("An array of verbs that are allowed for the `path` and `stream_path` HTTP endpoint.").
				Default([]any{"GET"}),
			service.NewDurationField(hsoFieldTimeout).
				Description("The maximum time to wait before a blocking, inactive connection is dropped (only applies to the `path` endpoint).").
				Default("5s").
				Advanced(),
			service.NewStringField(hsoFieldCertFile).
				Description("Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").
				Advanced().
				Default(""),
			service.NewStringField(hsoFieldKeyFile).
				Description("Enable TLS by specifying a certificate and key file. Only valid with a custom `address`.").
				Advanced().
				Default(""),
			service.NewInternalField(corsSpec),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"http_server", hsoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, pol service.BatchPolicy, mif int, err error) {
			var hsoConf hsoConfig
			if hsoConf, err = hsoConfigFromParsed(conf); err != nil {
				return
			}

			// TODO: If we refactor this input to implement WriteBatch then we
			// can return a proper service.BatchOutput implementation.

			oldMgr := interop.UnwrapManagement(mgr)

			var outStrm output.Streamed
			if outStrm, err = newHTTPServerOutput(hsoConf, oldMgr); err != nil {
				return
			}

			out = interop.NewUnwrapInternalOutput(outStrm)
			return
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpServerOutput struct {
	conf hsoConfig
	log  log.Modular

	mux    *mux.Router
	server *http.Server

	transactions <-chan message.Transaction

	mGetSent      metrics.StatCounter
	mGetBatchSent metrics.StatCounter

	mWSSent      metrics.StatCounter
	mWSBatchSent metrics.StatCounter
	mWSError     metrics.StatCounter

	mStreamSent      metrics.StatCounter
	mStreamBatchSent metrics.StatCounter
	mStreamError     metrics.StatCounter

	closeServerOnce sync.Once
	shutSig         *shutdown.Signaller
}

func newHTTPServerOutput(conf hsoConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	var gMux *mux.Router
	var server *http.Server

	var err error
	if len(conf.Address) > 0 {
		gMux = mux.NewRouter()
		server = &http.Server{Addr: conf.Address}
		if server.Handler, err = conf.CORS.WrapHandler(gMux); err != nil {
			return nil, fmt.Errorf("bad CORS configuration: %w", err)
		}
	}

	stats := mgr.Metrics()
	mSent := stats.GetCounter("output_sent")
	mBatchSent := stats.GetCounter("output_batch_sent")
	mError := stats.GetCounter("output_error")

	h := httpServerOutput{
		shutSig: shutdown.NewSignaller(),
		conf:    conf,
		log:     mgr.Logger(),
		mux:     gMux,
		server:  server,

		mGetSent:      mSent,
		mGetBatchSent: mBatchSent,

		mWSSent:      mSent,
		mWSBatchSent: mBatchSent,
		mWSError:     mError,

		mStreamSent:      mSent,
		mStreamBatchSent: mBatchSent,
		mStreamError:     mError,
	}

	if gMux != nil {
		if len(h.conf.Path) > 0 {
			api.GetMuxRoute(gMux, h.conf.Path).HandlerFunc(h.getHandler)
		}
		if len(h.conf.StreamPath) > 0 {
			api.GetMuxRoute(gMux, h.conf.StreamPath).HandlerFunc(h.streamHandler)
		}
		if len(h.conf.WSPath) > 0 {
			api.GetMuxRoute(gMux, h.conf.WSPath).HandlerFunc(h.wsHandler)
		}
	} else {
		if len(h.conf.Path) > 0 {
			mgr.RegisterEndpoint(
				h.conf.Path, "Read a single message from Benthos.",
				h.getHandler,
			)
		}
		if len(h.conf.StreamPath) > 0 {
			mgr.RegisterEndpoint(
				h.conf.StreamPath,
				"Read a continuous stream of messages from Benthos.",
				h.streamHandler,
			)
		}
		if len(h.conf.WSPath) > 0 {
			mgr.RegisterEndpoint(
				h.conf.WSPath,
				"Read messages from Benthos via websockets.",
				h.wsHandler,
			)
		}
	}

	return &h, nil
}

//------------------------------------------------------------------------------

func (h *httpServerOutput) getHandler(w http.ResponseWriter, r *http.Request) {
	if h.shutSig.ShouldCloseAtLeisure() {
		http.Error(w, "Server closed", http.StatusServiceUnavailable)
		return
	}

	ctx, done := h.shutSig.CloseAtLeisureCtx(r.Context())
	defer done()

	if _, exists := h.conf.AllowedVerbs[r.Method]; !exists {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	tStart := time.Now()

	var ts message.Transaction
	var open bool
	var err error

	select {
	case ts, open = <-h.transactions:
		if !open {
			http.Error(w, "Server closed", http.StatusServiceUnavailable)
			go h.TriggerCloseNow()
			return
		}
	case <-time.After(h.conf.Timeout - time.Since(tStart)):
		http.Error(w, "Timed out waiting for message", http.StatusRequestTimeout)
		return
	}

	if ts.Payload.Len() > 1 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < ts.Payload.Len() && err == nil; i++ {
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(ts.Payload.Get(i).AsBytes()))
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		_, _ = w.Write(body.Bytes())
	} else {
		w.Header().Add("Content-Type", "application/octet-stream")
		_, _ = w.Write(ts.Payload.Get(0).AsBytes())
	}

	h.mGetBatchSent.Incr(1)
	h.mGetSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))

	_ = ts.Ack(ctx, nil)
}

func (h *httpServerOutput) streamHandler(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Server error", http.StatusInternalServerError)
		h.log.Error("Failed to cast response writer to flusher")
		return
	}

	if _, exists := h.conf.AllowedVerbs[r.Method]; !exists {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	ctx, done := h.shutSig.CloseAtLeisureCtx(r.Context())
	defer done()

	for !h.shutSig.ShouldCloseAtLeisure() {
		var ts message.Transaction
		var open bool

		select {
		case ts, open = <-h.transactions:
			if !open {
				go h.TriggerCloseNow()
				return
			}
		case <-r.Context().Done():
			return
		}

		var data []byte
		if ts.Payload.Len() == 1 {
			data = ts.Payload.Get(0).AsBytes()
		} else {
			data = append(bytes.Join(message.GetAllBytes(ts.Payload), []byte("\n")), byte('\n'))
		}

		_, err := w.Write(data)
		_ = ts.Ack(ctx, err)
		if err != nil {
			h.mStreamError.Incr(1)
			return
		}

		_, _ = w.Write([]byte("\n"))
		flusher.Flush()
		h.mStreamSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))
		h.mStreamBatchSent.Incr(1)
	}
}

func (h *httpServerOutput) wsHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warn("Websocket request failed: %v\n", err)
			return
		}
	}()

	upgrader := websocket.Upgrader{}

	var ws *websocket.Conn
	if ws, err = upgrader.Upgrade(w, r, nil); err != nil {
		return
	}
	defer ws.Close()

	ctx, done := h.shutSig.CloseAtLeisureCtx(r.Context())
	defer done()

	for !h.shutSig.ShouldCloseAtLeisure() {
		var ts message.Transaction
		var open bool

		select {
		case ts, open = <-h.transactions:
			if !open {
				go h.TriggerCloseNow()
				return
			}
		case <-r.Context().Done():
			return
		case <-h.shutSig.CloseAtLeisureChan():
			return
		}

		var werr error
		for _, msg := range message.GetAllBytes(ts.Payload) {
			if werr = ws.WriteMessage(websocket.BinaryMessage, msg); werr != nil {
				break
			}
			h.mWSBatchSent.Incr(1)
			h.mWSSent.Incr(int64(batch.MessageCollapsedCount(ts.Payload)))
		}
		if werr != nil {
			h.mWSError.Incr(1)
		}
		_ = ts.Ack(ctx, werr)
	}
}

func (h *httpServerOutput) Consume(ts <-chan message.Transaction) error {
	if h.transactions != nil {
		return component.ErrAlreadyStarted
	}
	h.transactions = ts

	if h.server != nil {
		go func() {
			if len(h.conf.KeyFile) > 0 || len(h.conf.CertFile) > 0 {
				h.log.Info(
					"Serving messages through HTTPS GET request at: https://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServeTLS(
					h.conf.CertFile, h.conf.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Error("Server error: %v\n", err)
				}
			} else {
				h.log.Info(
					"Serving messages through HTTP GET request at: http://%s\n",
					h.conf.Address+h.conf.Path,
				)
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Error("Server error: %v\n", err)
				}
			}

			h.shutSig.CloseAtLeisure()
			h.shutSig.ShutdownComplete()
		}()
	}
	return nil
}

func (h *httpServerOutput) Connected() bool {
	// Always return true as this is fuzzy right now.
	return true
}

func (h *httpServerOutput) TriggerCloseNow() {
	h.shutSig.CloseNow()
	h.closeServerOnce.Do(func() {
		if h.server != nil {
			_ = h.server.Shutdown(context.Background())
		}
		h.shutSig.ShutdownComplete()
	})
}

func (h *httpServerOutput) WaitForClose(ctx context.Context) error {
	select {
	case <-h.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
