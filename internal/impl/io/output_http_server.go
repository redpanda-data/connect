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

	"github.com/gorilla/websocket"

	"github.com/benthosdev/benthos/v4/internal/batch"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/httpserver"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
)

func init() {
	corsSpec := httpserver.ServerCORSFieldSpec()
	corsSpec.Description += " Only valid with a custom `address`."

	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(conf output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newHTTPServerOutput(conf, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "http_server",
		Summary: `Sets up an HTTP server that will send messages over HTTP(S) GET requests. HTTP 2.0 is supported when using TLS, which is enabled when key and cert files are specified.`,
		Description: `
Sets up an HTTP server that will send messages over HTTP(S) GET requests. If the ` + "`address`" + ` config field is left blank the [service-wide HTTP server](/docs/components/http/about) will be used.

Three endpoints will be registered at the paths specified by the fields ` + "`path`, `stream_path` and `ws_path`" + `. Which allow you to consume a single message batch, a continuous stream of line delimited messages, or a websocket of messages for each request respectively.

When messages are batched the ` + "`path`" + ` endpoint encodes the batch according to [RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html). This behaviour can be overridden by [archiving your batches](/docs/configuration/batching#post-batch-processing).

Please note, messages are considered delivered as soon as the data is written to the client. There is no concept of at least once delivery on this output.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("address", "An optional address to listen from. If left empty the service wide HTTP server is used."),
			docs.FieldString("path", "The path from which discrete messages can be consumed."),
			docs.FieldString("stream_path", "The path from which a continuous stream of messages can be consumed."),
			docs.FieldString("ws_path", "The path from which websocket connections can be established."),
			docs.FieldString("allowed_verbs", "An array of verbs that are allowed for the `path` and `stream_path` HTTP endpoint.").Array(),
			docs.FieldString("timeout", "The maximum time to wait before a blocking, inactive connection is dropped (only applies to the `path` endpoint).").Advanced(),
			docs.FieldString("cert_file", "An optional certificate file to use for TLS connections. Only applicable when an `address` is specified.").Advanced(),
			docs.FieldString("key_file", "An optional certificate key file to use for TLS connections. Only applicable when an `address` is specified.").Advanced(),
			corsSpec,
		).ChildDefaultAndTypesFromStruct(output.NewHTTPServerConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type httpServerOutput struct {
	conf output.Config
	log  log.Modular

	mux     *http.ServeMux
	server  *http.Server
	timeout time.Duration

	transactions <-chan message.Transaction

	allowedVerbs map[string]struct{}

	mGetSent      metrics.StatCounter
	mGetBatchSent metrics.StatCounter

	mWSSent      metrics.StatCounter
	mWSBatchSent metrics.StatCounter
	mWSLatency   metrics.StatTimer
	mWSError     metrics.StatCounter

	mStreamSent      metrics.StatCounter
	mStreamBatchSent metrics.StatCounter
	mStreamError     metrics.StatCounter

	closeServerOnce sync.Once
	shutSig         *shutdown.Signaller
}

func newHTTPServerOutput(conf output.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	var mux *http.ServeMux
	var server *http.Server

	var err error
	if len(conf.HTTPServer.Address) > 0 {
		mux = http.NewServeMux()
		server = &http.Server{Addr: conf.HTTPServer.Address}
		if server.Handler, err = conf.HTTPServer.CORS.WrapHandler(mux); err != nil {
			return nil, fmt.Errorf("bad CORS configuration: %w", err)
		}
	}

	verbs := map[string]struct{}{}
	for _, v := range conf.HTTPServer.AllowedVerbs {
		verbs[v] = struct{}{}
	}
	if len(verbs) == 0 {
		return nil, errors.New("must provide at least one allowed verb")
	}

	mSent := stats.GetCounter("output_sent")
	mBatchSent := stats.GetCounter("output_batch_sent")
	mLatency := stats.GetTimer("output_latency_ns")
	mError := stats.GetCounter("output_error")

	h := httpServerOutput{
		shutSig: shutdown.NewSignaller(),
		conf:    conf,
		log:     log,
		mux:     mux,
		server:  server,

		allowedVerbs: verbs,

		mGetSent:      mSent,
		mGetBatchSent: mBatchSent,

		mWSSent:      mSent,
		mWSBatchSent: mBatchSent,
		mWSError:     mError,
		mWSLatency:   mLatency,

		mStreamSent:      mSent,
		mStreamBatchSent: mBatchSent,
		mStreamError:     mError,
	}

	if tout := conf.HTTPServer.Timeout; len(tout) > 0 {
		if h.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	if mux != nil {
		if len(h.conf.HTTPServer.Path) > 0 {
			h.mux.HandleFunc(h.conf.HTTPServer.Path, h.getHandler)
		}
		if len(h.conf.HTTPServer.StreamPath) > 0 {
			h.mux.HandleFunc(h.conf.HTTPServer.StreamPath, h.streamHandler)
		}
		if len(h.conf.HTTPServer.WSPath) > 0 {
			h.mux.HandleFunc(h.conf.HTTPServer.WSPath, h.wsHandler)
		}
	} else {
		if len(h.conf.HTTPServer.Path) > 0 {
			mgr.RegisterEndpoint(
				h.conf.HTTPServer.Path, "Read a single message from Benthos.",
				h.getHandler,
			)
		}
		if len(h.conf.HTTPServer.StreamPath) > 0 {
			mgr.RegisterEndpoint(
				h.conf.HTTPServer.StreamPath,
				"Read a continuous stream of messages from Benthos.",
				h.streamHandler,
			)
		}
		if len(h.conf.HTTPServer.WSPath) > 0 {
			mgr.RegisterEndpoint(
				h.conf.HTTPServer.WSPath,
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

	if _, exists := h.allowedVerbs[r.Method]; !exists {
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
	case <-time.After(h.timeout - time.Since(tStart)):
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
		h.log.Errorln("Failed to cast response writer to flusher")
		return
	}

	if _, exists := h.allowedVerbs[r.Method]; !exists {
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
			h.log.Warnf("Websocket request failed: %v\n", err)
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
			if len(h.conf.HTTPServer.KeyFile) > 0 || len(h.conf.HTTPServer.CertFile) > 0 {
				h.log.Infof(
					"Serving messages through HTTPS GET request at: https://%s\n",
					h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
				)
				if err := h.server.ListenAndServeTLS(
					h.conf.HTTPServer.CertFile, h.conf.HTTPServer.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			} else {
				h.log.Infof(
					"Serving messages through HTTP GET request at: http://%s\n",
					h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
				)
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
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
