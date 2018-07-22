// Copyright (c) 2014 Ashley Jeffs
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

package output

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/gorilla/websocket"
)

//------------------------------------------------------------------------------

func init() {
	Constructors["http_server"] = TypeSpec{
		constructor: NewHTTPServer,
		description: `
Sets up an HTTP server that will send messages over HTTP(S) GET requests. HTTP
2.0 is supported when using TLS, which is enabled when key and cert files are
specified.

You can leave the 'address' config field blank in order to use the default
service, but this will ignore TLS options.

You can receive a single, discrete message on the configured 'path' endpoint, or
receive a constant stream of line delimited messages on the configured
'stream_path' endpoint.`,
	}
}

//------------------------------------------------------------------------------

// HTTPServerConfig is configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address    string `json:"address" yaml:"address"`
	Path       string `json:"path" yaml:"path"`
	StreamPath string `json:"stream_path" yaml:"stream_path"`
	WSPath     string `json:"ws_path" yaml:"ws_path"`
	TimeoutMS  int64  `json:"timeout_ms" yaml:"timeout_ms"`
	CertFile   string `json:"cert_file" yaml:"cert_file"`
	KeyFile    string `json:"key_file" yaml:"key_file"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:    "",
		Path:       "/get",
		StreamPath: "/get/stream",
		WSPath:     "/get/ws",
		TimeoutMS:  5000,
		CertFile:   "",
		KeyFile:    "",
	}
}

//------------------------------------------------------------------------------

// HTTPServer is an input type that serves HTTPServer POST requests.
type HTTPServer struct {
	running int32

	conf  Config
	stats metrics.Type
	log   log.Modular

	mux    *http.ServeMux
	server *http.Server

	transactions <-chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}

	mCount    metrics.StatCounter
	mSendSucc metrics.StatCounter

	mGetReqRcvd  metrics.StatCounter
	mGetCount    metrics.StatCounter
	mGetSendSucc metrics.StatCounter

	mWSReqRcvd  metrics.StatCounter
	mWSCount    metrics.StatCounter
	mWSSendSucc metrics.StatCounter
	mWSSendErr  metrics.StatCounter

	mStrmReqRcvd  metrics.StatCounter
	mStrmErrCast  metrics.StatCounter
	mStrmErrWrong metrics.StatCounter
	mStrmClosed   metrics.StatCounter
	mStrmCount    metrics.StatCounter
	mStrmErrWrite metrics.StatCounter
	mStrmSndSucc  metrics.StatCounter
}

// NewHTTPServer creates a new HTTPServer input type.
func NewHTTPServer(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var mux *http.ServeMux
	var server *http.Server

	if len(conf.HTTPServer.Address) > 0 {
		mux = http.NewServeMux()
		server = &http.Server{Addr: conf.HTTPServer.Address, Handler: mux}
	}

	h := HTTPServer{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".output.http_server"),
		mux:        mux,
		server:     server,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),

		mCount:        stats.GetCounter("output.http_server.count"),
		mSendSucc:     stats.GetCounter("output.http_server.send.success"),
		mGetReqRcvd:   stats.GetCounter("output.http_server.get.request.received"),
		mGetCount:     stats.GetCounter("output.http_server.get.count"),
		mGetSendSucc:  stats.GetCounter("output.http_server.get.send.success"),
		mWSCount:      stats.GetCounter("output.http_server.ws.count"),
		mWSReqRcvd:    stats.GetCounter("output.http_server.stream.request.received"),
		mWSSendSucc:   stats.GetCounter("output.http_server.ws.send.success"),
		mWSSendErr:    stats.GetCounter("output.http_server.ws.send.error"),
		mStrmReqRcvd:  stats.GetCounter("output.http_server.stream.request.received"),
		mStrmErrCast:  stats.GetCounter("output.http_server.stream.error.cast_flusher"),
		mStrmErrWrong: stats.GetCounter("output.http_server.stream.error.wrong_method"),
		mStrmClosed:   stats.GetCounter("output.http_server.stream.client_closed"),
		mStrmCount:    stats.GetCounter("output.http_server.stream.count"),
		mStrmErrWrite: stats.GetCounter("output.http_server.stream.error.write"),
		mStrmSndSucc:  stats.GetCounter("output.http_server.stream.send.success"),
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

func (h *HTTPServer) getHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	h.mGetReqRcvd.Incr(1)

	if atomic.LoadInt32(&h.running) != 1 {
		http.Error(w, "Server closed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	tStart := time.Now()
	tOutDuration := time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)

	var ts types.Transaction
	var open bool
	var err error

	select {
	case ts, open = <-h.transactions:
		if !open {
			http.Error(w, "Server closed", http.StatusServiceUnavailable)
			go h.CloseAsync()
			return
		}
		h.mGetCount.Incr(1)
		h.mCount.Incr(1)
	case <-time.After(tOutDuration - time.Since(tStart)):
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
				_, err = io.Copy(part, bytes.NewReader(ts.Payload.Get(i)))
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	} else {
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(ts.Payload.Get(0))
	}

	h.mSendSucc.Incr(1)
	h.mGetSendSucc.Incr(1)

	select {
	case ts.ResponseChan <- types.NewSimpleResponse(nil):
	case <-h.closeChan:
		return
	}
}

func (h *HTTPServer) streamHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	h.mStrmReqRcvd.Incr(1)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Server error", http.StatusInternalServerError)
		h.mStrmErrCast.Incr(1)
		h.log.Errorln("Failed to cast response writer to flusher")
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		h.mStrmErrWrong.Incr(1)
		return
	}

	for atomic.LoadInt32(&h.running) == 1 {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-h.transactions:
			if !open {
				go h.CloseAsync()
				return
			}
		case <-r.Context().Done():
			h.mStrmClosed.Incr(1)
			h.stats.Incr("output.http_server.stream.client_closed", 1)
			return
		}
		h.mStrmCount.Incr(1)
		h.mCount.Incr(1)

		var data []byte
		if ts.Payload.Len() == 1 {
			data = ts.Payload.Get(0)
		} else {
			data = append(bytes.Join(ts.Payload.GetAll(), []byte("\n")), byte('\n'))
		}

		_, err := w.Write(data)
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(err):
		case <-h.closeChan:
			return
		}

		if err != nil {
			h.mStrmErrWrite.Incr(1)
			return
		}

		w.Write([]byte("\n"))
		flusher.Flush()
		h.mStrmSndSucc.Incr(1)
		h.mSendSucc.Incr(1)
	}
}

func (h *HTTPServer) wsHandler(w http.ResponseWriter, r *http.Request) {
	h.mWSReqRcvd.Incr(1)

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

	for atomic.LoadInt32(&h.running) == 1 {
		var ts types.Transaction
		var open bool

		select {
		case ts, open = <-h.transactions:
			if !open {
				go h.CloseAsync()
				return
			}
		case <-r.Context().Done():
			h.mStrmClosed.Incr(1)
			h.stats.Incr("output.http_server.stream.client_closed", 1)
			return
		case <-h.closeChan:
			return
		}
		h.mWSCount.Incr(1)
		h.mCount.Incr(1)

		var werr error
		for _, msg := range ts.Payload.GetAll() {
			if werr = ws.WriteMessage(websocket.BinaryMessage, msg); werr != nil {
				break
			}
			h.mWSSendSucc.Incr(1)
			h.mSendSucc.Incr(1)
		}

		if werr != nil {
			h.mWSSendErr.Incr(1)
		}
		select {
		case ts.ResponseChan <- types.NewSimpleResponse(werr):
		case <-h.closeChan:
			return
		}
	}
}

//------------------------------------------------------------------------------

// Consume assigns a messages channel for the output to read.
func (h *HTTPServer) Consume(ts <-chan types.Transaction) error {
	if h.transactions != nil {
		return types.ErrAlreadyStarted
	}
	h.transactions = ts

	if h.server != nil {
		go func() {
			h.stats.Incr("output.http_server.running", 1)

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

			h.stats.Decr("output.http_server.running", 1)

			atomic.StoreInt32(&h.running, 0)
			close(h.closeChan)
			close(h.closedChan)
		}()
	}
	return nil
}

// CloseAsync shuts down the HTTPServer input and stops processing requests.
func (h *HTTPServer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&h.running, 1, 0) {
		if h.server != nil {
			h.server.Shutdown(context.Background())
		} else {
			close(h.closedChan)
		}
	}
}

// WaitForClose blocks until the HTTPServer output has closed down.
func (h *HTTPServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
