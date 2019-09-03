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
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gorilla/websocket"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeHTTPServer] = TypeSpec{
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

// HTTPServerConfig contains configuration fields for the HTTPServer output
// type.
type HTTPServerConfig struct {
	Address    string `json:"address" yaml:"address"`
	Path       string `json:"path" yaml:"path"`
	StreamPath string `json:"stream_path" yaml:"stream_path"`
	WSPath     string `json:"ws_path" yaml:"ws_path"`
	Timeout    string `json:"timeout" yaml:"timeout"`
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
		Timeout:    "5s",
		CertFile:   "",
		KeyFile:    "",
	}
}

//------------------------------------------------------------------------------

// HTTPServer is an output type that serves HTTPServer GET requests.
type HTTPServer struct {
	running int32

	conf  Config
	stats metrics.Type
	log   log.Modular

	mux     *http.ServeMux
	server  *http.Server
	timeout time.Duration

	transactions <-chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}

	mRunning       metrics.StatGauge
	mCount         metrics.StatCounter
	mPartsCount    metrics.StatCounter
	mSendSucc      metrics.StatCounter
	mPartsSendSucc metrics.StatCounter
	mSent          metrics.StatCounter
	mPartsSent     metrics.StatCounter

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

// NewHTTPServer creates a new HTTPServer output type.
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
		log:        log,
		mux:        mux,
		server:     server,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),

		mRunning:       stats.GetGauge("running"),
		mCount:         stats.GetCounter("count"),
		mPartsCount:    stats.GetCounter("parts.count"),
		mSendSucc:      stats.GetCounter("send.success"),
		mPartsSendSucc: stats.GetCounter("parts.send.success"),
		mSent:          stats.GetCounter("batch.sent"),
		mPartsSent:     stats.GetCounter("sent"),
		mGetReqRcvd:    stats.GetCounter("get.request.received"),
		mGetCount:      stats.GetCounter("get.count"),
		mGetSendSucc:   stats.GetCounter("get.send.success"),
		mWSCount:       stats.GetCounter("ws.count"),
		mWSReqRcvd:     stats.GetCounter("stream.request.received"),
		mWSSendSucc:    stats.GetCounter("ws.send.success"),
		mWSSendErr:     stats.GetCounter("ws.send.error"),
		mStrmReqRcvd:   stats.GetCounter("stream.request.received"),
		mStrmErrCast:   stats.GetCounter("stream.error.cast_flusher"),
		mStrmErrWrong:  stats.GetCounter("stream.error.wrong_method"),
		mStrmClosed:    stats.GetCounter("stream.client_closed"),
		mStrmCount:     stats.GetCounter("stream.count"),
		mStrmErrWrite:  stats.GetCounter("stream.error.write"),
		mStrmSndSucc:   stats.GetCounter("stream.send.success"),
	}

	if tout := conf.HTTPServer.Timeout; len(tout) > 0 {
		var err error
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
		h.mPartsCount.Incr(int64(ts.Payload.Len()))
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
				_, err = io.Copy(part, bytes.NewReader(ts.Payload.Get(i).Get()))
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	} else {
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(ts.Payload.Get(0).Get())
	}

	h.mSendSucc.Incr(1)
	h.mPartsSendSucc.Incr(int64(ts.Payload.Len()))
	h.mSent.Incr(1)
	h.mPartsSent.Incr(int64(ts.Payload.Len()))
	h.mGetSendSucc.Incr(1)

	select {
	case ts.ResponseChan <- response.NewAck():
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
			return
		}
		h.mStrmCount.Incr(1)
		h.mCount.Incr(1)

		var data []byte
		if ts.Payload.Len() == 1 {
			data = ts.Payload.Get(0).Get()
		} else {
			data = append(bytes.Join(message.GetAllBytes(ts.Payload), []byte("\n")), byte('\n'))
		}

		_, err := w.Write(data)
		select {
		case ts.ResponseChan <- response.NewError(err):
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
		h.mPartsSendSucc.Incr(int64(ts.Payload.Len()))
		h.mSent.Incr(1)
		h.mPartsSent.Incr(int64(ts.Payload.Len()))
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
			return
		case <-h.closeChan:
			return
		}
		h.mWSCount.Incr(1)
		h.mCount.Incr(1)

		var werr error
		for _, msg := range message.GetAllBytes(ts.Payload) {
			if werr = ws.WriteMessage(websocket.BinaryMessage, msg); werr != nil {
				break
			}
			h.mWSSendSucc.Incr(1)
			h.mSendSucc.Incr(1)
			h.mPartsSendSucc.Incr(int64(ts.Payload.Len()))
			h.mSent.Incr(1)
			h.mPartsSent.Incr(int64(ts.Payload.Len()))
		}

		if werr != nil {
			h.mWSSendErr.Incr(1)
		}
		select {
		case ts.ResponseChan <- response.NewError(werr):
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
			h.mRunning.Incr(1)

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

			h.mRunning.Decr(1)

			atomic.StoreInt32(&h.running, 0)
			close(h.closeChan)
			close(h.closedChan)
		}()
	}
	return nil
}

// Connected returns a boolean indicating whether this output is currently
// connected to its target.
func (h *HTTPServer) Connected() bool {
	// Always return true as this is fuzzy right now.
	return true
}

// CloseAsync shuts down the HTTPServer output and stops processing requests.
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
