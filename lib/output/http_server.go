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

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
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

	closedChan chan struct{}
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
		closedChan: make(chan struct{}),
	}

	if mux != nil {
		if len(h.conf.HTTPServer.Path) > 0 {
			h.mux.HandleFunc(h.conf.HTTPServer.Path, h.getHandler)
		}
		if len(h.conf.HTTPServer.StreamPath) > 0 {
			h.mux.HandleFunc(h.conf.HTTPServer.StreamPath, h.streamHandler)
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
	}
	return &h, nil
}

//------------------------------------------------------------------------------

func (h *HTTPServer) getHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	h.stats.Incr("output.http_server.get.request.received", 1)

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
		h.stats.Incr("output.http_server.get.count", 1)
		h.stats.Incr("output.http_server.count", 1)
	case <-time.After(tOutDuration - time.Since(tStart)):
		http.Error(w, "Timed out waiting for message", http.StatusRequestTimeout)
		return
	}

	if len(ts.Payload.Parts) > 1 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < len(ts.Payload.Parts) && err == nil; i++ {
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(ts.Payload.Parts[i]))
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	} else {
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(ts.Payload.Parts[0])
	}

	ts.ResponseChan <- types.NewSimpleResponse(nil)
	h.stats.Incr("output.http_server.send.success", 1)
	h.stats.Incr("output.http_server.get.send.success", 1)
}

func (h *HTTPServer) streamHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	h.stats.Incr("output.http_server.stream.request.received", 1)

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Server error", http.StatusInternalServerError)
		h.stats.Incr("output.http_server.stream.error.cast_flusher", 1)
		h.log.Errorln("Failed to cast response writer to flusher")
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		h.stats.Incr("output.http_server.stream.error.wrong_method", 1)
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
			h.stats.Incr("output.http_server.stream.client_closed", 1)
			return
		}
		h.stats.Incr("output.http_server.count", 1)
		h.stats.Incr("output.http_server.stream.count", 1)

		var data []byte
		if len(ts.Payload.Parts) == 1 {
			data = ts.Payload.Parts[0]
		} else {
			data = append(bytes.Join(ts.Payload.Parts, []byte("\n")), byte('\n'))
		}

		_, err := w.Write(data)
		ts.ResponseChan <- types.NewSimpleResponse(err)

		if err != nil {
			h.stats.Incr("output.http_server.stream.error.write", 1)
			return
		}

		w.Write([]byte("\n"))
		flusher.Flush()
		h.stats.Incr("output.http_server.send.success", 1)
		h.stats.Incr("output.http_server.stream.send.success", 1)
	}
}

//------------------------------------------------------------------------------

// StartReceiving assigns a messages channel for the output to read.
func (h *HTTPServer) StartReceiving(ts <-chan types.Transaction) error {
	if h.transactions != nil {
		return types.ErrAlreadyStarted
	}
	h.transactions = ts

	if h.server != nil {
		go func() {
			h.stats.Incr("output.http_server.running", 1)

			h.log.Infof(
				"Serving messages through HTTP GET request at: %s\n",
				h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
			)

			if len(h.conf.HTTPServer.KeyFile) > 0 || len(h.conf.HTTPServer.CertFile) > 0 {
				if err := h.server.ListenAndServeTLS(
					h.conf.HTTPServer.CertFile, h.conf.HTTPServer.KeyFile,
				); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			} else {
				if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
					h.log.Errorf("Server error: %v\n", err)
				}
			}

			h.stats.Decr("output.http_server.running", 1)

			atomic.StoreInt32(&h.running, 0)
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
