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
	constructors["http_server"] = typeSpec{
		constructor: NewHTTPServer,
		description: `
Sets up an HTTP server that will return messages over HTTP GET requests. HTTP
2.0 is supported when using TLS, which is enabled when key and cert files are
specified.`,
	}
}

//------------------------------------------------------------------------------

// HTTPServerConfig is configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address   string `json:"address" yaml:"address"`
	Path      string `json:"path" yaml:"path"`
	TimeoutMS int64  `json:"timeout_ms" yaml:"timeout_ms"`
	CertFile  string `json:"cert_file" yaml:"cert_file"`
	KeyFile   string `json:"key_file" yaml:"key_file"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:   "localhost:8081",
		Path:      "/get",
		TimeoutMS: 5000,
		CertFile:  "",
		KeyFile:   "",
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

	messages     <-chan types.Message
	responseChan chan types.Response

	closedChan chan struct{}
}

// NewHTTPServer creates a new HTTPServer input type.
func NewHTTPServer(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	mux := http.NewServeMux()
	server := &http.Server{Addr: conf.HTTPServer.Address, Handler: mux}

	h := HTTPServer{
		running:      1,
		conf:         conf,
		stats:        stats,
		log:          log.NewModule(".output.http_server"),
		mux:          mux,
		server:       server,
		messages:     nil,
		responseChan: make(chan types.Response),
		closedChan:   make(chan struct{}),
	}

	h.mux.HandleFunc(h.conf.HTTPServer.Path, h.getHandler)
	return &h, nil
}

//------------------------------------------------------------------------------

func (h *HTTPServer) getHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

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

	var msg types.Message
	var open bool
	var err error

	select {
	case msg, open = <-h.messages:
		if !open {
			http.Error(w, "Server closed", http.StatusServiceUnavailable)
			go h.CloseAsync()
			return
		}
		h.stats.Incr("output.http_server.count", 1)
	case <-time.After(tOutDuration - time.Since(tStart)):
		http.Error(w, "Timed out waiting for message", http.StatusRequestTimeout)
		return
	}

	if len(msg.Parts) > 1 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for i := 0; i < len(msg.Parts) && err == nil; i++ {
			var part io.Writer
			if part, err = writer.CreatePart(textproto.MIMEHeader{
				"Content-Type": []string{"application/octet-stream"},
			}); err == nil {
				_, err = io.Copy(part, bytes.NewReader(msg.Parts[i]))
			}
		}

		writer.Close()
		w.Header().Add("Content-Type", writer.FormDataContentType())
		w.Write(body.Bytes())
	} else {
		w.Header().Add("Content-Type", "application/octet-stream")
		w.Write(msg.Parts[0])
	}

	h.responseChan <- types.NewSimpleResponse(nil)
	h.stats.Incr("output.http_server.send.success", 1)
}

//------------------------------------------------------------------------------

// StartReceiving assigns a messages channel for the output to read.
func (h *HTTPServer) StartReceiving(msgs <-chan types.Message) error {
	if h.messages != nil {
		return types.ErrAlreadyStarted
	}
	h.messages = msgs

	go func() {
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

		atomic.StoreInt32(&h.running, 0)
		close(h.responseChan)
		close(h.closedChan)
	}()
	return nil
}

// ResponseChan returns the errors channel.
func (h *HTTPServer) ResponseChan() <-chan types.Response {
	return h.responseChan
}

// CloseAsync shuts down the HTTPServer input and stops processing requests.
func (h *HTTPServer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&h.running, 1, 0) {
		h.server.Shutdown(context.Background())
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
