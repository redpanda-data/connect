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

package input

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//------------------------------------------------------------------------------

func init() {
	constructors["http_server"] = typeSpec{
		constructor: NewHTTPServer,
		description: `
In order to receive messages over HTTP Benthos hosts a server. Messages should
be sent as a POST request. HTTP 1.1 is currently supported and HTTP 2.0 is
planned for the future.`,
	}
}

//------------------------------------------------------------------------------

// Errors for the HTTPServer type.
var (
	ErrHWMInvalid = errors.New("high water mark is invalid (must be integer greater than 1)")
)

// HTTPServerConfig is configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address   string `json:"address" yaml:"address"`
	Path      string `json:"path" yaml:"path"`
	TimeoutMS int64  `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewHTTPServerConfig creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:   "localhost:8080",
		Path:      "/post",
		TimeoutMS: 5000,
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

	messages  chan types.Message
	responses <-chan types.Response

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewHTTPServer creates a new HTTPServer input type.
func NewHTTPServer(conf Config, log log.Modular, stats metrics.Type) (Type, error) {
	mux := http.NewServeMux()
	server := &http.Server{Addr: conf.HTTPServer.Address, Handler: mux}

	h := HTTPServer{
		running:    1,
		conf:       conf,
		stats:      stats,
		log:        log.NewModule(".input.http"),
		mux:        mux,
		server:     server,
		messages:   make(chan types.Message),
		responses:  nil,
		closeChan:  make(chan struct{}),
		closedChan: make(chan struct{}),
	}

	mux.HandleFunc(h.conf.HTTPServer.Path, h.postHandler)

	return &h, nil
}

//------------------------------------------------------------------------------

func (h *HTTPServer) postHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&h.running) != 1 {
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	var msg types.Message
	var err error

	defer func() {
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			h.log.Warnf("Request read failed: %v\n", err)
			return
		}
	}()

	var mediaType string
	var params map[string]string
	if mediaType, params, err = mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil {
		return
	}

	if strings.HasPrefix(mediaType, "multipart/") {
		msg.Parts = [][]byte{}
		mr := multipart.NewReader(r.Body, params["boundary"])
		for {
			var p *multipart.Part
			if p, err = mr.NextPart(); err != nil {
				if err == io.EOF {
					err = nil
					break
				}
				return
			}
			var msgBytes []byte
			if msgBytes, err = ioutil.ReadAll(p); err != nil {
				return
			}
			msg.Parts = append(msg.Parts, msgBytes)
		}
	} else {
		var msgBytes []byte
		if msgBytes, err = ioutil.ReadAll(r.Body); err != nil {
			return
		}
		msg.Parts = [][]byte{msgBytes}
	}

	select {
	case h.messages <- msg:
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.closeChan:
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	select {
	case res, open := <-h.responses:
		if !open {
			http.Error(w, "Server closing", http.StatusServiceUnavailable)
			return
		} else if res.Error() != nil {
			http.Error(w, res.Error().Error(), http.StatusBadGateway)
			return
		}
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		go func() {
			// Even if the request times out, we still need to drain a response.
			<-h.responses
		}()
		return
	}
}

//------------------------------------------------------------------------------

func (h *HTTPServer) loop() {
	defer func() {
		atomic.StoreInt32(&h.running, 0)

		h.server.Shutdown(context.Background())

		close(h.messages)
		close(h.closedChan)
	}()

	h.log.Infof(
		"Receiving HTTP Post messages at: %s\n",
		h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
	)

	go func() {
		if err := h.server.ListenAndServe(); err != http.ErrServerClosed {
			h.log.Errorf("Server error: %v\n", err)
		}
	}()

	<-h.closeChan
}

// StartListening sets the channel used by the input to validate message
// receipt.
func (h *HTTPServer) StartListening(responses <-chan types.Response) error {
	if h.responses != nil {
		return types.ErrAlreadyStarted
	}
	h.responses = responses
	go h.loop()
	return nil
}

// MessageChan returns the messages channel.
func (h *HTTPServer) MessageChan() <-chan types.Message {
	return h.messages
}

// CloseAsync shuts down the HTTPServer input and stops processing requests.
func (h *HTTPServer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&h.running, 1, 0) {
		close(h.closeChan)
	}
}

// WaitForClose blocks until the HTTPServer input has closed down.
func (h *HTTPServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
