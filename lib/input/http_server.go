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
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
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
Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS,
which is enabled when key and cert files are specified.

You can leave the 'address' config field blank in order to use the default
service, but this will ignore TLS options.`,
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
		Address:   "",
		Path:      "/post",
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

	transactions chan types.Transaction

	closeChan  chan struct{}
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
		running:      1,
		conf:         conf,
		stats:        stats,
		log:          log.NewModule(".input.http"),
		mux:          mux,
		server:       server,
		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	if mux != nil {
		mux.HandleFunc(h.conf.HTTPServer.Path, h.postHandler)
	} else {
		mgr.RegisterEndpoint(
			h.conf.HTTPServer.Path, "Post a message into Benthos.", h.postHandler,
		)
	}

	go h.loop()
	return &h, nil
}

//------------------------------------------------------------------------------

func (h *HTTPServer) postHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	if atomic.LoadInt32(&h.running) != 1 {
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	h.stats.Incr("input.http_server.count", 1)

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

	resChan := make(chan types.Response)
	select {
	case h.transactions <- types.NewTransaction(msg, resChan):
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
		h.stats.Incr("input.http_server.send.timeout", 1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		return
	case <-h.closeChan:
		http.Error(w, "Server closing", http.StatusServiceUnavailable)
		return
	}

	select {
	case res, open := <-resChan:
		if !open {
			http.Error(w, "Server closing", http.StatusServiceUnavailable)
			return
		} else if res.Error() != nil {
			h.stats.Incr("input.http_server.send.error", 1)
			http.Error(w, res.Error().Error(), http.StatusBadGateway)
			return
		}
		h.stats.Incr("input.http_server.send.success", 1)
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
		h.stats.Incr("input.http_server.send.timeout", 1)
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
		go func() {
			// Even if the request times out, we still need to drain a response.
			resAsync := <-resChan
			if resAsync.Error() != nil {
				h.stats.Incr("input.http_server.send.async_error", 1)
			} else {
				h.stats.Incr("input.http_server.send.async_success", 1)
			}
		}()
		return
	}
}

//------------------------------------------------------------------------------

func (h *HTTPServer) loop() {
	defer func() {
		atomic.StoreInt32(&h.running, 0)

		if h.server != nil {
			h.server.Shutdown(context.Background())
		}

		h.stats.Decr("input.http_server.running", 1)

		close(h.transactions)
		close(h.closedChan)
	}()
	h.stats.Incr("input.http_server.running", 1)

	if h.server != nil {
		h.log.Infof(
			"Receiving HTTP Post messages at: %s\n",
			h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
		)

		go func() {
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
		}()
	}

	<-h.closeChan
}

// TransactionChan returns the transactions channel.
func (h *HTTPServer) TransactionChan() <-chan types.Transaction {
	return h.transactions
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
