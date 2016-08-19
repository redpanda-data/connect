/*
Copyright (c) 2014 Ashley Jeffs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package output

import (
	"bytes"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/lib/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["http_server"] = typeSpec{
		constructor: NewHTTPServer,
		description: `
Sets up an HTTP server that will return messages over HTTP GET requests.`,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTPServerConfig - Configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address         string `json:"address" yaml:"address"`
	Path            string `json:"path" yaml:"path"`
	ServerTimeoutMS int64  `json:"server_timeout_ms" yaml:"server_timeout_ms"`
	ClientTimeoutMS int64  `json:"client_timeout_ms" yaml:"client_timeout_ms"`
}

// NewHTTPServerConfig - Creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:         "localhost:8081",
		Path:            "/get",
		ServerTimeoutMS: 5000,
		ClientTimeoutMS: 5000,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTPServer - An input type that serves HTTPServer POST requests.
type HTTPServer struct {
	running int32

	conf  Config
	stats metrics.Aggregator
	log   log.Modular

	mux *http.ServeMux

	messages     <-chan types.Message
	responseChan chan types.Response
}

// NewHTTPServer - Create a new HTTPServer input type.
func NewHTTPServer(conf Config, log log.Modular, stats metrics.Aggregator) (Type, error) {
	h := HTTPServer{
		running:      1,
		conf:         conf,
		stats:        stats,
		log:          log.NewModule(".output.http_server"),
		mux:          http.NewServeMux(),
		messages:     nil,
		responseChan: make(chan types.Response),
	}

	h.mux.HandleFunc(h.conf.HTTPServer.Path, h.getHandler)
	return &h, nil
}

//--------------------------------------------------------------------------------------------------

func (h *HTTPServer) getHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&h.running) != 1 {
		http.Error(w, "Server closed", http.StatusServiceUnavailable)
		return
	}

	if r.Method != "GET" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}

	tStart := time.Now()
	tOutDuration := time.Millisecond * time.Duration(h.conf.HTTPServer.ClientTimeoutMS)

	var msg types.Message
	var open bool
	var err error

	select {
	case msg, open = <-h.messages:
		if !open {
			http.Error(w, "Server closed", http.StatusServiceUnavailable)
			atomic.StoreInt32(&h.running, 0)
			return
		}
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
		w.Write(msg.Bytes())
	}

	h.responseChan <- types.NewSimpleResponse(nil)
}

//--------------------------------------------------------------------------------------------------

// StartReceiving - Assigns a messages channel for the output to read.
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
		err := http.ListenAndServe(h.conf.HTTPServer.Address, h.mux)
		panic(err)
	}()
	return nil
}

// ResponseChan - Returns the errors channel.
func (h *HTTPServer) ResponseChan() <-chan types.Response {
	return h.responseChan
}

// CloseAsync - Shuts down the HTTPServer input and stops processing requests.
func (h *HTTPServer) CloseAsync() {
	atomic.StoreInt32(&h.running, 0)
}

// WaitForClose - Blocks until the HTTPServer output has closed down.
func (h *HTTPServer) WaitForClose(timeout time.Duration) error {
	// NOTE: Using the default HTTP server means we haven't got an explicit method for shutting the
	// server down and waiting for completion.
	tStart := time.Now()
	for atomic.LoadInt32(&h.running) == 1 {
		if time.Since(tStart) >= timeout {
			return types.ErrTimeout
		}
		<-time.After(time.Millisecond * 100)
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
