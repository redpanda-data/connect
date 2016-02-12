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

package input

import (
	"io/ioutil"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jeffail/benthos/types"
	"github.com/jeffail/util/log"
	"github.com/jeffail/util/metrics"
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["http_server"] = NewHTTPServer
}

//--------------------------------------------------------------------------------------------------

// HTTPServerConfig - Configuration for the HTTPServer input type.
type HTTPServerConfig struct {
	Address        string `json:"address" yaml:"address"`
	Path           string `json:"path" yaml:"path"`
	TimeoutMS      int64  `json:"timeout_ms" yaml:"timeout_ms"`
	FullForwarding bool   `json:"full_contents_forwarding" yaml:"full_contents_forwarding"`
}

// NewHTTPServerConfig - Creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:        "localhost:8080",
		Path:           "/post",
		TimeoutMS:      5000,
		FullForwarding: false,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTPServer - An input type that serves HTTPServer POST requests.
type HTTPServer struct {
	running int32

	conf  Config
	stats metrics.Aggregator
	log   *log.Logger

	mux *http.ServeMux

	internalMessages chan [][]byte

	messages  chan types.Message
	responses <-chan types.Response

	closedChan chan struct{}

	sync.Mutex
}

// NewHTTPServer - Create a new HTTPServer input type.
func NewHTTPServer(conf Config, log *log.Logger, stats metrics.Aggregator) (Type, error) {
	h := HTTPServer{
		running:          1,
		conf:             conf,
		stats:            stats,
		log:              log.NewModule(".input.http"),
		mux:              http.NewServeMux(),
		internalMessages: make(chan [][]byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closedChan:       make(chan struct{}),
	}

	h.mux.HandleFunc(h.conf.HTTPServer.Path, h.postHandler)

	go func() {
		err := http.ListenAndServe(h.conf.HTTPServer.Address, h.mux)
		panic(err)
	}()

	return &h, nil
}

//--------------------------------------------------------------------------------------------------

func (h *HTTPServer) postHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}

	var msg types.Message

	if r.Header.Get("Content-Type") == "application/x-benthos-multipart" {
		msg, err = types.FromBytes(bytes)
		if err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}
	} else {
		msg.Parts = [][]byte{bytes}
	}

	h.Lock()
	defer h.Unlock()
	select {
	case h.internalMessages <- msg.Parts:
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
	}
	return
}

//--------------------------------------------------------------------------------------------------

func (h *HTTPServer) loop() {
	defer func() {
		close(h.messages)
		close(h.closedChan)
	}()

	var data [][]byte
	var open bool

	h.log.Infof(
		"Receiving HTTP Post messages at: %s\n",
		h.conf.HTTPServer.Address+h.conf.HTTPServer.Path,
	)

	for atomic.LoadInt32(&h.running) == 1 {
		if data == nil {
			data, open = <-h.internalMessages
			if !open {
				return
			}
		}
		if data != nil {
			h.messages <- types.Message{Parts: data}

			var res types.Response
			res, open = <-h.responses
			if !open {
				atomic.StoreInt32(&h.running, 0)
			} else if res.Error() == nil {
				data = nil
			}
		}
	}
}

// StartListening - Sets the channel used by the input to validate message receipt.
func (h *HTTPServer) StartListening(responses <-chan types.Response) error {
	if h.responses != nil {
		return types.ErrAlreadyStarted
	}
	h.responses = responses
	go h.loop()
	return nil
}

// MessageChan - Returns the messages channel.
func (h *HTTPServer) MessageChan() <-chan types.Message {
	return h.messages
}

// CloseAsync - Shuts down the HTTPServer input and stops processing requests.
func (h *HTTPServer) CloseAsync() {
	iMsgs := h.internalMessages
	h.Lock()
	h.internalMessages = nil
	close(iMsgs)
	h.Unlock()

	atomic.StoreInt32(&h.running, 0)
}

// WaitForClose - Blocks until the HTTPServer input has closed down.
func (h *HTTPServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
