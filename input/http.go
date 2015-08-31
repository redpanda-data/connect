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
	"time"

	"github.com/jeffail/benthos/types"
)

//--------------------------------------------------------------------------------------------------

// HTTPConfig - Configuration for the HTTP input type.
type HTTPConfig struct {
	Address string `json:"address" yaml:"address"`
	Path    string `json:"path" yaml:"path"`
}

// NewHTTPConfig - Creates a new HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Address: "localhost:8090",
		Path:    "/input",
	}
}

//--------------------------------------------------------------------------------------------------

// HTTP - An input type that serves HTTP POST requests.
type HTTP struct {
	conf Config

	closedChan chan struct{}
	closeChan  chan struct{}

	internalQueue chan types.Message
	messages      chan types.Message
	mux           *http.ServeMux
}

// NewHTTP - Create a new HTTP input type.
func NewHTTP(conf Config) *HTTP {
	mux := http.NewServeMux()

	h := HTTP{
		conf:          conf,
		closedChan:    make(chan struct{}),
		closeChan:     make(chan struct{}),
		internalQueue: make(chan types.Message),
		messages:      make(chan types.Message),
		mux:           mux,
	}

	mux.HandleFunc(conf.HTTP.Path, h.serveRequest)

	// TODO: Implement a closable server.
	go func() {
		if err := http.ListenAndServe(conf.HTTP.Address, mux); err != nil {
			panic(err)
		}
	}()

	go h.loop()

	return &h
}

//--------------------------------------------------------------------------------------------------

// serveRequest - Serve HTTP requests to POST messages.
func (m *HTTP) serveRequest(w http.ResponseWriter, r *http.Request) {
	select {
	case <-m.closeChan:
		http.Error(w, "Service is closing", 503)
		return
	default:
	}

	if r.Method != "POST" {
		http.Error(w, "Not supported", http.StatusMethodNotAllowed)
		return
	}

	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Not supported", http.StatusBadRequest)
		return
	}

	m.internalQueue <- types.Message{
		Content: bytes,
	}
}

// loop - Internal loop brokers incoming messages to output pipe.
func (m *HTTP) loop() {
	running := true
	for running {
		select {
		case _, running = <-m.closeChan:
		case msg := <-m.internalQueue:
			m.messages <- msg
		default:
		}
	}

	// Drain all remaining internal messages
	for {
		select {
		case msg := <-m.internalQueue:
			m.messages <- msg
		default:
			// Once drained close our output channels and return
			close(m.messages)
			close(m.closedChan)
			return
		}
	}
}

// ConsumerChan - Returns the messages channel.
func (m *HTTP) ConsumerChan() <-chan types.Message {
	return m.messages
}

// CloseAsync - Shuts down the HTTP server and stops processing requests.
func (m *HTTP) CloseAsync() {
	close(m.closeChan)
}

// WaitForClose - Blocks until the HTTP server has closed down.
func (m *HTTP) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
