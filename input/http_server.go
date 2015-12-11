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
	"encoding/json"
	"io/ioutil"
	"net/http"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/websocket"

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
	Address   string `json:"address" yaml:"address"`
	RootPath  string `json:"root_path" yaml:"root_path"`
	POSTPath  string `json:"POST_path" yaml:"POST_path"`
	WSPath    string `json:"websocket_path" yaml:"websocket_path"`
	TimeoutMS int64  `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewHTTPServerConfig - Creates a new HTTPServerConfig with default values.
func NewHTTPServerConfig() HTTPServerConfig {
	return HTTPServerConfig{
		Address:   "localhost:8080",
		RootPath:  "/",
		POSTPath:  "/post",
		WSPath:    "/ws",
		TimeoutMS: 5000,
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

	h.mux.HandleFunc(path.Join(h.conf.HTTPServer.RootPath, h.conf.HTTPServer.POSTPath), h.postHandler)
	h.mux.Handle(path.Join(h.conf.HTTPServer.RootPath, h.conf.HTTPServer.WSPath), websocket.Handler(h.wsHandler))

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
	var req types.HTTPMessage
	err = json.Unmarshal(bytes, &req)
	if err != nil {
		http.Error(w, "Failed to parse JSON", http.StatusBadRequest)
		return
	}
	var msg types.Message
	msg.Parts = make([][]byte, len(req.Parts))
	for i := range req.Parts {
		msg.Parts[i] = []byte(req.Parts[i])
	}

	h.Lock()
	defer h.Unlock()
	select {
	case h.internalMessages <- msg.Parts:
		enc := json.NewEncoder(w)
		if err = enc.Encode(types.HTTPResponse{}); err != nil {
			h.log.Errorf("Failed to write out HTTPServer response: %v\n", err)
		}
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
	}
	return
}

func (h *HTTPServer) wsHandler(ws *websocket.Conn) {
	for atomic.LoadInt32(&h.running) == 1 {
		var req types.HTTPMessage
		if err := websocket.JSON.Receive(ws, &req); err == nil {
			var msg types.Message
			msg.Parts = make([][]byte, len(req.Parts))
			for i := range req.Parts {
				msg.Parts[i] = []byte(req.Parts[i])
			}

			h.Lock()
			select {
			case h.internalMessages <- msg.Parts:
				err = websocket.JSON.Send(ws, &types.HTTPResponse{
					Error: "",
				})
			case <-time.After(time.Millisecond * time.Duration(h.conf.HTTPServer.TimeoutMS)):
				err = websocket.JSON.Send(ws, &types.HTTPResponse{
					Error: "request timed out",
				})
			}
			h.Unlock()

			if err != nil {
				return
			}
		} else {
			// Websocket is closed, exit handler.
			return
		}
	}
	ws.Close()
}

//--------------------------------------------------------------------------------------------------

func (h *HTTPServer) loop() {
	defer func() {
		close(h.messages)
		close(h.closedChan)
	}()

	var data [][]byte
	var open bool

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
