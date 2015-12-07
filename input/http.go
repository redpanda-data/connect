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
)

//--------------------------------------------------------------------------------------------------

func init() {
	constructors["http"] = NewHTTP
}

//--------------------------------------------------------------------------------------------------

// HTTPConfig - Configuration for the HTTP input type.
type HTTPConfig struct {
	Address   string `json:"address" yaml:"address"`
	RootPath  string `json:"root_path" yaml:"root_path"`
	POSTPath  string `json:"POST_path" yaml:"POST_path"`
	WSPath    string `json:"websocket_path" yaml:"websocket_path"`
	TimeoutMS int64  `json:"timeout_ms" yaml:"timeout_ms"`
}

// NewHTTPConfig - Creates a new HTTPConfig with default values.
func NewHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Address:   "localhost:8080",
		RootPath:  "/",
		POSTPath:  "/post",
		WSPath:    "/ws",
		TimeoutMS: 5000,
	}
}

//--------------------------------------------------------------------------------------------------

// HTTP - An input type that serves HTTP POST requests.
type HTTP struct {
	running int32

	conf Config

	mux *http.ServeMux

	internalMessages chan [][]byte

	messages  chan types.Message
	responses <-chan types.Response

	closedChan chan struct{}

	sync.Mutex
}

// NewHTTP - Create a new HTTP input type.
func NewHTTP(conf Config) (Type, error) {
	h := HTTP{
		running:          1,
		conf:             conf,
		mux:              http.NewServeMux(),
		internalMessages: make(chan [][]byte),
		messages:         make(chan types.Message),
		responses:        nil,
		closedChan:       make(chan struct{}),
	}

	h.mux.HandleFunc(path.Join(h.conf.HTTP.RootPath, h.conf.HTTP.POSTPath), h.postHandler)
	h.mux.Handle(path.Join(h.conf.HTTP.RootPath, h.conf.HTTP.WSPath), websocket.Handler(h.wsHandler))

	go func() {
		err := http.ListenAndServe(h.conf.HTTP.Address, h.mux)
		panic(err)
	}()

	return &h, nil
}

//--------------------------------------------------------------------------------------------------

type response struct {
	Error string `json:"error"`
}

type request struct {
	Parts []string `json:"parts"`
}

func (h *HTTP) postHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Incorrect method", http.StatusMethodNotAllowed)
		return
	}
	bytes, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Bad request", http.StatusBadRequest)
		return
	}
	var req request
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
	case <-time.After(time.Millisecond * time.Duration(h.conf.HTTP.TimeoutMS)):
		http.Error(w, "Request timed out", http.StatusRequestTimeout)
	}
	return
}

func (h *HTTP) wsHandler(ws *websocket.Conn) {
	for atomic.LoadInt32(&h.running) == 1 {
		var req request
		if err := websocket.JSON.Receive(ws, &req); err == nil {
			var msg types.Message
			msg.Parts = make([][]byte, len(req.Parts))
			for i := range req.Parts {
				msg.Parts[i] = []byte(req.Parts[i])
			}

			h.Lock()
			select {
			case h.internalMessages <- msg.Parts:
				err = websocket.JSON.Send(ws, &response{
					Error: "",
				})
			case <-time.After(time.Millisecond * time.Duration(h.conf.HTTP.TimeoutMS)):
				err = websocket.JSON.Send(ws, &response{
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

func (h *HTTP) loop() {
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
func (h *HTTP) StartListening(responses <-chan types.Response) error {
	if h.responses != nil {
		return types.ErrAlreadyStarted
	}
	h.responses = responses
	go h.loop()
	return nil
}

// MessageChan - Returns the messages channel.
func (h *HTTP) MessageChan() <-chan types.Message {
	return h.messages
}

// CloseAsync - Shuts down the HTTP input and stops processing requests.
func (h *HTTP) CloseAsync() {
	iMsgs := h.internalMessages
	h.Lock()
	h.internalMessages = nil
	close(iMsgs)
	h.Unlock()

	atomic.StoreInt32(&h.running, 0)
}

// WaitForClose - Blocks until the HTTP input has closed down.
func (h *HTTP) WaitForClose(timeout time.Duration) error {
	select {
	case <-h.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//--------------------------------------------------------------------------------------------------
