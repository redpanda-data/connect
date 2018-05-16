// Copyright (c) 2018 Ashley Jeffs
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

package manager

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/Jeffail/benthos/lib/stream"
	"github.com/gorilla/mux"
)

//------------------------------------------------------------------------------

func (m *Type) registerEndpoints() {
	m.manager.RegisterEndpoint(
		"/list", "List all streams along with their status and uptimes.",
		m.HandleList,
	)
	m.manager.RegisterEndpoint(
		"/stream/{id}",
		"Perform CRUD operations on streams, supporting POST (Create),"+
			" GET (Read), PUT (Update) and DELETE (Delete).",
		m.HandleCRUD,
	)
}

// HandleList is an http.HandleFunc for returning maps of active benthos
// streams by their id, status and uptime.
func (m *Type) HandleList(w http.ResponseWriter, r *http.Request) {
	var httpErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if httpErr != nil {
			http.Error(w, "Internal server error", http.StatusBadGateway)
		}
	}()

	type confInfo struct {
		Active bool   `json:"active"`
		Uptime string `json:"uptime"`
	}
	infos := map[string]confInfo{}

	m.lock.Lock()
	for id, strInfo := range m.streams {
		infos[id] = confInfo{
			Active: strInfo.IsRunning(),
			Uptime: strInfo.Uptime().String(),
		}
	}
	m.lock.Unlock()

	var resBytes []byte
	if resBytes, httpErr = json.Marshal(infos); httpErr == nil {
		w.Write(resBytes)
	}
}

// HandleCRUD is an http.HandleFunc for performing CRUD operations on streams.
func (m *Type) HandleCRUD(w http.ResponseWriter, r *http.Request) {
	var httpErr error
	defer func() {
		if r.Body != nil {
			r.Body.Close()
		}
		if httpErr != nil {
			m.logger.Warnf("Streams CRUD Error: %v", httpErr)
			http.Error(w, fmt.Sprintf("Error: %v", httpErr), http.StatusBadRequest)
		}
	}()

	id := mux.Vars(r)["id"]
	if len(id) == 0 {
		http.Error(w, "Var `id` must be set", http.StatusBadRequest)
		return
	}

	readConfig := func() (conf stream.Config, err error) {
		var confBytes []byte
		if confBytes, err = ioutil.ReadAll(r.Body); err != nil {
			return
		}

		err = json.Unmarshal(confBytes, &conf)
		return
	}

	deadline, hasDeadline := r.Context().Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(time.Second * 5)
	}

	var conf stream.Config
	switch r.Method {
	case "POST":
		if conf, httpErr = readConfig(); httpErr != nil {
			return
		}
		httpErr = m.Create(id, conf)
	case "GET":
		var info StreamStatus
		if info, httpErr = m.Read(id); httpErr != nil {
			return
		}

		sanit, _ := info.Config.Sanitised()

		var bodyBytes []byte
		if bodyBytes, httpErr = json.Marshal(struct {
			Active bool        `json:"active"`
			Uptime string      `json:"uptime"`
			Config interface{} `json:"config"`
		}{
			Active: info.Active,
			Uptime: info.Uptime.String(),
			Config: sanit,
		}); httpErr != nil {
			return
		}

		w.Write(bodyBytes)
	case "PUT":
		if conf, httpErr = readConfig(); httpErr != nil {
			return
		}
		httpErr = m.Update(id, conf, time.Until(deadline))
	case "DELETE":
		httpErr = m.Delete(id, time.Until(deadline))
	default:
		httpErr = fmt.Errorf("verb not supported: %v", r.Method)
	}
}

//------------------------------------------------------------------------------
