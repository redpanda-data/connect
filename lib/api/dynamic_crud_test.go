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

package api

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/gorilla/mux"
)

//------------------------------------------------------------------------------

func TestDynamicConfMgr(t *testing.T) {
	hasher := newDynamicConfMgr()
	hasher.Remove("foo")

	if hasher.Matches("foo", []byte("test")) {
		t.Error("matched hash on non-existing id")
	}

	if !hasher.Set("foo", []byte("test")) {
		t.Error("Collision on new id")
	}

	if !hasher.Matches("foo", []byte("test")) {
		t.Error("Non-matched on same content")
	}

	if hasher.Matches("foo", []byte("test 2")) {
		t.Error("Matched on different content")
	}

	if hasher.Set("foo", []byte("test")) {
		t.Error("Non-collision on existing id")
	}

	if !hasher.Set("foo", []byte("test 2")) {
		t.Error("Collision on new content")
	}

	if !hasher.Matches("foo", []byte("test 2")) {
		t.Error("Non-matched on same content")
	}

	if hasher.Matches("foo", []byte("test")) {
		t.Error("Matched on different content")
	}

	if hasher.Set("foo", []byte("test 2")) {
		t.Error("Non-collision on existing content")
	}
}

//------------------------------------------------------------------------------

func router(dAPI *Dynamic) *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/inputs", dAPI.HandleList)
	router.HandleFunc("/input/{id}", dAPI.HandleCRUD)
	return router
}

func TestDynamicCRUDBadReqs(t *testing.T) {
	dAPI := NewDynamic()
	r := router(dAPI)

	request, _ := http.NewRequest("DERP", "/input/foo", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadGateway, response.Code; exp != act {
		t.Errorf("Unexpected response code: %v != %v", act, exp)
	}
}

func TestDynamicDelete(t *testing.T) {
	dAPI := NewDynamic()
	r := router(dAPI)

	expRemoved := []string{}
	removed := []string{}
	failRemove := true

	dAPI.OnDelete(func(id string) error {
		if failRemove {
			return errors.New("foo err")
		}
		removed = append(removed, id)
		return nil
	})
	dAPI.OnUpdate(func(id string, content []byte) ([]byte, error) {
		t.Error("Unexpected update called")
		return nil, nil
	})

	request, _ := http.NewRequest("DELETE", "/input/foo", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusBadGateway, response.Code; exp != act {
		t.Errorf("Unexpected response code: %v != %v", act, exp)
	}
	if exp, act := expRemoved, removed; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong collection of removed configs: %v != %v", act, exp)
	}

	failRemove = false
	expRemoved = append(expRemoved, "foo")
	request, _ = http.NewRequest("DELETE", "/input/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected response code: %v != %v", act, exp)
	}
	if exp, act := expRemoved, removed; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong collection of removed configs: %v != %v", act, exp)
	}
}

func TestDynamicBasicCRUD(t *testing.T) {
	dAPI := NewDynamic()
	r := router(dAPI)

	deleteExp := ""
	var deleteErr error
	dAPI.OnDelete(func(id string) error {
		if exp, act := deleteExp, id; exp != act {
			t.Errorf("Wrong content on delete: %v != %v", act, exp)
		}
		return deleteErr
	})

	updateExp := []byte("hello world")
	updateRes := []byte("foo bar")
	var updateErr error
	dAPI.OnUpdate(func(id string, content []byte) ([]byte, error) {
		if exp, act := updateExp, content; !reflect.DeepEqual(exp, act) {
			t.Errorf("Wrong content on update: %s != %s", act, exp)
		}
		return updateRes, updateErr
	})

	request, _ := http.NewRequest("GET", "/input/foo", nil)
	response := httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusNotFound, response.Code; exp != act {
		t.Errorf("Unexpected response code: %v != %v", act, exp)
	}

	request, _ = http.NewRequest("POST", "/input/foo", bytes.NewReader([]byte("hello world")))
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected response code: %v != %v", act, exp)
	}

	request, _ = http.NewRequest("GET", "/input/foo", nil)
	response = httptest.NewRecorder()
	r.ServeHTTP(response, request)
	if exp, act := http.StatusOK, response.Code; exp != act {
		t.Errorf("Unexpected response code: %v != %v", act, exp)
	}
	if exp, act := []byte("foo bar"), response.Body.Bytes(); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong content on GET: %s != %s", act, exp)
	}
}

//------------------------------------------------------------------------------
