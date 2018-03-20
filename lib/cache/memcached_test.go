// Copyright (c) 2018 Lorenzo Alberton
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

// +build integration

package cache

import (
	"os"
	"testing"

	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/service/log"
	"github.com/Jeffail/benthos/lib/util/service/metrics"
)

func TestMemcachedAddDuplicate(t *testing.T) {
	conf := NewConfig()
	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	c, err := NewMemcached(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Set("benthos_test_connection", []byte("hello world")); err != nil {
		t.Skipf("Memcached server not available: %v", err)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}

	if err = c.Add("benthos_test_foo", []byte("bar")); err != nil {
		t.Error(err)
	}
	if err = c.Add("benthos_test_foo", []byte("baz")); err != types.ErrKeyAlreadyExists {
		t.Errorf("Wrong error returned: %v != %v", err, types.ErrKeyAlreadyExists)
	}

	exp := "bar"
	var act []byte

	if act, err = c.Get("benthos_test_foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong value returned: %v != %v", string(act), exp)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}
}

func TestMemcachedGetAndSet(t *testing.T) {
	conf := NewConfig()
	testLog := log.NewLogger(os.Stdout, log.LoggerConfig{LogLevel: "NONE"})
	c, err := NewMemcached(conf, nil, testLog, metrics.DudType{})
	if err != nil {
		t.Fatal(err)
	}

	if err = c.Set("benthos_test_connection", []byte("hello world")); err != nil {
		t.Skipf("Memcached server not available: %v", err)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}

	if err = c.Set("benthos_test_foo", []byte("bar")); err != nil {
		t.Error(err)
	}

	exp := "bar"
	var act []byte

	if act, err = c.Get("benthos_test_foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong value returned: %v != %v", string(act), exp)
	}

	if err = c.Set("benthos_test_foo", []byte("baz")); err != nil {
		t.Error(err)
	}

	exp = "baz"
	act = nil

	if act, err = c.Get("benthos_test_foo"); err != nil {
		t.Error(err)
	} else if string(act) != exp {
		t.Errorf("Wrong value returned: %v != %v", string(act), exp)
	}

	if err = c.Delete("benthos_test_foo"); err != nil {
		t.Error(err)
	}
}
