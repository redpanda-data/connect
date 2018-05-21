// Copyright (c) 2014 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, sub to the following conditions:
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

package metrics

import (
	"reflect"
	"testing"
)

func TestInterfaces(t *testing.T) {
	foo, err := New(NewConfig())
	if err != nil {
		t.Error(err)
	}
	bar := Type(foo)
	foo.Incr("nope", 1)
	bar.Incr("nope", 1)

	foo.Decr("nope", 1)
	bar.Decr("nope", 1)

	foo.Gauge("foo", 1)
	foo.Gauge("foo", 2)

	foo.Timing("bar", 1)
	foo.Timing("bar", 2)
}

func TestSanitise(t *testing.T) {
	exp := map[string]interface{}{
		"type":        "http_server",
		"http_server": map[string]interface{}{},
	}

	conf := NewConfig()
	conf.Type = "http_server"

	act, err := SanitiseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}

	exp = map[string]interface{}{
		"type": "statsd",
		"statsd": map[string]interface{}{
			"address":         "foo",
			"flush_period":    "100ms",
			"max_packet_size": float64(1440),
			"network":         "udp",
		},
	}

	conf = NewConfig()
	conf.Type = "statsd"
	conf.Statsd.Address = "foo"

	act, err = SanitiseConfig(conf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(act, exp) {
		t.Errorf("Wrong sanitised output: %v != %v", act, exp)
	}
}
