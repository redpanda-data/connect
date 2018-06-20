// Copyright (c) 2018 Ashley Jeffs
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

import "github.com/Jeffail/benthos/lib/log"

//------------------------------------------------------------------------------

// namespacedWrapper wraps an existing Type under a namespace. The full path of
// subsequent metrics under this type will be:
//
// Underlying prefix + this namespace + path of metric
type namespacedWrapper struct {
	ns string
	t  Type
}

// Namespaced embeds an existing metrics aggregator under a new namespace. The
// prefix of the embedded aggregator is still the ultimate prefix of metrics.
func Namespaced(t Type, ns string) Type {
	return namespacedWrapper{
		ns: ns,
		t:  t,
	}
}

//------------------------------------------------------------------------------

func (d namespacedWrapper) GetCounter(path ...string) StatCounter {
	return d.t.GetCounter(append([]string{d.ns}, path...)...)
}

func (d namespacedWrapper) GetTimer(path ...string) StatTimer {
	return d.t.GetTimer(append([]string{d.ns}, path...)...)
}

func (d namespacedWrapper) GetGauge(path ...string) StatGauge {
	return d.t.GetGauge(append([]string{d.ns}, path...)...)
}

func (d namespacedWrapper) Incr(path string, count int64) error {
	return d.t.Incr(d.ns+"."+path, count)
}

func (d namespacedWrapper) Decr(path string, count int64) error {
	return d.t.Decr(d.ns+"."+path, count)
}

func (d namespacedWrapper) Timing(path string, delta int64) error {
	return d.t.Timing(d.ns+"."+path, delta)
}

func (d namespacedWrapper) Gauge(path string, value int64) error {
	return d.t.Gauge(d.ns+"."+path, value)
}

func (d namespacedWrapper) SetLogger(log log.Modular) {
	d.t.SetLogger(log.NewModule(d.ns))
}

func (d namespacedWrapper) Close() error {
	return d.t.Close()
}

//------------------------------------------------------------------------------
