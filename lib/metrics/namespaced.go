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

import "github.com/Jeffail/benthos/v3/lib/log"

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

func (d namespacedWrapper) GetCounter(path string) StatCounter {
	return d.t.GetCounter(d.ns + "." + path)
}

func (d namespacedWrapper) GetCounterVec(path string, labelNames []string) StatCounterVec {
	return d.t.GetCounterVec(d.ns+"."+path, labelNames)
}

func (d namespacedWrapper) GetTimer(path string) StatTimer {
	return d.t.GetTimer(d.ns + "." + path)
}

func (d namespacedWrapper) GetTimerVec(path string, labelNames []string) StatTimerVec {
	return d.t.GetTimerVec(d.ns+"."+path, labelNames)
}

func (d namespacedWrapper) GetGauge(path string) StatGauge {
	return d.t.GetGauge(d.ns + "." + path)
}

func (d namespacedWrapper) GetGaugeVec(path string, labelNames []string) StatGaugeVec {
	return d.t.GetGaugeVec(d.ns+"."+path, labelNames)
}

func (d namespacedWrapper) SetLogger(log log.Modular) {
	d.t.SetLogger(log.NewModule(d.ns))
}

func (d namespacedWrapper) Close() error {
	return d.t.Close()
}

//------------------------------------------------------------------------------
