// Copyright (c) 2018 Kraig Amador
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

package processor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Jeffail/benthos/lib/log"
	"github.com/Jeffail/benthos/lib/metrics"
	"github.com/Jeffail/benthos/lib/types"
	"github.com/Jeffail/benthos/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetric] = TypeSpec{
		constructor: NewMetric,
		description: `
Creates metrics by extracting values or counting the appearance of values within 
a message. Supported types are ` + "`counter`, `gauge`, and `timing`" + `. Path
describes name of the metric and value the field to extract.

For example, the configuration below will increment the ` +
			"`count.of.times.field.existed`" + ` metric every time the ` + "`field.to.count`" + ` 
field appeared in a message.

` + "``` yaml" + `
type: metric
metric:
  type: counter
  path: count.of.times.field.existed
  value: ${!json_field:field.to.count}
` + "```",
	}
}

//------------------------------------------------------------------------------

// MetricConfig contains configuration fields for the Metric processor.
type MetricConfig struct {
	Type   string            `json:"type" yaml:"type"`
	Path   string            `json:"path" yaml:"path"`
	Labels map[string]string `json:"labels" yaml:"labels"`
	Value  string            `json:"value" yaml:"value"`
}

// NewMetricConfig returns a MetricConfig with default values.
func NewMetricConfig() MetricConfig {
	return MetricConfig{
		Type:  "",
		Path:  "",
		Value: "",
	}
}

//------------------------------------------------------------------------------

// Metric is a processor that creates a metric from extracted values from a message part.
type Metric struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	interpolateValue bool

	labels   labels
	mCounter metrics.StatCounterVec
	mGauge   metrics.StatGaugeVec
	mTimer   metrics.StatTimerVec

	mCount metrics.StatCounter
	mSucc  metrics.StatCounter
	mErr   metrics.StatCounter

	handler func(string, types.Message) error
}

type labels []label
type label struct {
	name             string
	value            string
	interpolateValue bool
}

func (l *label) val(msg types.Message) string {
	if l.interpolateValue {
		return string(text.ReplaceFunctionVariables(msg, []byte(l.value)))
	}
	return l.value
}

func (l labels) names() []string {
	var names []string
	for i := range l {
		names = append(names, l[i].name)
	}
	return names
}

func (l labels) values(msg types.Message) []string {
	var values []string
	for i := range l {
		values = append(values, l[i].val(msg))
	}
	return values
}

// NewMetric returns a Metric processor.
func NewMetric(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (Type, error) {
	m := &Metric{
		conf:             conf,
		log:              log.NewModule(".processor.metric"),
		stats:            stats,
		mCount:           stats.GetCounter("processor.metric.count"),
		mSucc:            stats.GetCounter("processor.metric.success"),
		mErr:             stats.GetCounter("processor.metric.error"),
		interpolateValue: text.ContainsFunctionVariables([]byte(conf.Metric.Value)),
	}

	for n, v := range conf.Metric.Labels {
		m.labels = append(m.labels, label{
			name:             n,
			value:            v,
			interpolateValue: text.ContainsFunctionVariables([]byte(v)),
		})
	}

	switch strings.ToLower(conf.Metric.Type) {
	case "counter":
		m.mCounter = stats.GetCounterVec(conf.Metric.Path, m.labels.names())
		m.handler = m.handleCounter
	case "gauge":
		m.mGauge = stats.GetGaugeVec(conf.Metric.Path, m.labels.names())
		m.handler = m.handleGauge
	case "timer":
		m.mTimer = stats.GetTimerVec(conf.Metric.Path, m.labels.names())
		m.handler = m.handleTimer
	default:
		return nil, fmt.Errorf("failed to create metric from: %v", conf.Metric.Type)
	}

	return m, nil
}

func (m *Metric) handleCounter(val string, msg types.Message) error {
	if val == "" {
		return nil
	}
	m.mCounter.With(m.labels.values(msg)...).Incr(1)
	return nil
}

func (m *Metric) handleGauge(val string, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	m.mGauge.With(m.labels.values(msg)...).Set(i)
	return nil
}

func (m *Metric) handleTimer(val string, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	m.mTimer.With(m.labels.values(msg)...).Timing(i)
	return nil
}

// ProcessMessage applies the processor to a message
func (m *Metric) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	m.mCount.Incr(1)

	value := m.conf.Metric.Value
	if m.interpolateValue {
		value = string(text.ReplaceFunctionVariables(msg, []byte(m.conf.Metric.Value)))
	}

	err := m.handler(value, msg)
	if err != nil {
		m.mErr.Incr(1)
	} else {
		m.mSucc.Incr(1)
	}

	return []types.Message{msg}, nil
}
