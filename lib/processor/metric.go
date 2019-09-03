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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/text"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetric] = TypeSpec{
		constructor: NewMetric,
		description: `
Expose custom metrics by extracting values from message batches. This processor
executes once per batch, in order to execute once per message place it within a
` + "[`for_each`](#for_each)" + ` processor:

` + "``` yaml" + `
for_each:
- metric:
    type: counter_by
    path: count.custom.field
    value: ${!json_field:field.some.value}
` + "```" + `

The ` + "`path`" + ` field should be a dot separated path of the metric to be
set and will automatically be converted into the correct format of the
configured metric aggregator.

The ` + "`value`" + ` field can be set using function interpolations described
[here](../config_interpolation.md#functions) and is used according to the
specific type.

### Types

#### ` + "`counter`" + `

Increments a counter by exactly 1, the contents of ` + "`value`" + ` are ignored
by this type.

#### ` + "`counter_parts`" + `

Increments a counter by the number of parts within the message batch, the
contents of ` + "`value`" + ` are ignored by this type.

#### ` + "`counter_by`" + `

If the contents of ` + "`value`" + ` can be parsed as a positive integer value
then the counter is incremented by this value.

For example, the following configuration will increment the value of the
` + "`count.custom.field` metric by the contents of `field.some.value`" + `:

` + "``` yaml" + `
metric:
  type: counter_by
  path: count.custom.field
  value: ${!json_field:field.some.value}
` + "```" + `

#### ` + "`gauge`" + `

If the contents of ` + "`value`" + ` can be parsed as a positive integer value
then the gauge is set to this value.

For example, the following configuration will set the value of the
` + "`gauge.custom.field` metric to the contents of `field.some.value`" + `:

` + "``` yaml" + `
metric:
  type: gauge
  path: gauge.custom.field
  value: ${!json_field:field.some.value}
` + "```" + `

#### ` + "`timing`" + `

Equivalent to ` + "`gauge`" + ` where instead the metric is a timing.

### Labels

Some metrics aggregators, such as Prometheus, support arbitrary labels, in which
case the ` + "`labels`" + ` field can be used in order to create them. Label
values can also be set using function interpolations in order to dynamically
populate them with context about the message.`,
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
		Type:   "counter",
		Path:   "",
		Labels: map[string]string{},
		Value:  "",
	}
}

//------------------------------------------------------------------------------

// Metric is a processor that creates a metric from extracted values from a message part.
type Metric struct {
	conf  Config
	log   log.Modular
	stats metrics.Type

	interpolateValue bool

	labels labels

	mCounter metrics.StatCounter
	mGauge   metrics.StatGauge
	mTimer   metrics.StatTimer

	mCounterVec metrics.StatCounterVec
	mGaugeVec   metrics.StatGaugeVec
	mTimerVec   metrics.StatTimerVec

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
		log:              log,
		stats:            stats,
		interpolateValue: text.ContainsFunctionVariables([]byte(conf.Metric.Value)),
	}

	if len(conf.Metric.Path) == 0 {
		return nil, errors.New("path must not be empty")
	}

	labelNames := make([]string, 0, len(conf.Metric.Labels))
	for n := range conf.Metric.Labels {
		labelNames = append(labelNames, n)
	}
	sort.Strings(labelNames)

	for _, n := range labelNames {
		v := conf.Metric.Labels[n]
		m.labels = append(m.labels, label{
			name:             n,
			value:            v,
			interpolateValue: text.ContainsFunctionVariables([]byte(v)),
		})
	}

	switch strings.ToLower(conf.Metric.Type) {
	case "counter":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(conf.Metric.Path, m.labels.names())
		} else {
			m.mCounter = stats.GetCounter(conf.Metric.Path)
		}
		m.handler = m.handleCounter
	case "counter_parts":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(conf.Metric.Path, m.labels.names())
		} else {
			m.mCounter = stats.GetCounter(conf.Metric.Path)
		}
		m.handler = m.handleCounterParts
	case "counter_by":
		if len(m.labels) > 0 {
			m.mCounterVec = stats.GetCounterVec(conf.Metric.Path, m.labels.names())
		} else {
			m.mCounter = stats.GetCounter(conf.Metric.Path)
		}
		m.handler = m.handleCounterBy
	case "gauge":
		if len(m.labels) > 0 {
			m.mGaugeVec = stats.GetGaugeVec(conf.Metric.Path, m.labels.names())
		} else {
			m.mGauge = stats.GetGauge(conf.Metric.Path)
		}
		m.handler = m.handleGauge
	case "timing":
		if len(m.labels) > 0 {
			m.mTimerVec = stats.GetTimerVec(conf.Metric.Path, m.labels.names())
		} else {
			m.mTimer = stats.GetTimer(conf.Metric.Path)
		}
		m.handler = m.handleTimer
	default:
		return nil, fmt.Errorf("metric type unrecognised: %v", conf.Metric.Type)
	}

	return m, nil
}

func (m *Metric) handleCounter(val string, msg types.Message) error {
	if len(m.labels) > 0 {
		m.mCounterVec.With(m.labels.values(msg)...).Incr(1)
	} else {
		m.mCounter.Incr(1)
	}
	return nil
}

func (m *Metric) handleCounterParts(val string, msg types.Message) error {
	if msg.Len() == 0 {
		return nil
	}
	if len(m.labels) > 0 {
		m.mCounterVec.With(m.labels.values(msg)...).Incr(int64(msg.Len()))
	} else {
		m.mCounter.Incr(int64(msg.Len()))
	}
	return nil
}

func (m *Metric) handleCounterBy(val string, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		m.mCounterVec.With(m.labels.values(msg)...).Incr(i)
	} else {
		m.mCounter.Incr(i)
	}
	return nil
}

func (m *Metric) handleGauge(val string, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		m.mGaugeVec.With(m.labels.values(msg)...).Set(i)
	} else {
		m.mGauge.Set(i)
	}
	return nil
}

func (m *Metric) handleTimer(val string, msg types.Message) error {
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	if i < 0 {
		return errors.New("value is negative")
	}
	if len(m.labels) > 0 {
		m.mTimerVec.With(m.labels.values(msg)...).Timing(i)
	} else {
		m.mTimer.Timing(i)
	}
	return nil
}

// ProcessMessage applies the processor to a message
func (m *Metric) ProcessMessage(msg types.Message) ([]types.Message, types.Response) {
	value := m.conf.Metric.Value
	if m.interpolateValue {
		value = string(text.ReplaceFunctionVariables(msg, []byte(m.conf.Metric.Value)))
	}

	if err := m.handler(value, msg); err != nil {
		m.log.Errorf("Handler error: %v\n", err)
	}

	return []types.Message{msg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (m *Metric) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (m *Metric) WaitForClose(timeout time.Duration) error {
	return nil
}
