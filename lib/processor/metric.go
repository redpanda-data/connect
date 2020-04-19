package processor

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMetric] = TypeSpec{
		constructor: NewMetric,
		Summary: `
Expose custom metrics by extracting values from message batches.`,
		Description: `
This processor executes once per batch, in order to execute once per message
place it within a ` + "[`for_each`](/docs/components/processors/for_each)" + ` processor:

` + "```yaml" + `
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
[here](/docs/configuration/interpolation#functions) and is used according to the
specific type.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("type", "The metric [type](#types) to create.").HasOptions(),
			docs.FieldCommon("path", "The path of the metric to create."),
			docs.FieldCommon(
				"labels", "A map of label names and values that can be used to enrich metrics with aggregators such as Prometheus.",
				map[string]string{
					"type":  "${!json_field:doc.type}",
					"topic": "${!metadata:kafka_topic}",
				},
			).SupportsInterpolation(true),
			docs.FieldCommon("value", "For some metric types specifies a value to set, increment.").SupportsInterpolation(true),
		},
		Footnotes: `
## Types

### ` + "`counter`" + `

Increments a counter by exactly 1, the contents of ` + "`value`" + ` are ignored
by this type.

### ` + "`counter_parts`" + `

Increments a counter by the number of parts within the message batch, the
contents of ` + "`value`" + ` are ignored by this type.

### ` + "`counter_by`" + `

If the contents of ` + "`value`" + ` can be parsed as a positive integer value
then the counter is incremented by this value.

For example, the following configuration will increment the value of the
` + "`count.custom.field` metric by the contents of `field.some.value`" + `:

` + "```yaml" + `
metric:
  type: counter_by
  path: count.custom.field
  value: ${!json_field:field.some.value}
` + "```" + `

### ` + "`gauge`" + `

If the contents of ` + "`value`" + ` can be parsed as a positive integer value
then the gauge is set to this value.

For example, the following configuration will set the value of the
` + "`gauge.custom.field` metric to the contents of `field.some.value`" + `:

` + "```yaml" + `
metric:
  type: gauge
  path: gauge.custom.field
  value: ${!json_field:field.some.value}
` + "```" + `

### ` + "`timing`" + `

Equivalent to ` + "`gauge`" + ` where instead the metric is a timing.`,
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

	value  field.Expression
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
	name  string
	value field.Expression
}

func (l *label) val(msg types.Message) string {
	return l.value.String(0, msg)
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
	value, err := field.New(conf.Metric.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value expression: %v", err)
	}

	m := &Metric{
		conf:  conf,
		log:   log,
		stats: stats,
		value: value,
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
		v, err := field.New(conf.Metric.Labels[n])
		if err != nil {
			return nil, fmt.Errorf("failed to parse label '%v' expression: %v", n, err)
		}
		m.labels = append(m.labels, label{
			name:  n,
			value: v,
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
	value := m.value.String(0, msg)
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
