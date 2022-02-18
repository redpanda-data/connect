package metrics

import (
	"encoding/json"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/gabs/v2"
)

func init() {
	Constructors[TypeStdout] = TypeSpec{
		constructor: newStdout,
		Status:      docs.StatusBeta,
		Summary: `
Prints aggregated metrics as JSON objects to stdout.`,
		Description: `
When Benthos shuts down all aggregated metrics are printed. If a
` + "`push_interval`" + ` is specified then metrics are also printed
periodically.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("push_interval", "An optional period of time to continuously print metrics."),
			docs.FieldCommon("static_fields", "A map of static fields to add to each flushed metric object.").Map().HasDefault(map[string]interface{}{
				"@service": "benthos",
			}).HasType(docs.FieldTypeUnknown),
			docs.FieldCommon("flush_metrics", "Whether counters and timing metrics should be reset to 0 each time metrics are printed."),
		},
	}
}

//------------------------------------------------------------------------------

// StdoutConfig contains configuration parameters for the Stdout metrics
// aggregator.
type StdoutConfig struct {
	PushInterval string                 `json:"push_interval" yaml:"push_interval"`
	StaticFields map[string]interface{} `json:"static_fields" yaml:"static_fields"`
	FlushMetrics bool                   `json:"flush_metrics" yaml:"flush_metrics"`
}

// NewStdoutConfig returns a new StdoutConfig with default values.
func NewStdoutConfig() StdoutConfig {
	return StdoutConfig{
		PushInterval: "",
		StaticFields: map[string]interface{}{
			"@service": "benthos",
		},
		FlushMetrics: false,
	}
}

//------------------------------------------------------------------------------

type stdoutMetrics struct {
	local     *Local
	timestamp time.Time
	log       log.Modular

	config     StdoutConfig
	closedChan chan struct{}
	running    int32

	staticFields []byte
}

func newStdout(config Config, log log.Modular) (Type, error) {
	t := &stdoutMetrics{
		local:      NewLocal(),
		timestamp:  time.Now(),
		config:     config.Stdout,
		closedChan: make(chan struct{}),
		running:    1,
		log:        log,
	}

	sf, err := json.Marshal(config.Stdout.StaticFields)
	if err != nil {
		return t, fmt.Errorf("failed to parse static fields: %v", err)
	}
	t.staticFields = sf

	if len(t.config.PushInterval) > 0 {
		interval, err := time.ParseDuration(t.config.PushInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse push interval: %v", err)
		}
		go func() {
			for {
				select {
				case <-t.closedChan:
					return
				case <-time.After(interval):
					t.publishMetrics()
				}
			}
		}()
	}

	return t, nil
}

//------------------------------------------------------------------------------

func (s *stdoutMetrics) writeMetric(metricSet *gabs.Container) {
	base, _ := gabs.ParseJSON(s.staticFields)
	base.SetP(time.Now().Format(time.RFC3339), "@timestamp")
	base.Merge(metricSet)

	fmt.Printf("%s\n", base.String())
}

func (s *stdoutMetrics) publishMetrics() {
	counterObjs := make(map[string]*gabs.Container)

	var counters map[string]int64
	var timings map[string]int64
	if s.config.FlushMetrics {
		counters = s.local.FlushCounters()
		timings = s.local.FlushTimings()
	} else {
		counters = s.local.GetCounters()
		timings = s.local.GetTimings()
	}

	s.constructMetrics(counterObjs, counters)
	s.constructMetrics(counterObjs, timings)

	system := make(map[string]int64)
	uptime := time.Since(s.timestamp).Milliseconds()
	goroutines := runtime.NumGoroutine()
	system["system.uptime"] = uptime
	system["system.goroutines"] = int64(goroutines)
	s.constructMetrics(counterObjs, system)

	for _, o := range counterObjs {
		s.writeMetric(o)
	}
}

// constructMetrics groups individual Benthos metrics contained in a map into
// a container for each component instance.  For example,
// pipeline.processor.1.count and pipeline.processor.1.error would be grouped
// into a single pipeline.processor.1 object.
func (s *stdoutMetrics) constructMetrics(co map[string]*gabs.Container, metrics map[string]int64) {
	for k, v := range metrics {
		parts := strings.Split(k, ".")
		var objKey string
		var valKey string
		// walk key parts backwards building up objects of instances of processor/broker/etc
		for i := len(parts) - 1; i >= 0; i-- {
			if _, err := strconv.Atoi(parts[i]); err == nil {
				// part is a reference to an index of a processor/broker/etc
				objKey = strings.Join(parts[:i+1], ".")
				valKey = strings.Join(parts[i+1:], ".")
				break
			}
		}

		if objKey == "" {
			// key is not referencing an 'instance' of a processor/broker/etc
			objKey = parts[0]
			valKey = strings.Join(parts[0:], ".")
		}

		_, exists := co[objKey]
		if !exists {
			co[objKey] = gabs.New()
			co[objKey].SetP(objKey, "metric")
			co[objKey].SetP(parts[0], "component")

		}
		co[objKey].SetP(v, valKey)
	}
}

func (s *stdoutMetrics) GetCounter(path string) StatCounter {
	return s.GetCounterVec(path).With()
}

func (s *stdoutMetrics) GetCounterVec(path string, n ...string) StatCounterVec {
	return s.local.GetCounterVec(path, n...)
}

func (s *stdoutMetrics) GetTimer(path string) StatTimer {
	return s.GetTimerVec(path).With()
}

func (s *stdoutMetrics) GetTimerVec(path string, n ...string) StatTimerVec {
	return s.local.GetTimerVec(path, n...)
}

func (s *stdoutMetrics) GetGauge(path string) StatGauge {
	return s.GetGaugeVec(path).With()
}

func (s *stdoutMetrics) GetGaugeVec(path string, n ...string) StatGaugeVec {
	return s.local.GetGaugeVec(path, n...)
}

func (s *stdoutMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (s *stdoutMetrics) Close() error {
	if atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		close(s.closedChan)
	}
	s.publishMetrics()

	return nil
}
