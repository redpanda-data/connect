package metrics

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/gabs/v2"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeStdout] = TypeSpec{
		constructor: NewStdout,
		Status:      docs.StatusBeta,
		Summary: `
Prints aggregated metrics as JSON objects to stdout.`,
		Description: `
When Benthos shuts down all aggregated metrics are printed. If a
` + "`push_interval`" + ` is specified then metrics are also printed
periodically.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("push_interval", "An optional period of time to continuously print metrics."),
			docs.FieldCommon("static_fields", "A map of static fields to add to each flushed metric object.").Map(),
			docs.FieldCommon("flush_metrics", "Whether counters and timing metrics should be reset to 0 each time metrics are printed."),
			pathMappingDocs(false, false),
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
	PathMapping  string                 `json:"path_mapping" yaml:"path_mapping"`
}

// NewStdoutConfig returns a new StdoutConfig with default values.
func NewStdoutConfig() StdoutConfig {
	return StdoutConfig{
		PushInterval: "",
		StaticFields: map[string]interface{}{
			"@service": "benthos",
		},
		FlushMetrics: false,
		PathMapping:  "",
	}
}

//------------------------------------------------------------------------------

// Stdout is an object with capability to hold internal stats and emit them as
// individual JSON objects via stdout.
type Stdout struct {
	local     *Local
	timestamp time.Time
	log       log.Modular

	pathMapping *pathMapping
	config      StdoutConfig
	closedChan  chan struct{}
	running     int32

	staticFields []byte
}

// NewStdout creates and returns a new Stdout metric object.
func NewStdout(config Config, opts ...func(Type)) (Type, error) {
	t := &Stdout{
		local:      NewLocal(),
		timestamp:  time.Now(),
		config:     config.Stdout,
		closedChan: make(chan struct{}),
		running:    1,
	}

	//TODO: add field interpolation here
	sf, err := json.Marshal(config.Stdout.StaticFields)
	if err != nil {
		return t, fmt.Errorf("failed to parse static fields: %v", err)
	}
	t.staticFields = sf

	for _, opt := range opts {
		opt(t)
	}

	if t.pathMapping, err = newPathMapping(t.config.PathMapping, t.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}

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

// writeMetric prints a metric object with any configured extras merged in to
// t.
func (s *Stdout) writeMetric(metricSet *gabs.Container) {
	base, _ := gabs.ParseJSON(s.staticFields)
	base.SetP(time.Now().Format(time.RFC3339), "@timestamp")
	base.Merge(metricSet)

	fmt.Printf("%s\n", base.String())
}

// publishMetrics
func (s *Stdout) publishMetrics() {
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
func (s *Stdout) constructMetrics(co map[string]*gabs.Container, metrics map[string]int64) {
	for k, v := range metrics {
		kParts := strings.Split(k, ".")
		var objKey string
		var valKey string
		// walk key parts backwards building up objects of instances of processor/broker/etc
		for i := len(kParts) - 1; i >= 0; i-- {
			if _, err := strconv.Atoi(kParts[i]); err == nil {
				// part is a reference to an index of a processor/broker/etc
				objKey = strings.Join(kParts[:i+1], ".")
				valKey = strings.Join(kParts[i+1:], ".")
				break
			}
		}

		if objKey == "" {
			// key is not referencing an 'instance' of a processor/broker/etc
			objKey = kParts[0]
			valKey = strings.Join(kParts[0:], ".")
		}

		_, exists := co[objKey]
		if !exists {
			co[objKey] = gabs.New()
			co[objKey].SetP(objKey, "metric")
			co[objKey].SetP(kParts[0], "component")

		}
		co[objKey].SetP(v, valKey)
	}
}

// GetCounter returns a stat counter object for a path.
func (s *Stdout) GetCounter(path string) StatCounter {
	if path = s.pathMapping.mapPathNoTags(path); len(path) == 0 {
		return DudStat{}
	}
	return s.local.GetCounter(path)
}

// GetCounterVec returns a stat counter object for a path with the labels
// discarded.
func (s *Stdout) GetCounterVec(path string, n []string) StatCounterVec {
	path = s.pathMapping.mapPathNoTags(path)
	return fakeCounterVec(func([]string) StatCounter {
		if len(path) == 0 {
			return DudStat{}
		}
		return s.local.GetCounter(path)
	})
}

// GetTimer returns a stat timer object for a path.
func (s *Stdout) GetTimer(path string) StatTimer {
	if path = s.pathMapping.mapPathNoTags(path); len(path) == 0 {
		return DudStat{}
	}
	return s.local.GetTimer(path)
}

// GetTimerVec returns a stat timer object for a path with the labels
// discarded.
func (s *Stdout) GetTimerVec(path string, n []string) StatTimerVec {
	path = s.pathMapping.mapPathNoTags(path)
	return fakeTimerVec(func([]string) StatTimer {
		if len(path) == 0 {
			return DudStat{}
		}
		return s.local.GetTimer(path)
	})
}

// GetGauge returns a stat gauge object for a path.
func (s *Stdout) GetGauge(path string) StatGauge {
	if path = s.pathMapping.mapPathNoTags(path); len(path) == 0 {
		return DudStat{}
	}
	return s.local.GetGauge(path)
}

// GetGaugeVec returns a stat timer object for a path with the labels
// discarded.
func (s *Stdout) GetGaugeVec(path string, n []string) StatGaugeVec {
	path = s.pathMapping.mapPathNoTags(path)
	return fakeGaugeVec(func([]string) StatGauge {
		if len(path) == 0 {
			return DudStat{}
		}
		return s.local.GetGauge(path)
	})
}

// SetLogger does nothing.
func (s *Stdout) SetLogger(log log.Modular) {
	s.log = log
}

// Close stops the Stdout object from aggregating metrics and does a publish
// (write to stdout) of metrics.
func (s *Stdout) Close() error {
	if atomic.CompareAndSwapInt32(&s.running, 1, 0) {
		close(s.closedChan)
	}
	s.publishMetrics()

	return nil
}

//------------------------------------------------------------------------------
