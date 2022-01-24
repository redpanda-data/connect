package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeAWSCloudWatch] = TypeSpec{
		constructor: NewAWSCloudWatch,
		Version:     "3.36.0",
		Summary: `
Send metrics to AWS CloudWatch using the PutMetricData endpoint.`,
		Description: `
It is STRONGLY recommended that you reduce the metrics that are exposed with a
` + "`path_mapping`" + ` like this:

` + "```yaml" + `
metrics:
  aws_cloudwatch:
    namespace: Foo
    path_mapping: |
      if ![
        "input.received",
        "input.latency",
        "output.sent",
      ].contains(this) { deleted() }
` + "```" + ``,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldCommon("namespace", "The namespace used to distinguish metrics from other services."),
			docs.FieldAdvanced("flush_period", "The period of time between PutMetricData requests."),
			pathMappingDocs(true, false),
		}, session.FieldSpecs()...),
	}
}

//------------------------------------------------------------------------------

// CloudWatchConfig contains config fields for the CloudWatch metrics type.
type CloudWatchConfig struct {
	session.Config `json:",inline" yaml:",inline"`
	Namespace      string `json:"namespace" yaml:"namespace"`
	FlushPeriod    string `json:"flush_period" yaml:"flush_period"`
	PathMapping    string `json:"path_mapping" yaml:"path_mapping"`
}

// NewCloudWatchConfig creates an CloudWatchConfig struct with default values.
func NewCloudWatchConfig() CloudWatchConfig {
	return CloudWatchConfig{
		Config:      session.NewConfig(),
		Namespace:   "Benthos",
		FlushPeriod: "100ms",
		PathMapping: "",
	}
}

//------------------------------------------------------------------------------

const maxCloudWatchMetrics = 20
const maxCloudWatchValues = 150
const maxCloudWatchDimensions = 10

type cloudWatchDatum struct {
	MetricName string
	Unit       string
	Dimensions []*cloudwatch.Dimension
	Timestamp  time.Time
	Value      int64
	Values     map[int64]int64
}

type cloudWatchStat struct {
	root       *CloudWatch
	id         string
	name       string
	unit       string
	dimensions []*cloudwatch.Dimension
}

// Trims a map of datum values to a ceiling. The primary goal here is to be fast
// and efficient rather than accurately preserving the most common values.
func trimValuesMap(m map[int64]int64) {
	ceiling := maxCloudWatchValues

	// Start off by randomly removing values that have been seen only once.
	for k, v := range m {
		if len(m) <= ceiling {
			// If we reach our ceiling already then we're done.
			return
		}
		if v == 1 {
			delete(m, k)
		}
	}

	// Next, randomly remove any values until ceiling is hit.
	for k := range m {
		if len(m) <= ceiling {
			return
		}
		delete(m, k)
	}
}

func (c *cloudWatchStat) appendValue(v int64) {
	c.root.datumLock.Lock()
	existing := c.root.datumses[c.id]
	if existing == nil {
		existing = &cloudWatchDatum{
			MetricName: c.name,
			Unit:       c.unit,
			Dimensions: c.dimensions,
			Timestamp:  time.Now(),
			Values:     map[int64]int64{v: 1},
		}
		c.root.datumses[c.id] = existing
	} else {
		tally := existing.Values[v]
		existing.Values[v] = tally + 1
		if len(existing.Values) > maxCloudWatchValues*5 {
			trimValuesMap(existing.Values)
		}
	}
	c.root.datumLock.Unlock()
}

func (c *cloudWatchStat) addValue(v int64) {
	c.root.datumLock.Lock()
	existing := c.root.datumses[c.id]
	if existing == nil {
		existing = &cloudWatchDatum{
			MetricName: c.name,
			Unit:       c.unit,
			Dimensions: c.dimensions,
			Timestamp:  time.Now(),
			Value:      v,
		}
		c.root.datumses[c.id] = existing
	} else {
		existing.Value += v
	}
	c.root.datumLock.Unlock()
}

// Incr increments a metric by an amount.
func (c *cloudWatchStat) Incr(count int64) error {
	c.addValue(count)
	return nil
}

// Decr decrements a metric by an amount.
func (c *cloudWatchStat) Decr(count int64) error {
	c.addValue(-count)
	return nil
}

// Timing sets a timing metric.
func (c *cloudWatchStat) Timing(delta int64) error {
	// Most granular value for timing metrics in cloudwatch is microseconds
	// versus nanoseconds.
	c.appendValue(delta / 1000)
	return nil
}

// Set sets a gauge metric.
func (c *cloudWatchStat) Set(value int64) error {
	c.appendValue(value)
	return nil
}

type cloudWatchStatVec struct {
	root       *CloudWatch
	name       string
	unit       string
	labelNames []string
}

func (c *cloudWatchStatVec) with(labelValues ...string) *cloudWatchStat {
	lDim := len(c.labelNames)
	if lDim >= maxCloudWatchDimensions {
		lDim = maxCloudWatchDimensions
	}
	dimensions := make([]*cloudwatch.Dimension, lDim)
	for i, k := range c.labelNames {
		if len(labelValues) <= i || i >= maxCloudWatchDimensions {
			break
		}
		dimensions[i] = &cloudwatch.Dimension{
			Name:  aws.String(k),
			Value: aws.String(labelValues[i]),
		}
	}
	return &cloudWatchStat{
		root:       c.root,
		id:         c.name + fmt.Sprintf("%v", labelValues),
		name:       c.name,
		unit:       c.unit,
		dimensions: dimensions,
	}
}

type cloudWatchCounterVec struct {
	cloudWatchStatVec
}

func (c *cloudWatchCounterVec) With(labelValues ...string) StatCounter {
	return c.with(labelValues...)
}

type cloudWatchTimerVec struct {
	cloudWatchStatVec
}

func (c *cloudWatchTimerVec) With(labelValues ...string) StatTimer {
	return c.with(labelValues...)
}

type cloudWatchGaugeVec struct {
	cloudWatchStatVec
}

func (c *cloudWatchGaugeVec) With(labelValues ...string) StatGauge {
	return c.with(labelValues...)
}

//------------------------------------------------------------------------------

// CloudWatch is a stats object with capability to hold internal stats as a JSON
// endpoint.
type CloudWatch struct {
	client cloudwatchiface.CloudWatchAPI

	datumses  map[string]*cloudWatchDatum
	datumLock *sync.Mutex

	flushPeriod time.Duration

	ctx    context.Context
	cancel func()

	pathMapping *pathMapping
	config      CloudWatchConfig
	log         log.Modular
}

// NewAWSCloudWatch creates and returns a new CloudWatch object.
func NewAWSCloudWatch(config Config, opts ...func(Type)) (Type, error) {
	return newCloudWatch(config.AWSCloudWatch, opts...)
}

func newCloudWatch(config CloudWatchConfig, opts ...func(Type)) (Type, error) {
	c := &CloudWatch{
		config:    config,
		datumses:  map[string]*cloudWatchDatum{},
		datumLock: &sync.Mutex{},
		log:       log.Noop(),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(c)
	}

	var err error
	if c.pathMapping, err = newPathMapping(config.PathMapping, c.log); err != nil {
		return nil, fmt.Errorf("failed to init path mapping: %v", err)
	}

	sess, err := config.GetSession()
	if err != nil {
		return nil, err
	}

	if c.flushPeriod, err = time.ParseDuration(config.FlushPeriod); err != nil {
		return nil, fmt.Errorf("failed to parse flush period: %v", err)
	}

	c.client = cloudwatch.New(sess)
	go c.loop()
	return c, nil
}

//------------------------------------------------------------------------------

func (c *CloudWatch) toCMName(dotSepName string) (outPath string, labelNames, labelValues []string) {
	return c.pathMapping.mapPathWithTags(dotSepName)
}

// GetCounter returns a stat counter object for a path.
func (c *CloudWatch) GetCounter(path string) StatCounter {
	name, labels, values := c.toCMName(path)
	if name == "" {
		return DudStat{}
	}
	if len(labels) == 0 {
		return &cloudWatchStat{
			root: c,
			id:   name,
			name: name,
			unit: cloudwatch.StandardUnitCount,
		}
	}
	return (&cloudWatchCounterVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       name,
			unit:       cloudwatch.StandardUnitCount,
			labelNames: labels,
		},
	}).With(values...)
}

// GetCounterVec returns a stat counter object for a path with the labels
func (c *CloudWatch) GetCounterVec(path string, n []string) StatCounterVec {
	name, labels, values := c.toCMName(path)
	if name == "" {
		return fakeCounterVec(func([]string) StatCounter {
			return DudStat{}
		})
	}
	if len(labels) > 0 {
		labels = append(labels, n...)
		return fakeCounterVec(func(vs []string) StatCounter {
			fvs := append([]string{}, values...)
			fvs = append(fvs, vs...)
			return (&cloudWatchCounterVec{
				cloudWatchStatVec: cloudWatchStatVec{
					root:       c,
					name:       name,
					unit:       cloudwatch.StandardUnitCount,
					labelNames: labels,
				},
			}).With(fvs...)
		})
	}
	return &cloudWatchCounterVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       name,
			unit:       cloudwatch.StandardUnitCount,
			labelNames: n,
		},
	}
}

// GetTimer returns a stat timer object for a path.
func (c *CloudWatch) GetTimer(path string) StatTimer {
	name, labels, values := c.toCMName(path)
	if name == "" {
		return DudStat{}
	}
	if len(labels) == 0 {
		return &cloudWatchStat{
			root: c,
			id:   name,
			name: name,
			unit: cloudwatch.StandardUnitMicroseconds,
		}
	}
	return (&cloudWatchTimerVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       name,
			unit:       cloudwatch.StandardUnitMicroseconds,
			labelNames: labels,
		},
	}).With(values...)
}

// GetTimerVec returns a stat timer object for a path with the labels
func (c *CloudWatch) GetTimerVec(path string, n []string) StatTimerVec {
	name, labels, values := c.toCMName(path)
	if name == "" {
		return fakeTimerVec(func([]string) StatTimer {
			return DudStat{}
		})
	}
	if len(labels) > 0 {
		labels = append(labels, n...)
		return fakeTimerVec(func(vs []string) StatTimer {
			fvs := append([]string{}, values...)
			fvs = append(fvs, vs...)
			return (&cloudWatchTimerVec{
				cloudWatchStatVec: cloudWatchStatVec{
					root:       c,
					name:       name,
					unit:       cloudwatch.StandardUnitMicroseconds,
					labelNames: labels,
				},
			}).With(fvs...)
		})
	}
	return &cloudWatchTimerVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       name,
			unit:       cloudwatch.StandardUnitMicroseconds,
			labelNames: n,
		},
	}
}

// GetGauge returns a stat gauge object for a path.
func (c *CloudWatch) GetGauge(path string) StatGauge {
	name, labels, values := c.toCMName(path)
	if name == "" {
		return DudStat{}
	}
	if len(labels) == 0 {
		return &cloudWatchStat{
			root: c,
			id:   name,
			name: name,
			unit: cloudwatch.StandardUnitNone,
		}
	}
	return (&cloudWatchGaugeVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       name,
			unit:       cloudwatch.StandardUnitNone,
			labelNames: labels,
		},
	}).With(values...)
}

// GetGaugeVec returns a stat timer object for a path with the labels
func (c *CloudWatch) GetGaugeVec(path string, n []string) StatGaugeVec {
	name, labels, values := c.toCMName(path)
	if name == "" {
		return fakeGaugeVec(func([]string) StatGauge {
			return DudStat{}
		})
	}
	if len(labels) > 0 {
		labels = append(labels, n...)
		return fakeGaugeVec(func(vs []string) StatGauge {
			fvs := append([]string{}, values...)
			fvs = append(fvs, vs...)
			return (&cloudWatchGaugeVec{
				cloudWatchStatVec: cloudWatchStatVec{
					root:       c,
					name:       name,
					unit:       cloudwatch.StandardUnitNone,
					labelNames: labels,
				},
			}).With(fvs...)
		})
	}
	return &cloudWatchGaugeVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       name,
			unit:       cloudwatch.StandardUnitNone,
			labelNames: n,
		},
	}
}

//------------------------------------------------------------------------------

func (c *CloudWatch) loop() {
	ticker := time.NewTicker(c.flushPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.flush()
		}
	}
}

func valuesMapToSlices(m map[int64]int64) (values, counts []*float64) {
	ceiling := maxCloudWatchValues
	lM := len(m)

	useCounts := false
	if lM < ceiling {
		values = make([]*float64, 0, lM)
		counts = make([]*float64, 0, lM)

		for k, v := range m {
			values = append(values, aws.Float64(float64(k)))
			counts = append(counts, aws.Float64(float64(v)))
			if v > 1 {
				useCounts = true
			}
		}

		if !useCounts {
			counts = nil
		}
		return
	}

	values = make([]*float64, 0, ceiling)
	counts = make([]*float64, 0, ceiling)

	// Try and make our target without taking values with one count.
	for k, v := range m {
		if len(values) == ceiling {
			return
		}
		if v > 1 {
			values = append(values, aws.Float64(float64(k)))
			counts = append(counts, aws.Float64(float64(v)))
			useCounts = true
			delete(m, k)
		}
	}

	// Otherwise take randomly.
	for k, v := range m {
		if len(values) == ceiling {
			break
		}
		values = append(values, aws.Float64(float64(k)))
		counts = append(counts, aws.Float64(float64(v)))
	}

	if !useCounts {
		counts = nil
	}
	return
}

func (c *CloudWatch) flush() error {
	c.datumLock.Lock()
	datumMap := c.datumses
	c.datumses = map[string]*cloudWatchDatum{}
	c.datumLock.Unlock()

	datums := []*cloudwatch.MetricDatum{}
	for _, v := range datumMap {
		if v != nil {
			d := cloudwatch.MetricDatum{
				MetricName: &v.MetricName,
				Dimensions: v.Dimensions,
				Unit:       &v.Unit,
				Timestamp:  &v.Timestamp,
			}
			if len(v.Values) > 0 {
				d.Values, d.Counts = valuesMapToSlices(v.Values)
			} else {
				d.Value = aws.Float64(float64(v.Value))
			}
			datums = append(datums, &d)
		}
	}

	input := cloudwatch.PutMetricDataInput{
		Namespace:  &c.config.Namespace,
		MetricData: datums,
	}

	throttled := false
	for len(input.MetricData) > 0 {
		if !throttled {
			if len(datums) > maxCloudWatchMetrics {
				input.MetricData, datums = datums[:maxCloudWatchMetrics], datums[maxCloudWatchMetrics:]
			} else {
				datums = nil
			}
		}
		throttled = false

		if _, err := c.client.PutMetricData(&input); err != nil {
			if request.IsErrorThrottle(err) {
				throttled = true
				c.log.Warnln("Metrics request was throttled. Either increase flush period or reduce number of services sending metrics.")
			} else {
				c.log.Errorf("Failed to send metric data: %v\n", err)
			}
			select {
			case <-time.After(time.Second):
			case <-c.ctx.Done():
				return types.ErrTimeout
			}
		}

		if !throttled {
			input.MetricData = datums
		}
	}

	return nil
}

//------------------------------------------------------------------------------

// SetLogger sets the logger used to print connection errors.
func (c *CloudWatch) SetLogger(log log.Modular) {
	c.log = log
}

// Close stops the CloudWatch object from aggregating metrics and cleans up
// resources.
func (c *CloudWatch) Close() error {
	c.cancel()
	c.flush()
	return nil
}

//------------------------------------------------------------------------------
