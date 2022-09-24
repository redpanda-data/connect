package aws

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/cloudwatch/cloudwatchiface"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
)

func init() {
	_ = bundle.AllMetrics.Add(func(c metrics.Config, nm bundle.NewManagement) (metrics.Type, error) {
		return newCloudWatch(c.AWSCloudWatch, nm.Logger())
	}, docs.ComponentSpec{
		Name:    "aws_cloudwatch",
		Type:    docs.TypeMetrics,
		Status:  docs.StatusStable,
		Version: "3.36.0",
		Summary: `Send metrics to AWS CloudWatch using the PutMetricData endpoint.`,
		Description: `
### Timing Metrics

The smallest timing unit that CloudWatch supports is microseconds, therefore timing metrics are automatically downgraded to microseconds (by dividing delta values by 1000). This conversion will also apply to custom timing metrics produced with a ` + "`metric`" + ` processor.

### Billing

AWS bills per metric series exported, it is therefore STRONGLY recommended that you reduce the metrics that are exposed with a ` + "`mapping`" + ` like this:

` + "```yaml" + `
metrics:
  mapping: |
    if ![
      "input_received",
      "input_latency",
      "output_sent",
    ].contains(this) { deleted() }
  aws_cloudwatch:
    namespace: Foo
` + "```" + ``,
		Config: docs.FieldComponent().WithChildren(
			append(docs.FieldSpecs{
				docs.FieldString("namespace", "The namespace used to distinguish metrics from other services.").HasDefault("Benthos"),
				docs.FieldString("flush_period", "The period of time between PutMetricData requests.").Advanced().HasDefault("100ms"),
			}, session.FieldSpecs()...)...,
		),
	})
}

//------------------------------------------------------------------------------

const (
	maxCloudWatchMetrics    = 20
	maxCloudWatchValues     = 150
	maxCloudWatchDimensions = 10
)

type cloudWatchDatum struct {
	MetricName string
	Unit       string
	Dimensions []*cloudwatch.Dimension
	Timestamp  time.Time
	Value      int64
	Values     map[int64]int64
}

type cloudWatchStat struct {
	root       *cwMetrics
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
func (c *cloudWatchStat) Incr(count int64) {
	c.addValue(count)
}

// Decr decrements a metric by an amount.
func (c *cloudWatchStat) Decr(count int64) {
	c.addValue(-count)
}

// Timing sets a timing metric.
func (c *cloudWatchStat) Timing(delta int64) {
	// Most granular value for timing metrics in cloudwatch is microseconds
	// versus nanoseconds.
	c.appendValue(delta / 1000)
}

// Set sets a gauge metric.
func (c *cloudWatchStat) Set(value int64) {
	c.appendValue(value)
}

type cloudWatchStatVec struct {
	root       *cwMetrics
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

func (c *cloudWatchCounterVec) With(labelValues ...string) metrics.StatCounter {
	return c.with(labelValues...)
}

type cloudWatchTimerVec struct {
	cloudWatchStatVec
}

func (c *cloudWatchTimerVec) With(labelValues ...string) metrics.StatTimer {
	return c.with(labelValues...)
}

type cloudWatchGaugeVec struct {
	cloudWatchStatVec
}

func (c *cloudWatchGaugeVec) With(labelValues ...string) metrics.StatGauge {
	return c.with(labelValues...)
}

//------------------------------------------------------------------------------

type cwMetrics struct {
	client cloudwatchiface.CloudWatchAPI

	datumses  map[string]*cloudWatchDatum
	datumLock *sync.Mutex

	flushPeriod time.Duration

	ctx    context.Context
	cancel func()

	config metrics.CloudWatchConfig
	log    log.Modular
}

func newCloudWatch(config metrics.CloudWatchConfig, log log.Modular) (metrics.Type, error) {
	c := &cwMetrics{
		config:    config,
		datumses:  map[string]*cloudWatchDatum{},
		datumLock: &sync.Mutex{},
		log:       log,
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())

	sess, err := GetSessionFromConf(config.Config)
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

func (c *cwMetrics) GetCounter(path string) metrics.StatCounter {
	return &cloudWatchStat{
		root: c,
		id:   path,
		name: path,
		unit: cloudwatch.StandardUnitCount,
	}
}

func (c *cwMetrics) GetCounterVec(path string, n ...string) metrics.StatCounterVec {
	return &cloudWatchCounterVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       path,
			unit:       cloudwatch.StandardUnitCount,
			labelNames: n,
		},
	}
}

func (c *cwMetrics) GetTimer(path string) metrics.StatTimer {
	return &cloudWatchStat{
		root: c,
		id:   path,
		name: path,
		unit: cloudwatch.StandardUnitMicroseconds,
	}
}

func (c *cwMetrics) GetTimerVec(path string, n ...string) metrics.StatTimerVec {
	return &cloudWatchTimerVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       path,
			unit:       cloudwatch.StandardUnitMicroseconds,
			labelNames: n,
		},
	}
}

func (c *cwMetrics) GetGauge(path string) metrics.StatGauge {
	return &cloudWatchStat{
		root: c,
		id:   path,
		name: path,
		unit: cloudwatch.StandardUnitNone,
	}
}

func (c *cwMetrics) GetGaugeVec(path string, n ...string) metrics.StatGaugeVec {
	return &cloudWatchGaugeVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       path,
			unit:       cloudwatch.StandardUnitNone,
			labelNames: n,
		},
	}
}

//------------------------------------------------------------------------------

func (c *cwMetrics) loop() {
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

func (c *cwMetrics) flush() error {
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
				return c.ctx.Err()
			}
		}

		if !throttled {
			input.MetricData = datums
		}
	}

	return nil
}

//------------------------------------------------------------------------------

func (c *cwMetrics) HandlerFunc() http.HandlerFunc {
	return nil
}

func (c *cwMetrics) Close() error {
	c.cancel()
	c.flush()
	return nil
}
