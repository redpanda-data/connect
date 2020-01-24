package metrics

import (
	"context"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeCloudWatch] = TypeSpec{
		constructor: NewCloudWatch,
		Summary: `
Send metrics to AWS CloudWatch.

ALPHA: This metrics target is experimental, untested, and subject to breaking
changes outside of major releases. It also wrote Game of Thrones Season 8.`,
		Description: `
It is STRONGLY recommended that you [whitelist](/docs/components/metrics/whitelist)
metrics, here's an example:

` + "```yaml" + `
metrics:
  whitelist:
    paths:
      - input.received
      - input.latency
      - output.sent
    child:
      cloudwatch:
        namespace: Foo
` + "```" + ``,
		FieldSpecs: append(docs.FieldSpecs{
			docs.FieldCommon("namespace", "The namespace used to distinguish metrics from other services."),
		}, session.FieldSpecs()...),
	}
}

//------------------------------------------------------------------------------

// CloudWatchConfig contains config fields for the CloudWatch metrics type.
type CloudWatchConfig struct {
	session.Config `json:",inline" yaml:",inline"`
	Namespace      string `json:"namespace" yaml:"namespace"`
}

// NewCloudWatchConfig creates an CloudWatchConfig struct with default values.
func NewCloudWatchConfig() CloudWatchConfig {
	return CloudWatchConfig{
		Config:    session.NewConfig(),
		Namespace: "Benthos",
	}
}

//------------------------------------------------------------------------------

const maxCloudWatchValues = 250

type cloudWatchStat struct {
	root       *CloudWatch
	name       string
	unit       string
	dimensions []*cloudwatch.Dimension
}

func (c *cloudWatchStat) appendValue(v int64) {
	c.root.datumLock.Lock()
	existing := c.root.datumses[c.name]
	if existing == nil {
		existing = &cloudwatch.MetricDatum{
			MetricName: &c.name,
			Unit:       &c.unit,
			Dimensions: c.dimensions,
			Values:     []*float64{aws.Float64(float64(v))},
		}
		c.root.datumses[c.name] = existing
	} else {
		existing.Values = append(existing.Values, aws.Float64(float64(v)))
		if len(existing.Values) > maxCloudWatchValues {
			// Trim first value if we're maxed out
			c.root.log.Tracef("Hit max values for metric '%v'\n", c.name)
			existing.Values = existing.Values[1:]
		}
	}
	c.root.datumLock.Unlock()
}

func (c *cloudWatchStat) addValue(v int64) {
	c.root.datumLock.Lock()
	existing := c.root.datumses[c.name]
	if existing == nil {
		existing = &cloudwatch.MetricDatum{
			MetricName: &c.name,
			Unit:       &c.unit,
			Dimensions: c.dimensions,
			Value:      aws.Float64(float64(v)),
		}
		c.root.datumses[c.name] = existing
	} else {
		existing.Value = aws.Float64(*existing.Value + float64(v))
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
	c.appendValue(delta)
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
	dimensions := make([]*cloudwatch.Dimension, len(c.labelNames))
	for i, k := range c.labelNames {
		if len(labelValues) <= i {
			break
		}
		dimensions[i] = &cloudwatch.Dimension{
			Name:  &k,
			Value: &labelValues[i],
		}
	}
	return &cloudWatchStat{
		root:       c.root,
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
	client *cloudwatch.CloudWatch

	// Values in range of -2^360 to 2^360
	// No more than 10 dimensions (tags)
	// No more than 250 values
	datumses  map[string]*cloudwatch.MetricDatum
	datumLock *sync.Mutex

	ctx    context.Context
	cancel func()

	config CloudWatchConfig
	log    log.Modular
}

// NewCloudWatch creates and returns a new CloudWatch object.
func NewCloudWatch(config Config, opts ...func(Type)) (Type, error) {
	c := &CloudWatch{
		config:    config.CloudWatch,
		datumses:  map[string]*cloudwatch.MetricDatum{},
		datumLock: &sync.Mutex{},
		log:       log.Noop(),
	}

	c.ctx, c.cancel = context.WithCancel(context.Background())
	for _, opt := range opts {
		opt(c)
	}

	sess, err := config.CloudWatch.GetSession()
	if err != nil {
		return nil, err
	}

	c.client = cloudwatch.New(sess)
	go c.loop()
	return c, nil
}

//------------------------------------------------------------------------------

func toCMName(dotSepName string) string {
	return dotSepName
}

// GetCounter returns a stat counter object for a path.
func (c *CloudWatch) GetCounter(path string) StatCounter {
	return &cloudWatchStat{
		root: c,
		name: toCMName(path),
		unit: cloudwatch.StandardUnitCount,
	}
}

// GetCounterVec returns a stat counter object for a path with the labels
func (c *CloudWatch) GetCounterVec(path string, n []string) StatCounterVec {
	return &cloudWatchCounterVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       toCMName(path),
			unit:       cloudwatch.StandardUnitCount,
			labelNames: n,
		},
	}
}

// GetTimer returns a stat timer object for a path.
func (c *CloudWatch) GetTimer(path string) StatTimer {
	return &cloudWatchStat{
		root: c,
		name: toCMName(path),
		unit: cloudwatch.StandardUnitMicroseconds,
	}
}

// GetTimerVec returns a stat timer object for a path with the labels
func (c *CloudWatch) GetTimerVec(path string, n []string) StatTimerVec {
	return &cloudWatchTimerVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       toCMName(path),
			unit:       cloudwatch.StandardUnitMicroseconds,
			labelNames: n,
		},
	}
}

// GetGauge returns a stat gauge object for a path.
func (c *CloudWatch) GetGauge(path string) StatGauge {
	return &cloudWatchStat{
		root: c,
		name: toCMName(path),
		unit: cloudwatch.StandardUnitNone,
	}
}

// GetGaugeVec returns a stat timer object for a path with the labels
func (c *CloudWatch) GetGaugeVec(path string, n []string) StatGaugeVec {
	return &cloudWatchGaugeVec{
		cloudWatchStatVec: cloudWatchStatVec{
			root:       c,
			name:       toCMName(path),
			unit:       cloudwatch.StandardUnitNone,
			labelNames: n,
		},
	}
}

//------------------------------------------------------------------------------

func (c *CloudWatch) loop() {
	ticker := time.NewTicker(time.Millisecond * 100) // TODO
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

// No more than 20 metrics each request
// 40KB of data
/*
	* ErrCodeInvalidParameterValueException "InvalidParameterValue"
	The value of an input parameter is bad or out-of-range.

	* ErrCodeMissingRequiredParameterException "MissingParameter"
	An input parameter that is required is missing.

	* ErrCodeInvalidParameterCombinationException "InvalidParameterCombination"
	Parameters were used together that cannot be used together.

	* ErrCodeInternalServiceFault "InternalServiceError"
	Request processing has failed due to some unknown error, exception, or failure.
*/

const maxCloudWatchMetrics = 20

func (c *CloudWatch) flush() error {
	c.datumLock.Lock()
	datumMap := c.datumses
	c.datumses = map[string]*cloudwatch.MetricDatum{}
	c.datumLock.Unlock()

	datums := []*cloudwatch.MetricDatum{}
	tNow := time.Now()

	for _, v := range datumMap {
		if v != nil {
			v.Timestamp = &tNow
			datums = append(datums, v)
		}
	}

	input := cloudwatch.PutMetricDataInput{
		Namespace:  &c.config.Namespace,
		MetricData: datums,
	}

	for len(input.MetricData) > 0 {
		if len(datums) > maxCloudWatchMetrics {
			input.MetricData, datums = datums[:maxCloudWatchMetrics], datums[maxCloudWatchMetrics:]
		} else {
			datums = nil
		}

		if _, err := c.client.PutMetricData(&input); err != nil {
			c.log.Errorf("Failed to send metric data: %v\n", err)
			select {
			case <-time.After(time.Second):
			case <-c.ctx.Done():
				return types.ErrTimeout
			}
		}

		input.MetricData = datums
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
	return nil
}

//------------------------------------------------------------------------------
