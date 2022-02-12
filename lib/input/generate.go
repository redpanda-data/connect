package input

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/input"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/robfig/cron/v3"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGenerate] = TypeSpec{
		constructor: fromSimpleConstructor(func(conf Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
			b, err := newBloblang(mgr, conf.Generate)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(TypeGenerate, false, reader.NewAsyncPreserver(b), log, stats)
		}),
		Version: "3.40.0",
		Status:  docs.StatusStable,
		Summary: `
Generates messages at a given interval using a [Bloblang](/docs/guides/bloblang/about)
mapping executed without a context. This allows you to generate messages for
testing your pipeline configs.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldBloblang(
				"mapping", "A [bloblang](/docs/guides/bloblang/about) mapping to use for generating messages.",
				`root = "hello world"`,
				`root = {"test":"message","id":uuid_v4()}`,
			),
			docs.FieldCommon(
				"interval",
				"The time interval at which messages should be generated, expressed either as a duration string or as a cron expression. If set to an empty string messages will be generated as fast as downstream services can process them. Cron expressions can specify a timezone by prefixing the expression with `TZ=<location name>`, where the location name corresponds to a file within the IANA Time Zone database.",
				"5s", "1m", "1h",
				"@every 1s", "0,30 */2 * * * *", "TZ=Europe/London 30 3-6,20-23 * * *",
			),
			docs.FieldCommon("count", "An optional number of messages to generate, if set above 0 the specified number of messages is generated and then the input will shut down."),
		},
		Categories: []Category{
			CategoryUtility,
		},
		Examples: []docs.AnnotatedExample{
			{
				Title:   "Cron Scheduled Processing",
				Summary: "A common use case for the generate input is to trigger processors on a schedule so that the processors themselves can behave similarly to an input. The following configuration reads rows from a PostgreSQL table every 5 minutes.",
				Config: `
input:
  generate:
    interval: '@every 5m'
    mapping: 'root = {}'
  processors:
    - sql_select:
        driver: postgres
        dsn: postgres://foouser:foopass@localhost:5432/testdb?sslmode=disable
        table: foo
        columns: [ "*" ]
`,
			},
			{
				Title:   "Generate 100 Rows",
				Summary: "The generate input can be used as a convenient way to generate test data. The following example generates 100 rows of structured data by setting an explicit count. The interval field is set to empty, which means data is generated as fast as the downstream components can consume it.",
				Config: `
input:
  generate:
    count: 100
    interval: ""
    mapping: |
      root = if random_int() % 2 == 0 {
        {
          "type": "foo",
          "foo": "is yummy"
        }
      } else {
        {
          "type": "bar",
          "bar": "is gross"
        }
      }
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// BloblangConfig contains configuration for the Bloblang input type.
type BloblangConfig struct {
	Mapping string `json:"mapping" yaml:"mapping"`
	// internal can be both duration string or cron expression
	Interval string `json:"interval" yaml:"interval"`
	Count    int    `json:"count" yaml:"count"`
}

// NewBloblangConfig creates a new BloblangConfig with default values.
func NewBloblangConfig() BloblangConfig {
	return BloblangConfig{
		Mapping:  "",
		Interval: "1s",
		Count:    0,
	}
}

// Bloblang executes a bloblang mapping with an empty context each time this
// input is read from. An interval period must be specified that determines how
// often a message is generated.
type Bloblang struct {
	remaining   int64
	limited     bool
	firstIsFree bool
	exec        *mapping.Executor
	timer       *time.Ticker
	schedule    *cron.Schedule
	location    *time.Location
}

// newBloblang creates a new bloblang input reader type.
func newBloblang(mgr interop.Manager, conf BloblangConfig) (*Bloblang, error) {
	var (
		duration    time.Duration
		timer       *time.Ticker
		schedule    *cron.Schedule
		location    *time.Location
		err         error
		firstIsFree = true
	)

	if len(conf.Interval) > 0 {
		if duration, err = time.ParseDuration(conf.Interval); err != nil {
			// interval is not a duration so try to parse as a cron expression
			var cerr error
			if schedule, location, cerr = parseCronExpression(conf.Interval); cerr != nil {
				return nil, fmt.Errorf("failed to parse interval as duration string: %v, or as cron expression: %w", err, cerr)
			}
			firstIsFree = false
			duration = getDurationTillNextSchedule(*schedule, location)
		}
		if duration > 0 {
			timer = time.NewTicker(duration)
		}
	}
	exec, err := mgr.BloblEnvironment().NewMapping(conf.Mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("failed to parse mapping: %v", perr.ErrorAtPosition([]rune(conf.Mapping)))
		}
		return nil, fmt.Errorf("failed to parse mapping: %v", err)
	}
	remaining := int64(conf.Count)
	return &Bloblang{
		exec:        exec,
		remaining:   remaining,
		limited:     remaining > 0,
		timer:       timer,
		schedule:    schedule,
		location:    location,
		firstIsFree: firstIsFree,
	}, nil
}

func getDurationTillNextSchedule(schedule cron.Schedule, location *time.Location) time.Duration {
	now := time.Now().In(location)
	return schedule.Next(now).Sub(now)
}

func parseCronExpression(cronExpression string) (*cron.Schedule, *time.Location, error) {
	// If time zone is not included, set default to UTC
	if !strings.HasPrefix(cronExpression, "TZ=") {
		cronExpression = fmt.Sprintf("TZ=%s %s", "UTC", cronExpression)
	}

	end := strings.Index(cronExpression, " ")
	eq := strings.Index(cronExpression, "=")
	tz := cronExpression[eq+1 : end]

	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, nil, err
	}
	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	cronSchedule, err := parser.Parse(cronExpression)
	if err != nil {
		return nil, nil, err
	}

	return &cronSchedule, loc, nil
}

// ConnectWithContext establishes a Bloblang reader.
func (b *Bloblang) ConnectWithContext(ctx context.Context) error {
	return nil
}

// ReadWithContext a new bloblang generated message.
func (b *Bloblang) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	if b.limited {
		if remaining := atomic.AddInt64(&b.remaining, -1); remaining < 0 {
			return nil, nil, component.ErrTypeClosed
		}
	}

	if !b.firstIsFree && b.timer != nil {
		select {
		case _, open := <-b.timer.C:
			if !open {
				return nil, nil, component.ErrTypeClosed
			}
			if b.schedule != nil {
				b.timer.Reset(getDurationTillNextSchedule(*b.schedule, b.location))
			}
		case <-ctx.Done():
			return nil, nil, component.ErrTimeout
		}
	}

	b.firstIsFree = false
	p, err := b.exec.MapPart(0, message.QuickBatch(nil))
	if err != nil {
		return nil, nil, err
	}
	if p == nil {
		return nil, nil, component.ErrTimeout
	}

	msg := message.QuickBatch(nil)
	msg.Append(p)

	return msg, func(context.Context, response.Error) error { return nil }, nil
}

// CloseAsync shuts down the bloblang reader.
func (b *Bloblang) CloseAsync() {
	if b.timer != nil {
		b.timer.Stop()
	}
}

// WaitForClose blocks until the bloblang input has closed down.
func (b *Bloblang) WaitForClose(timeout time.Duration) error {
	return nil
}
