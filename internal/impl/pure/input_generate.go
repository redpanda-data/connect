package pure

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	giFieldMapping   = "mapping"
	giFieldInterval  = "interval"
	giFieldCount     = "count"
	giFieldBatchSize = "batch_size"
)

func genInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Version("3.40.0").
		Summary("Generates messages at a given interval using a [Bloblang](/docs/guides/bloblang/about) mapping executed without a context. This allows you to generate messages for testing your pipeline configs.").
		Fields(
			service.NewBloblangField(giFieldMapping).
				Description("A [bloblang](/docs/guides/bloblang/about) mapping to use for generating messages.").
				Examples(
					`root = "hello world"`,
					`root = {"test":"message","id":uuid_v4()}`,
				),
			service.NewStringField(giFieldInterval).
				Description("The time interval at which messages should be generated, expressed either as a duration string or as a cron expression. If set to an empty string messages will be generated as fast as downstream services can process them. Cron expressions can specify a timezone by prefixing the expression with `TZ=<location name>`, where the location name corresponds to a file within the IANA Time Zone database.").
				Examples(
					"5s", "1m", "1h",
					"@every 1s", "0,30 */2 * * * *", "TZ=Europe/London 30 3-6,20-23 * * *",
				).Default("1s"),
			service.NewIntField(giFieldCount).
				Description("An optional number of messages to generate, if set above 0 the specified number of messages is generated and then the input will shut down.").
				Default(0),
			service.NewIntField(giFieldBatchSize).
				Description("The number of generated messages that should be accumulated into each batch flushed at the specified interval.").
				Default(1),
			service.NewAutoRetryNacksToggleField(),
		).
		Example("Cron Scheduled Processing", "A common use case for the generate input is to trigger processors on a schedule so that the processors themselves can behave similarly to an input. The following configuration reads rows from a PostgreSQL table every 5 minutes.", `
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
`).
		Example("Generate 100 Rows", "The generate input can be used as a convenient way to generate test data. The following example generates 100 rows of structured data by setting an explicit count. The interval field is set to empty, which means data is generated as fast as the downstream components can consume it.", `
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
`)
}

func init() {
	err := service.RegisterBatchInput("generate", genInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		nm := interop.UnwrapManagement(mgr)

		var b input.Async
		var err error
		if b, err = newGenerateReaderFromParsed(conf, nm); err != nil {
			return nil, err
		}

		if autoRetry, _ := conf.FieldBool(service.AutoRetryNacksToggleFieldName); autoRetry {
			b = input.NewAsyncPreserver(b)
		}

		i, err := input.NewAsyncReader("generate", input.NewAsyncPreserver(b), nm)
		if err != nil {
			return nil, err
		}
		return interop.NewUnwrapInternalInput(i), nil
	})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type generateReader struct {
	remaining    int
	batchSize    int
	limited      bool
	firstIsFree  bool
	exec         *mapping.Executor
	timer        *time.Ticker
	schedule     *cron.Schedule
	schedulePrev *time.Time
}

func newGenerateReaderFromParsed(conf *service.ParsedConfig, mgr bundle.NewManagement) (*generateReader, error) {
	var (
		duration     time.Duration
		timer        *time.Ticker
		schedule     *cron.Schedule
		schedulePrev *time.Time
		err          error
		firstIsFree  = true
	)

	mappingStr, err := conf.FieldString(giFieldMapping)
	if err != nil {
		return nil, err
	}

	intervalStr, err := conf.FieldString(giFieldInterval)
	if err != nil {
		return nil, err
	}

	if intervalStr != "" {
		if duration, err = time.ParseDuration(intervalStr); err != nil {
			// interval is not a duration so try to parse as a cron expression
			var cerr error
			if schedule, cerr = parseCronExpression(intervalStr); cerr != nil {
				return nil, fmt.Errorf("failed to parse interval as duration string: %v, or as cron expression: %w", err, cerr)
			}
			firstIsFree = false

			tNext := (*schedule).Next(time.Now())
			if duration = time.Until(tNext); duration < 1 {
				duration = 1
			}
			schedulePrev = &tNext
		}
		if duration > 0 {
			timer = time.NewTicker(duration)
		}
	}
	exec, err := mgr.BloblEnvironment().NewMapping(mappingStr)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("failed to parse mapping: %v", perr.ErrorAtPosition([]rune(mappingStr)))
		}
		return nil, fmt.Errorf("failed to parse mapping: %v", err)
	}

	count, err := conf.FieldInt(giFieldCount)
	if err != nil {
		return nil, err
	}

	batchSize, err := conf.FieldInt(giFieldBatchSize)
	if err != nil {
		return nil, err
	}

	return &generateReader{
		exec:         exec,
		remaining:    count,
		batchSize:    batchSize,
		limited:      count > 0,
		timer:        timer,
		schedule:     schedule,
		schedulePrev: schedulePrev,
		firstIsFree:  firstIsFree,
	}, nil
}

func parseCronExpression(cronExpression string) (*cron.Schedule, error) {
	// If time zone is not included, set default to UTC
	if !strings.HasPrefix(cronExpression, "TZ=") {
		cronExpression = fmt.Sprintf("TZ=%s %s", "UTC", cronExpression)
	}

	parser := cron.NewParser(cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	cronSchedule, err := parser.Parse(cronExpression)
	if err != nil {
		return nil, err
	}

	return &cronSchedule, nil
}

// Connect establishes a Bloblang reader.
func (b *generateReader) Connect(ctx context.Context) error {
	return nil
}

// ReadBatch a new bloblang generated message.
func (b *generateReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	batchSize := b.batchSize
	if b.limited {
		if b.remaining <= 0 {
			return nil, nil, component.ErrTypeClosed
		}
		if b.remaining < batchSize {
			batchSize = b.remaining
		}
	}

	if !b.firstIsFree && b.timer != nil {
		select {
		case t, open := <-b.timer.C:
			if !open {
				return nil, nil, component.ErrTypeClosed
			}
			if b.schedule != nil {
				if b.schedulePrev != nil {
					t = *b.schedulePrev
				}

				tNext := (*b.schedule).Next(t)
				tNow := time.Now()
				duration := tNext.Sub(tNow)
				if duration < 1 {
					duration = 1
				}

				b.schedulePrev = &tNext
				b.timer.Reset(duration)
			}
		case <-ctx.Done():
			return nil, nil, component.ErrTimeout
		}
	}
	b.firstIsFree = false

	batch := make(message.Batch, 0, batchSize)
	for i := 0; i < batchSize; i++ {
		p, err := b.exec.MapPart(0, batch)
		if err != nil {
			return nil, nil, err
		}
		if p != nil {
			if b.limited {
				b.remaining--
			}
			batch = append(batch, p)
		}
	}
	if len(batch) == 0 {
		return nil, nil, component.ErrTimeout
	}
	return batch, func(context.Context, error) error { return nil }, nil
}

// CloseAsync shuts down the bloblang reader.
func (b *generateReader) Close(ctx context.Context) (err error) {
	if b.timer != nil {
		b.timer.Stop()
	}
	return
}
