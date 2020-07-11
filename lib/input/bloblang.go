package input

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/bloblang/x/mapping"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/x/docs"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeBloblang] = TypeSpec{
		constructor: func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
			b, err := newBloblang(conf.Bloblang)
			if err != nil {
				return nil, err
			}
			return NewAsyncReader(TypeBloblang, true, b, log, stats)
		},
		Summary: `
BETA: This input is currently in a BETA stage and is therefore subject to
breaking configuration changes outside of major version releases.

Generates messages at a given interval using a [Bloblang](/docs/guides/bloblang/about)
mapping executed without a context. This allows you to generate messages for
testing your pipeline configs.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon(
				"mapping", "A [bloblang](/docs/guides/bloblang/about) mapping to use for generating messages.",
				`root = "hello world"`,
				`root = {"test":"message","id":uuid_v4()}`,
			),
			docs.FieldCommon("interval", "The time interval at which messages should be generated."),
			docs.FieldCommon("count", "An optional number of messages to generate, if set above 0 the specified number of messages is generated and then the input will shut down."),
		},
		Footnotes: `
## Examples

You can use Bloblang to generate payloads of differing structure at random:

` + "```yaml" + `
input:
  bloblang:
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
` + "```" + ``,
	}
}

//------------------------------------------------------------------------------

// BloblangConfig contains configuration for the Bloblang input type.
type BloblangConfig struct {
	Mapping  string `json:"mapping" yaml:"mapping"`
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
	remaining int32

	timerDuration time.Duration
	exec          *mapping.Executor

	timerMut sync.Mutex
	timer    *time.Ticker
}

// newBloblang creates a new bloblang input reader type.
func newBloblang(conf BloblangConfig) (*Bloblang, error) {
	duration, err := time.ParseDuration(conf.Interval)
	if err != nil {
		return nil, fmt.Errorf("failed to parse interval: %w", err)
	}
	exec, err := mapping.NewExecutor(conf.Mapping)
	if err != nil {
		return nil, fmt.Errorf("failed to parse bloblang mapping: %w", err)
	}
	remaining := int32(conf.Count)
	if remaining <= 0 {
		remaining = -1
	}
	return &Bloblang{
		timerDuration: duration,
		exec:          exec,
		remaining:     remaining,
	}, nil
}

// ConnectWithContext establishes a Bloblang reader.
func (b *Bloblang) ConnectWithContext(ctx context.Context) error {
	b.timerMut.Lock()
	defer b.timerMut.Unlock()

	if b.timer != nil {
		return nil
	}

	b.timer = time.NewTicker(b.timerDuration)
	return nil
}

// ReadWithContext a new bloblang generated message.
func (b *Bloblang) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	b.timerMut.Lock()
	timer := b.timer
	b.timerMut.Unlock()

	if timer == nil {
		return nil, nil, types.ErrNotConnected
	}

	if atomic.LoadInt32(&b.remaining) >= 0 {
		if atomic.AddInt32(&b.remaining, -1) < 0 {
			return nil, nil, types.ErrTypeClosed
		}
	}

	select {
	case _, open := <-timer.C:
		if !open {
			return nil, nil, types.ErrNotConnected
		}
	case <-ctx.Done():
		return nil, nil, types.ErrTimeout
	}

	p, err := b.exec.MapPart(0, message.New(nil))
	if err != nil {
		return nil, nil, err
	}
	if p == nil {
		return nil, nil, types.ErrTimeout
	}

	msg := message.New(nil)
	msg.Append(p)

	return msg, func(context.Context, types.Response) error { return nil }, nil
}

// CloseAsync shuts down the bloblang reader.
func (b *Bloblang) CloseAsync() {
	b.timerMut.Lock()
	b.timer.Stop()
	b.timer = nil
	b.timerMut.Unlock()
}

// WaitForClose blocks until the bloblang input has closed down.
func (b *Bloblang) WaitForClose(timeout time.Duration) error {
	return nil
}
