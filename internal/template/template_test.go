package template_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/template"

	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestCacheTemplate(t *testing.T) {
	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	require.NoError(t, template.RegisterTemplateYAML(mgr.Environment(), []byte(`
name: foo_memory
type: cache

fields:
  - name: foovalue
    type: string

mapping: |
  root.memory.init_values.foo = this.foovalue
`)))

	conf, err := cache.FromAny(mgr, map[string]any{
		"foo_memory": map[string]any{
			"foovalue": "meow",
		},
	})
	require.NoError(t, err)

	c, err := mgr.NewCache(conf)
	require.NoError(t, err)

	res, err := c.Get(context.Background(), "foo")
	require.NoError(t, err)

	assert.Equal(t, "meow", string(res))
}

func TestInputTemplate(t *testing.T) {
	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	require.NoError(t, template.RegisterTemplateYAML(mgr.Environment(), []byte(`
name: generate_a_foo
type: input

fields:
  - name: name
    type: string

mapping: |
  root.generate.count = 1
  root.generate.interval = "1ms"
  root.generate.mapping = """root.foo = "%v" """.format(this.name)
  root.processors = [
    {
      "mutation": """root.bar = "and this too" """,
    },
  ]
`)))

	conf, err := input.FromAny(mgr, map[string]any{
		"generate_a_foo": map[string]any{
			"name": "meow",
		},
		"processors": []any{
			map[string]any{
				"mutation": "root.bar = this.bar.uppercase()",
			},
		},
	})
	require.NoError(t, err)

	strm, err := mgr.NewInput(conf)
	require.NoError(t, err)

	var tran message.Transaction
	var open bool
	select {
	case tran, open = <-strm.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	require.True(t, open)

	require.Len(t, tran.Payload, 1)
	assert.Equal(t, `{"bar":"AND THIS TOO","foo":"meow"}`, string(tran.Payload[0].AsBytes()))

	require.NoError(t, tran.Ack(context.Background(), nil))

	select {
	case _, open = <-strm.TransactionChan():
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	require.False(t, open)
}

func TestOutputTemplate(t *testing.T) {
	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	require.NoError(t, template.RegisterTemplateYAML(mgr.Environment(), []byte(`
name: write_inproc
type: output

fields:
  - name: name
    type: string

mapping: |
  root.inproc = this.name
  root.processors = [
    {
      "mapping": "root = content().uppercase()",
    },
  ]
`)))

	conf, err := output.FromAny(mgr, map[string]any{
		"write_inproc": map[string]any{
			"name": "foos",
		},
		"processors": []any{
			map[string]any{
				"mapping": `root = content() + " woof"`,
			},
		},
	})
	require.NoError(t, err)

	strm, err := mgr.NewOutput(conf)
	require.NoError(t, err)

	tInChan := make(chan message.Transaction)
	require.NoError(t, strm.Consume(tInChan))

	tOutChan, err := mgr.GetPipe("foos")
	require.NoError(t, err)

	select {
	case tInChan <- message.NewTransactionFunc(message.Batch{
		message.NewPart([]byte("meow")),
	}, func(ctx context.Context, err error) error {
		return nil
	}):
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}

	var tran message.Transaction
	var open bool
	select {
	case tran, open = <-tOutChan:
	case <-time.After(time.Second):
		t.Fatal("timed out")
	}
	require.True(t, open)

	require.Len(t, tran.Payload, 1)
	assert.Equal(t, `MEOW WOOF`, string(tran.Payload[0].AsBytes()))

	require.NoError(t, tran.Ack(context.Background(), nil))

	close(tInChan)
	strm.TriggerCloseNow()

	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	require.NoError(t, strm.WaitForClose(ctx))
}

func TestProcessorTemplate(t *testing.T) {
	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	require.NoError(t, template.RegisterTemplateYAML(mgr.Environment(), []byte(`
name: append_foo
type: processor

fields:
  - name: foo
    type: string

mapping: |
  root.mapping = """root = content() + "%v" """.format(this.foo)
`)))

	conf, err := processor.FromAny(mgr, map[string]any{
		"append_foo": map[string]any{
			"foo": " meow",
		},
	})
	require.NoError(t, err)

	p, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	res, err := p.ProcessBatch(context.Background(), message.Batch{
		message.NewPart([]byte("woof")),
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], 1)
	assert.Equal(t, `woof meow`, string(res[0][0].AsBytes()))
}

func TestProcessorTemplateOddIndentation(t *testing.T) {
	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	require.NoError(t, template.RegisterTemplateYAML(mgr.Environment(), []byte(`
name: meow
type: processor

mapping: |
  map switch_if {
    root.check = "this.go == true"

    root.processors = [
      {
        "mutation": """
  root.id = this.id.uppercase()
  """
      },
    ]
  }
  root.switch = [
    this.apply("switch_if")
  ]
`)))

	conf, err := processor.FromAny(mgr, map[string]any{
		"meow": map[string]any{},
	})
	require.NoError(t, err)

	p, err := mgr.NewProcessor(conf)
	require.NoError(t, err)

	res, err := p.ProcessBatch(context.Background(), message.Batch{
		message.NewPart([]byte(`{"go":true,"id":"aaa"}`)),
	})
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Len(t, res[0], 1)
	assert.Equal(t, `{"go":true,"id":"AAA"}`, string(res[0][0].AsBytes()))
}

func TestRateLimitTemplate(t *testing.T) {
	mgr, err := manager.New(manager.NewResourceConfig())
	require.NoError(t, err)

	require.NoError(t, template.RegisterTemplateYAML(mgr.Environment(), []byte(`
name: foo
type: rate_limit

fields:
  - name: i
    type: string

mapping: |
  root.local.count = 1
  root.local.interval = this.i
`)))

	conf, err := ratelimit.FromAny(mgr, map[string]any{
		"foo": map[string]any{
			"i": "1h",
		},
	})
	require.NoError(t, err)

	r, err := mgr.NewRateLimit(conf)
	require.NoError(t, err)

	d, err := r.Access(context.Background())
	require.NoError(t, err)
	assert.Equal(t, d, time.Duration(0))

	d, err = r.Access(context.Background())
	require.NoError(t, err)
	assert.Greater(t, d, time.Hour-time.Minute)
	assert.Less(t, d, time.Hour+time.Minute)
}
