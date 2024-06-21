package service_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/service"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

func TestStreamBuilderDefault(t *testing.T) {
	b := service.NewStreamBuilder()

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`http:
    enabled: false`,
		`input:
    label: ""
    stdin:`,
		`buffer:
    none: {}`,
		`pipeline:
    threads: 0
    processors: []`,
		`output:
    label: ""
    stdout:`,
		`logger:
    level: INFO`,
		`metrics:
    none:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderProducerFunc(t *testing.T) {
	tmpDir := t.TempDir()

	outFilePath := filepath.Join(tmpDir, "out.txt")

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddProcessorYAML(`bloblang: 'root = content().uppercase()'`))
	require.NoError(t, b.AddOutputYAML(fmt.Sprintf(`
file:
  codec: lines
  path: %v`, outFilePath)))

	pushFn, err := b.AddProducerFunc()
	require.NoError(t, err)

	// Fails on second call.
	_, err = b.AddProducerFunc()
	require.Error(t, err)

	// Don't allow input overrides now.
	err = b.SetYAML(`input: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		defer done()

		require.NoError(t, pushFn(ctx, service.NewMessage([]byte("hello world 1"))))
		require.NoError(t, pushFn(ctx, service.NewMessage([]byte("hello world 2"))))
		require.NoError(t, pushFn(ctx, service.NewMessage([]byte("hello world 3"))))

		require.NoError(t, strm.StopWithin(time.Second*5))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	outBytes, err := os.ReadFile(outFilePath)
	require.NoError(t, err)

	assert.Equal(t, "HELLO WORLD 1\nHELLO WORLD 2\nHELLO WORLD 3\n", string(outBytes))
}

func TestStreamBuilderBatchProducerFunc(t *testing.T) {
	tmpDir := t.TempDir()

	outFilePath := filepath.Join(tmpDir, "out.txt")

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddProcessorYAML(`bloblang: 'root = content().uppercase()'`))
	require.NoError(t, b.AddOutputYAML(fmt.Sprintf(`
file:
  codec: lines
  path: %v`, outFilePath)))

	pushFn, err := b.AddBatchProducerFunc()
	require.NoError(t, err)

	// Fails on second call.
	_, err = b.AddProducerFunc()
	require.Error(t, err)

	// Don't allow input overrides now.
	err = b.SetYAML(`input: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		defer done()

		require.NoError(t, pushFn(ctx, service.MessageBatch{
			service.NewMessage([]byte("hello world 1")),
			service.NewMessage([]byte("hello world 2")),
		}))
		require.NoError(t, pushFn(ctx, service.MessageBatch{
			service.NewMessage([]byte("hello world 3")),
			service.NewMessage([]byte("hello world 4")),
		}))
		require.NoError(t, pushFn(ctx, service.MessageBatch{
			service.NewMessage([]byte("hello world 5")),
			service.NewMessage([]byte("hello world 6")),
		}))

		require.NoError(t, strm.StopWithin(time.Second*5))
	}()

	require.NoError(t, strm.Run(context.Background()))
	wg.Wait()

	outBytes, err := os.ReadFile(outFilePath)
	require.NoError(t, err)

	assert.Equal(t, "HELLO WORLD 1\nHELLO WORLD 2\nHELLO WORLD 3\nHELLO WORLD 4\nHELLO WORLD 5\nHELLO WORLD 6\n", string(outBytes))
}

func TestStreamBuilderEnvVarInterpolation(t *testing.T) {
	t.Setenv("BENTHOS_TEST_ONE", "foo")
	t.Setenv("BENTHOS_TEST_TWO", "warn")

	b := service.NewStreamBuilder()
	require.NoError(t, b.AddInputYAML(`
generate:
  mapping: 'root = "${BENTHOS_TEST_ONE}"'
`))

	require.NoError(t, b.SetLoggerYAML(`level: ${BENTHOS_TEST_TWO}`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		` mapping: 'root = "foo"'`,
		`level: warn`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}

	b = service.NewStreamBuilder()
	require.NoError(t, b.SetYAML(`
input:
  generate:
    mapping: 'root = "${BENTHOS_TEST_ONE}"'
logger:
  level: ${BENTHOS_TEST_TWO}
`))

	act, err = b.AsYAML()
	require.NoError(t, err)

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderConsumerFunc(t *testing.T) {
	tmpDir := t.TempDir()

	inFilePath := filepath.Join(tmpDir, "in.txt")
	require.NoError(t, os.WriteFile(inFilePath, []byte(`HELLO WORLD 1
HELLO WORLD 2
HELLO WORLD 3`), 0o755))

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddInputYAML(fmt.Sprintf(`
file:
  codec: lines
  paths: [ %v ]`, inFilePath)))
	require.NoError(t, b.AddProcessorYAML(`bloblang: 'root = content().lowercase()'`))

	outMsgs := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, m *service.Message) error {
		outMut.Lock()
		defer outMut.Unlock()

		b, err := m.AsBytes()
		assert.NoError(t, err)

		outMsgs[string(b)] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddConsumerFunc(handler))

	// Fails on second call.
	require.Error(t, b.AddConsumerFunc(handler))

	// Don't allow output overrides now.
	err := b.SetYAML(`output: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		"hello world 1": {},
		"hello world 2": {},
		"hello world 3": {},
	}, outMsgs)
	outMut.Unlock()
}

func TestStreamBuilderConsumerFuncInlineProcs(t *testing.T) {
	tmpDir := t.TempDir()

	inFilePath := filepath.Join(tmpDir, "in.txt")
	require.NoError(t, os.WriteFile(inFilePath, []byte(`HELLO WORLD 1
HELLO WORLD 2
HELLO WORLD 3`), 0o755))

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddInputYAML(fmt.Sprintf(`
file:
  codec: lines
  paths: [ %v ]
processors:
  - bloblang: 'root = content().lowercase()'
`, inFilePath)))

	outMsgs := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, m *service.Message) error {
		outMut.Lock()
		defer outMut.Unlock()

		b, err := m.AsBytes()
		assert.NoError(t, err)

		outMsgs[string(b)] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddConsumerFunc(handler))

	// Fails on second call.
	require.Error(t, b.AddConsumerFunc(handler))

	// Don't allow output overrides now.
	err := b.SetYAML(`output: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		"hello world 1": {},
		"hello world 2": {},
		"hello world 3": {},
	}, outMsgs)
	outMut.Unlock()
}

func TestStreamBuilderBatchConsumerFunc(t *testing.T) {
	tmpDir := t.TempDir()

	inFilePath := filepath.Join(tmpDir, "in.txt")
	require.NoError(t, os.WriteFile(inFilePath, []byte(`HELLO WORLD 1
HELLO WORLD 2

HELLO WORLD 3
HELLO WORLD 4

HELLO WORLD 5
HELLO WORLD 6
`), 0o755))

	b := service.NewStreamBuilder()
	require.NoError(t, b.SetLoggerYAML("level: NONE"))
	require.NoError(t, b.AddInputYAML(fmt.Sprintf(`
file:
  codec: lines/multipart
  paths: [ %v ]`, inFilePath)))
	require.NoError(t, b.AddProcessorYAML(`bloblang: 'root = content().lowercase()'`))

	outBatches := map[string]struct{}{}
	var outMut sync.Mutex
	handler := func(_ context.Context, mb service.MessageBatch) error {
		outMut.Lock()
		defer outMut.Unlock()

		outMsgs := []string{}
		for _, m := range mb {
			b, err := m.AsBytes()
			assert.NoError(t, err)
			outMsgs = append(outMsgs, string(b))
		}

		outBatches[strings.Join(outMsgs, ",")] = struct{}{}
		return nil
	}
	require.NoError(t, b.AddBatchConsumerFunc(handler))

	// Fails on second call.
	require.Error(t, b.AddBatchConsumerFunc(handler))

	// Don't allow output overrides now.
	err := b.SetYAML(`output: {}`)
	require.Error(t, err)

	strm, err := b.Build()
	require.NoError(t, err)

	require.NoError(t, strm.Run(context.Background()))

	outMut.Lock()
	assert.Equal(t, map[string]struct{}{
		"hello world 1,hello world 2": {},
		"hello world 3,hello world 4": {},
		"hello world 5,hello world 6": {},
	}, outBatches)
	outMut.Unlock()
}

func TestStreamBuilderCustomLogger(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetPrintLogger(nil)

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := `logger:
    level: INFO`

	assert.NotContains(t, act, exp)
}

func TestStreamBuilderSetYAML(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetThreads(10)
	require.NoError(t, b.AddCacheYAML(`label: foocache
type: memory`))
	require.NoError(t, b.AddInputYAML(`type: generate`))
	require.NoError(t, b.AddOutputYAML(`type: drop`))
	require.NoError(t, b.AddProcessorYAML(`type: bloblang`))
	require.NoError(t, b.AddProcessorYAML(`type: jmespath`))
	require.NoError(t, b.AddRateLimitYAML(`label: foorl
type: local`))
	require.NoError(t, b.SetMetricsYAML(`type: none`))
	require.NoError(t, b.SetLoggerYAML(`level: DEBUG`))
	require.NoError(t, b.SetBufferYAML(`type: memory`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    generate:`,
		`buffer:
    memory: {}`,
		`pipeline:
    threads: 10
    processors:`,
		`
        - label: ""
          bloblang: ""`,
		`
        - label: ""
          jmespath: {}`,
		`output:
    label: ""
    drop:`,
		`metrics:
    none:`,
		`cache_resources:
    - label: foocache
      memory:`,
		`rate_limit_resources:
    - label: foorl
      local:`,
		`  level: DEBUG`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderSetResourcesYAML(t *testing.T) {
	b := service.NewStreamBuilder()
	require.NoError(t, b.AddResourcesYAML(`
cache_resources:
  - label: foocache
    type: memory

rate_limit_resources:
  - label: foorl
    type: local

processor_resources:
  - label: fooproc1
    type: bloblang
  - label: fooproc2
    type: jmespath

input_resources:
  - label: fooinput
    generate:
      mapping: 'root = "meow"'

output_resources:
  - label: foooutput
    type: drop
`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`cache_resources:
    - label: foocache
      memory:`,
		`rate_limit_resources:
    - label: foorl
      local:`,
		`processor_resources:
    - label: fooproc1
      bloblang:`,
		`    - label: fooproc2
      jmespath:`,
		`input_resources:
    - label: fooinput
      generate:`,
		`output_resources:
    - label: foooutput
      drop:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderSetYAMLBrokers(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetThreads(10)
	require.NoError(t, b.AddInputYAML(`
label: foo
generate:
  mapping: root = deleted()
`))
	require.NoError(t, b.AddInputYAML(`
label: bar
generate:
  mapping: root = deleted()
`))
	require.NoError(t, b.AddOutputYAML(`
label: baz
drop: {}
`))
	require.NoError(t, b.AddOutputYAML(`
label: buz
drop: {}
`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    broker:
        inputs:`,
		`            - label: foo
              generate:`,
		`            - label: bar
              generate:`,
		`output:
    label: ""
    broker:
        outputs:`,
		`            - label: baz
              drop:`,
		`            - label: buz
              drop:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderYAMLErrors(t *testing.T) {
	b := service.NewStreamBuilder()

	err := b.AddCacheYAML(`{ label: "", type: memory }`)
	require.Error(t, err)
	assert.EqualError(t, err, "a label must be specified for cache resources")

	err = b.AddInputYAML(`not valid ! yaml 34324`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected object")

	err = b.SetYAML(`not valid ! yaml 34324`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected object value")

	err = b.SetYAML(`input: { foo: nope }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to infer")

	err = b.SetYAML(`input: { generate: { not_a_field: nope } }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field not_a_field not recognised")

	err = b.AddInputYAML(`not_a_field: nah`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unable to infer")

	err = b.AddInputYAML(`generate: { not_a_field: nah }`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field not_a_field not recognised")

	err = b.SetLoggerYAML(`not_a_field: nah`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "field not_a_field not recognised")

	err = b.AddRateLimitYAML(`{ label: "", local: {} }`)
	require.Error(t, err)
	assert.EqualError(t, err, "a label must be specified for rate limit resources")
}

func TestStreamBuilderSetFields(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		args        []any
		output      string
		errContains string
	}{
		{
			name:  "odd number of args",
			input: `{}`,
			args: []any{
				"just a field",
			},
			errContains: "odd number of pathValues",
		},
		{
			name:  "a path isnt a string",
			input: `{}`,
			args: []any{
				10, "hello world",
			},
			errContains: "should be a string",
		},
		{
			name: "unknown field error",
			input: `
input:
  generate:
    mapping: 'root = deleted()'
`,
			args: []any{
				"input.generate.unknown_field", "baz",
			},
			errContains: "field not recognised",
		},
		{
			name: "create lint error",
			input: `
input:
  generate:
    mapping: 'root = deleted()'
`,
			args: []any{
				"input.label", "foo",
				"output.label", "foo",
			},
			errContains: "collides with a previously",
		},
		{
			name: "set file paths",
			input: `
input:
  file:
    paths: [ foo, bar ]
`,
			args: []any{
				"input.file.paths.1", "baz",
			},
			output: `
input:
  file:
    paths: [ foo, baz ]
`,
		},
		{
			name: "append file paths",
			input: `
input:
  file:
    paths: [ foo, bar ]
`,
			args: []any{
				"input.file.paths.-", "baz",
				"input.file.paths.-", "buz",
				"input.file.paths.-", "bev",
			},
			output: `
input:
  file:
    paths: [ foo, bar, baz, buz, bev ]
`,
		},
		{
			name: "add a processor",
			input: `
input:
  generate:
    mapping: 'root = deleted()'
`,
			args: []any{
				"pipeline.processors.-.bloblang", `root = "meow"`,
			},
			output: `
input:
  generate:
    mapping: 'root = deleted()'
pipeline:
  processors:
    - bloblang: 'root = "meow"'
`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := service.NewStreamBuilder()
			require.NoError(t, b.SetYAML(test.input))
			err := b.SetFields(test.args...)
			if test.errContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), test.errContains)
			} else {
				require.NoError(t, err)

				b2 := service.NewStreamBuilder()
				require.NoError(t, b2.SetYAML(test.output))

				bAsYAML, err := b.AsYAML()
				require.NoError(t, err)

				b2AsYAML, err := b2.AsYAML()
				require.NoError(t, err)

				assert.YAMLEq(t, b2AsYAML, bAsYAML)
			}
		})
	}
}

func TestStreamBuilderWalk(t *testing.T) {
	type walkedComponent struct {
		typeStr string
		name    string
		label   string
		conf    string
	}
	tests := []struct {
		name   string
		input  string
		output []walkedComponent
	}{
		{
			name: "basic components",
			input: `
input:
  generate:
    mapping: 'root = deleted()'

pipeline:
  processors:
    - mutation: 'root = "hm"'
      label: fooproc

output:
  reject: lol nah
`,
			output: []walkedComponent{
				{
					typeStr: "input",
					name:    "generate",
					conf: `label: ""
generate:
    mapping: 'root = deleted()'`,
				},
				{
					typeStr: "buffer",
					name:    "none",
					conf:    `none: {}`,
				},
				{
					typeStr: "processor",
					name:    "mutation",
					label:   "fooproc",
					conf: `label: fooproc
mutation: 'root = "hm"'`,
				},
				{
					typeStr: "output",
					name:    "reject",
					conf: `label: ""
reject: lol nah`,
				},
				{
					typeStr: "metrics",
					name:    "none",
					conf: `none: {}
mapping: ""`,
				},
				{
					typeStr: "tracer",
					name:    "none",
					conf:    `none: {}`,
				},
			},
		},
		{
			name: "input and output procs",
			input: `
input:
  generate:
    mapping: 'root = deleted()'
  processors:
    - mutation: 'root = "hm"'

output:
  reject: lol nah
  processors:
    - mutation: 'root = "eh"'
`,
			output: []walkedComponent{
				{
					typeStr: "input",
					name:    "generate",
					conf: `label: ""
generate:
    mapping: 'root = deleted()'
processors:
    - label: ""
      mutation: 'root = "hm"'`,
				},
				{
					typeStr: "processor",
					name:    "mutation",
					conf: `label: ""
mutation: 'root = "hm"'`,
				},
				{
					typeStr: "buffer",
					name:    "none",
					conf:    `none: {}`,
				},
				{
					typeStr: "output",
					name:    "reject",
					conf: `label: ""
reject: lol nah
processors:
    - label: ""
      mutation: 'root = "eh"'`,
				},
				{
					typeStr: "processor",
					name:    "mutation",
					conf: `label: ""
mutation: 'root = "eh"'`,
				},
				{
					typeStr: "metrics",
					name:    "none",
					conf: `none: {}
mapping: ""`,
				},
				{
					typeStr: "tracer",
					name:    "none",
					conf:    `none: {}`,
				},
			},
		},
		{
			name: "nested components",
			input: `
input:
  dynamic:
    inputs:
      foo:
        file:
          paths: [ aaa.txt ]
`,
			output: []walkedComponent{
				{
					typeStr: "input",
					name:    "dynamic",
					conf: `label: ""
dynamic:
    inputs:
        foo:
            file:
                paths: [aaa.txt]`,
				},
				{
					typeStr: "input",
					name:    "file",
					conf: `file:
    paths: [aaa.txt]`,
				},
				{
					typeStr: "buffer",
					name:    "none",
					conf:    `none: {}`,
				},
				{
					typeStr: "output",
					name:    "stdout",
					conf: `label: ""
stdout: {}`,
				},
				{
					typeStr: "metrics",
					name:    "none",
					conf: `none: {}
mapping: ""`,
				},
				{
					typeStr: "tracer",
					name:    "none",
					conf:    `none: {}`,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			b := service.NewStreamBuilder()
			require.NoError(t, b.SetYAML(test.input))

			var results []walkedComponent
			require.NoError(t, b.WalkComponents(func(w *service.WalkedComponent) error {
				results = append(results, walkedComponent{
					typeStr: w.ComponentType,
					name:    w.Name,
					label:   w.Label,
					conf:    strings.TrimSpace(w.ConfigYAML()),
				})
				return nil
			}))

			assert.Equal(t, test.output, results)
		})
	}
}

func TestStreamBuilderSetCoreYAML(t *testing.T) {
	b := service.NewStreamBuilder()
	b.SetThreads(10)
	require.NoError(t, b.SetYAML(`
input:
  generate:
    mapping: 'root = deleted()'

pipeline:
  threads: 5
  processors:
    - type: bloblang
    - type: jmespath

output:
  drop: {}
`))

	act, err := b.AsYAML()
	require.NoError(t, err)

	exp := []string{
		`input:
    label: ""
    generate:`,
		`buffer:
    none: {}`,
		`pipeline:
    threads: 5
    processors:`,
		`
        - label: ""
          bloblang: ""`,
		`
        - label: ""
          jmespath: {}`,
		`output:
    label: ""
    drop:`,
	}

	for _, str := range exp {
		assert.Contains(t, act, str)
	}
}

func TestStreamBuilderDisabledLinting(t *testing.T) {
	lintingErrorConfig := `
input:
  generate:
    mapping: 'root = deleted()'
  meow: ignore this field

output:
  another: linting error
  drop: {}
`
	b := service.NewStreamBuilder()
	require.Error(t, b.SetYAML(lintingErrorConfig))

	b = service.NewStreamBuilder()
	b.DisableLinting()
	require.NoError(t, b.SetYAML(lintingErrorConfig))
}

type noopProc struct{}

func (n noopProc) Process(ctx context.Context, m *service.Message) (service.MessageBatch, error) {
	return service.MessageBatch{m}, nil
}

func (n noopProc) Close(context.Context) error {
	return nil
}

func TestStreamBuilderSecretsSetYAML(t *testing.T) {
	var meowValues []string

	env := service.NewEnvironment()
	require.NoError(t, env.RegisterProcessor("foo",
		service.NewConfigSpec().
			Field(service.NewStringField("meow").Secret()),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			meowValue, _ := conf.FieldString("meow")
			meowValues = append(meowValues, meowValue)
			return noopProc{}, nil
		}))

	b := env.NewStreamBuilder()
	require.NoError(t, b.SetYAML(`
input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root.id = "foo"'
  processors:
    - foo:
        meow: first
output:
  drop: {}
`))

	strm, err := b.Build()
	require.NoError(t, err)

	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	require.NoError(t, strm.Run(tCtx))

	assert.Equal(t, []string{"first"}, meowValues)
}

func TestStreamBuilderSecretsSetField(t *testing.T) {
	var meowValues []string

	env := service.NewEnvironment()
	require.NoError(t, env.RegisterProcessor("foo",
		service.NewConfigSpec().
			Field(service.NewStringField("meow").Secret()),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			meowValue, _ := conf.FieldString("meow")
			meowValues = append(meowValues, meowValue)
			return noopProc{}, nil
		}))

	b := env.NewStreamBuilder()
	require.NoError(t, b.SetYAML(`
input:
  generate:
    count: 1
    interval: 1ms
    mapping: 'root.id = "foo"'
  processors:
    - foo:
        meow: ignorethisvalue
output:
  drop: {}
`))

	require.NoError(t, b.SetFields("input.processors.0.foo.meow", "second"))

	strm, err := b.Build()
	require.NoError(t, err)

	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	require.NoError(t, strm.Run(tCtx))

	assert.Equal(t, []string{"second"}, meowValues)
}

type disabledMux struct{}

func (d disabledMux) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
}

func BenchmarkStreamRun(b *testing.B) {
	config := `
input:
  generate:
    count: 5
    interval: ""
    mapping: |
      root.id = uuid_v4()

pipeline:
  processors:
    - bloblang: 'root = this'

output:
  drop: {}

logger:
  level: OFF
`

	strmBuilder := service.NewStreamBuilder()
	strmBuilder.SetHTTPMux(disabledMux{})
	require.NoError(b, strmBuilder.SetYAML(config))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		strm, err := strmBuilder.Build()
		require.NoError(b, err)

		require.NoError(b, strm.Run(context.Background()))
	}
}

func BenchmarkStreamRunOutputN1(b *testing.B) {
	benchmarkStreamRunOutputNX(b, 1)
}

func BenchmarkStreamRunOutputN10(b *testing.B) {
	benchmarkStreamRunOutputNX(b, 10)
}

func BenchmarkStreamRunOutputN100(b *testing.B) {
	benchmarkStreamRunOutputNX(b, 100)
}

type noopOutput struct{}

func (n *noopOutput) Connect(ctx context.Context) error {
	return nil
}

func (n *noopOutput) Write(ctx context.Context, msg *service.Message) error {
	return nil
}

func (n *noopOutput) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	return nil
}

func (n *noopOutput) Close(ctx context.Context) error {
	return nil
}

func benchmarkStreamRunOutputNX(b *testing.B, size int) {
	var outputsBuf bytes.Buffer
	for i := 0; i < size; i++ {
		outputsBuf.WriteString("      - custom: {}\n")
	}

	config := fmt.Sprintf(`
input:
  generate:
    count: 5
    interval: ""
    mapping: |
      root.id = uuid_v4()

pipeline:
  processors:
    - bloblang: 'root = this'

output:
  broker:
    outputs:
%v

logger:
  level: OFF
`, outputsBuf.String())

	env := service.NewEnvironment()
	require.NoError(b, env.RegisterOutput(
		"custom",
		service.NewConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return &noopOutput{}, 1, nil
		},
	))

	strmBuilder := env.NewStreamBuilder()
	strmBuilder.SetHTTPMux(disabledMux{})
	require.NoError(b, strmBuilder.SetYAML(config))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		strm, err := strmBuilder.Build()
		require.NoError(b, err)

		require.NoError(b, strm.Run(context.Background()))
	}
}

func TestStreamBuilderLargeNestingSmoke(t *testing.T) {
	b := service.NewStreamBuilder()
	require.NoError(t, b.SetYAML(`
input:
  label: ibroker0
  broker:
    inputs:
      - label: ifoo
        generate:
          count: 1
          interval: 1ms
          mapping: 'root = "ifoo: " + counter().string()'
        processors:
          - mutation: 'root = content().string() + " pfoo0"'
          - mutation: 'root = content().string() + " pfoo1"'
      - label: ibroker1
        broker:
          inputs:
            - label: ibar
              generate:
                count: 1
                interval: 1ms
                mapping: 'root = "ibar: " + counter().string()'
              processors:
                - mutation: 'root = content().string() + " pbar0"'
            - label: ibaz
              generate:
                count: 1
                interval: 1ms
                mapping: 'root = "ibaz: " + counter().string()'
              processors:
                - mutation: 'root = content().string() + " pbaz0"'
        processors:
          - mutation: 'root = content().string() + " pibroker10"'
  processors:
    - mutation: 'root = content().string() + " pibroker00"'

pipeline:
  processors:
    - try:
      - mutation: 'root = content().string() + " pquack0"'
      - for_each:
        - mutation: 'root = content().string() + " pwoof0"'

output:
  label: obroker0
  broker:
    outputs:
      - label: ofoo
        drop: {}
        processors:
          - mutation: 'root = content().string() + " pofoo0"'
      - label: obroker1
        broker:
          outputs:
            - label: obar
              drop: {}
            - label: obaz
              drop: {}
              processors:
                - mutation: 'root = content().string() + " pobaz0"'
        processors:
          - mutation: 'root = content().string() + " pobroker10"'
  processors:
    - mutation: 'root = content().string() + " pobroker00"'
`))

	strm, tracSum, err := b.BuildTraced()
	require.NoError(t, err)

	tCtx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	require.NoError(t, strm.Run(tCtx))

	eventKeys := map[string]map[string]struct{}{}
	for k, v := range tracSum.InputEvents() {
		eMap := map[string]struct{}{}
		for _, e := range v {
			eMap[e.Content] = struct{}{}
		}
		eventKeys[k] = eMap
	}

	assert.Equal(t, map[string]map[string]struct{}{
		"ifoo": {"ifoo: 1 pfoo0 pfoo1": struct{}{}},
		"ibar": {"ibar: 1 pbar0": struct{}{}},
		"ibaz": {"ibaz: 1 pbaz0": struct{}{}},
		"ibroker0": {
			"ifoo: 1 pfoo0 pfoo1 pibroker00":      struct{}{},
			"ibar: 1 pbar0 pibroker10 pibroker00": struct{}{},
			"ibaz: 1 pbaz0 pibroker10 pibroker00": struct{}{},
		},
		"ibroker1": {
			"ibar: 1 pbar0 pibroker10": struct{}{},
			"ibaz: 1 pbaz0 pibroker10": struct{}{},
		},
	}, eventKeys)

	eventKeys = map[string]map[string]struct{}{}
	for k, v := range tracSum.OutputEvents() {
		eMap := map[string]struct{}{}
		for _, e := range v {
			eMap[e.Content] = struct{}{}
		}
		eventKeys[k] = eMap
	}

	assert.Equal(t, map[string]map[string]struct{}{
		"ofoo": {
			"ifoo: 1 pfoo0 pfoo1 pibroker00 pquack0 pwoof0 pobroker00 pofoo0":      struct{}{},
			"ibar: 1 pbar0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pofoo0": struct{}{},
			"ibaz: 1 pbaz0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pofoo0": struct{}{},
		},
		"obar": {
			"ifoo: 1 pfoo0 pfoo1 pibroker00 pquack0 pwoof0 pobroker00 pobroker10":      struct{}{},
			"ibar: 1 pbar0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pobroker10": struct{}{},
			"ibaz: 1 pbaz0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pobroker10": struct{}{},
		},
		"obaz": {
			"ifoo: 1 pfoo0 pfoo1 pibroker00 pquack0 pwoof0 pobroker00 pobroker10 pobaz0":      struct{}{},
			"ibar: 1 pbar0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pobroker10 pobaz0": struct{}{},
			"ibaz: 1 pbaz0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pobroker10 pobaz0": struct{}{},
		},
		"obroker0": {
			"ifoo: 1 pfoo0 pfoo1 pibroker00 pquack0 pwoof0 pobroker00":      struct{}{},
			"ibar: 1 pbar0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00": struct{}{},
			"ibaz: 1 pbaz0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00": struct{}{},
		},
		"obroker1": {
			"ifoo: 1 pfoo0 pfoo1 pibroker00 pquack0 pwoof0 pobroker00 pobroker10":      struct{}{},
			"ibar: 1 pbar0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pobroker10": struct{}{},
			"ibaz: 1 pbaz0 pibroker10 pibroker00 pquack0 pwoof0 pobroker00 pobroker10": struct{}{},
		},
	}, eventKeys)
}
