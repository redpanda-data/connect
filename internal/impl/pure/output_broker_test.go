package pure_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/testutil"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/public/components/io"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestFanOutBroker(t *testing.T) {
	dir := t.TempDir()

	conf, err := testutil.OutputFromYAML(strings.ReplaceAll(`
broker:
  pattern: fan_out
  outputs:
    - file:
        path: '$DIR/one/foo-${!count("1s")}.txt'
        codec: all-bytes
      processors:
        - bloblang: 'root  = "one-" + content()'
    - file:
        path: '$DIR/two/bar-${!count("2s")}.txt'
        codec: all-bytes
      processors:
        - bloblang: 'root  = "two-" + content()'
`, "$DIR", dir))
	require.NoError(t, err)

	s, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		s.TriggerCloseNow()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		assert.NoError(t, s.WaitForClose(ctx))
		done()
	}()

	inputs := []string{
		"first", "second", "third",
	}
	expFiles := map[string]string{
		"./one/foo-1.txt": "one-first",
		"./one/foo-2.txt": "one-second",
		"./one/foo-3.txt": "one-third",
		"./two/bar-1.txt": "two-first",
		"./two/bar-2.txt": "two-second",
		"./two/bar-3.txt": "two-third",
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res != nil {
				t.Fatal(res)
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}

func TestRoundRobinBroker(t *testing.T) {
	dir := t.TempDir()

	conf, err := testutil.OutputFromYAML(strings.ReplaceAll(`
broker:
  pattern: round_robin
  outputs:
    - file:
        path: '$DIR/one/foo-${!count("rrfoo")}.txt'
        codec: all-bytes
      processors:
        - bloblang: 'root  = "one-" + content()'
    - file:
        path: '$DIR/two/bar-${!count("rrbar")}.txt'
        codec: all-bytes
      processors:
        - bloblang: 'root  = "two-" + content()'
`, "$DIR", dir))
	require.NoError(t, err)

	s, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		s.TriggerCloseNow()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		assert.NoError(t, s.WaitForClose(ctx))
		done()
	})

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string]string{
		"./one/foo-1.txt": "one-first",
		"./one/foo-2.txt": "one-third",
		"./two/bar-1.txt": "two-second",
		"./two/bar-2.txt": "two-fourth",
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res != nil {
				t.Fatal(res)
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp != act {
			t.Errorf("Wrong contents for file '%v': %v != %v", k, act, exp)
		}
	}
}

func TestGreedyBroker(t *testing.T) {
	dir := t.TempDir()

	conf, err := testutil.OutputFromYAML(strings.ReplaceAll(`
broker:
  pattern: greedy
  outputs:
    - file:
        path: '$DIR/one/foo-${!count("gfoo")}.txt'
        codec: all-bytes
      processors:
        - bloblang: 'root  = "one-" + content()'
        - sleep:
            duration: 50ms
    - file:
        path: '$DIR/two/bar-${!count("gbar")}.txt'
        codec: all-bytes
      processors:
        - bloblang: 'root  = "two-" + content()'
        - sleep:
            duration: 50ms
`, "$DIR", dir))
	require.NoError(t, err)

	s, err := mock.NewManager().NewOutput(conf)
	require.NoError(t, err)

	sendChan := make(chan message.Transaction)
	resChan := make(chan error)
	if err = s.Consume(sendChan); err != nil {
		t.Fatal(err)
	}

	defer func() {
		close(sendChan)
		s.TriggerCloseNow()

		ctx, done := context.WithTimeout(context.Background(), time.Second*10)
		assert.NoError(t, s.WaitForClose(ctx))
		done()
	}()

	inputs := []string{
		"first", "second", "third", "fourth",
	}
	expFiles := map[string][2]string{
		"./one/foo-1.txt": {"one-first", "one-second"},
		"./one/foo-2.txt": {"one-third", "one-fourth"},
		"./two/bar-1.txt": {"two-first", "two-second"},
		"./two/bar-2.txt": {"two-third", "two-fourth"},
	}

	for _, input := range inputs {
		testMsg := message.QuickBatch([][]byte{[]byte(input)})
		select {
		case sendChan <- message.NewTransaction(testMsg, resChan):
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}

		select {
		case res := <-resChan:
			if res != nil {
				t.Fatal(res)
			}
		case <-time.After(time.Second):
			t.Fatal("Action timed out")
		}
	}

	for k, exp := range expFiles {
		k = filepath.Join(dir, k)
		fileBytes, err := os.ReadFile(k)
		if err != nil {
			t.Errorf("Expected file '%v' could not be read: %v", k, err)
			continue
		}
		if act := string(fileBytes); exp[0] != act && exp[1] != act {
			t.Errorf("Wrong contents for file '%v': %v != (%v || %v)", k, act, exp[0], exp[1])
		}
	}
}

type mockOutput struct {
	outputs map[string]struct{}
	mut     sync.Mutex
}

func (m *mockOutput) Connect(context.Context) error {
	return nil
}

func (m *mockOutput) Write(ctx context.Context, msg *service.Message) error {
	m.mut.Lock()
	defer m.mut.Unlock()
	mBytes, err := msg.AsBytes()
	m.outputs[string(mBytes)] = struct{}{}
	return err
}

func (m *mockOutput) Close(context.Context) error {
	return nil
}

func TestOutputBrokerConfigs(t *testing.T) {
	for _, test := range []struct {
		name         string
		inputConfig  string
		outputConfig string
		output       map[string]struct{}
	}{
		{
			name: "simple inputs",
			inputConfig: `
generate:
  count: 1
  interval: ""
  mapping: 'root = "hello world 1"'
`,
			outputConfig: `
broker:
  outputs:
    - testmeow: {}
      processors:
        - bloblang: '"first " + content()'
    - testmeow: {}
      processors:
        - bloblang: '"second " + content()'
`,
			output: map[string]struct{}{
				"first hello world 1":  {},
				"second hello world 1": {},
			},
		},
		{
			name: "single input nested processors",
			inputConfig: `
generate:
  count: 1
  interval: ""
  mapping: 'root = "hello world 1"'
`,
			outputConfig: `
broker:
  outputs:
    - testmeow: {}
      processors:
        - bloblang: 'root = content().uppercase()'
processors:
  - bloblang: 'root = "outer: " + content()'
`,
			output: map[string]struct{}{
				"OUTER: HELLO WORLD 1": {},
			},
		},
		{
			name: "single input nested and batched processors",
			inputConfig: `
generate:
  count: 3
  interval: ""
  mapping: 'root = "hello world 1"'
`,
			outputConfig: `
processors:
  - bloblang: 'root = "outer: " + content()'

broker:
  batching:
    count: 3
    processors:
      - archive:
          format: lines

  outputs:
    - testmeow: {}
      processors:
        - bloblang: 'root = "inner: " + content()'
`,
			output: map[string]struct{}{
				"inner: outer: hello world 1\nouter: hello world 1\nouter: hello world 1": {},
			},
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			mOut := &mockOutput{
				outputs: map[string]struct{}{},
			}

			env := service.NewEnvironment()
			require.NoError(t, env.RegisterOutput("testmeow", service.NewConfigSpec(),
				func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
					maxInFlight = 1
					out = mOut
					return
				}))

			builder := env.NewStreamBuilder()
			require.NoError(t, builder.AddInputYAML(test.inputConfig))
			require.NoError(t, builder.AddOutputYAML(test.outputConfig))
			require.NoError(t, builder.SetLoggerYAML(`level: none`))

			strm, err := builder.Build()
			require.NoError(t, err)

			tCtx, done := context.WithTimeout(context.Background(), time.Minute)
			defer done()

			require.NoError(t, strm.Run(tCtx))
			assert.Equal(t, test.output, mOut.outputs)
		})
	}
}
