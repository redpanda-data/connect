package integration

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v3"
)

type testConfigVars struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	id string

	// A port to use in connector URLs. Allowing tests to override this
	// potentially enables tests that check for faulty connections by bridging.
	port string

	// A second port to use in secondary connector URLs.
	portTwo string

	// Used by batching testers to check the input honours batching fields.
	inputBatchCount int

	// Used by batching testers to check the output honours batching fields.
	outputBatchCount int

	// Used by testers to check the max in flight option of outputs.
	maxInFlight int

	// Generic variables.
	var1 string
}

type testEnvironment struct {
	configTemplate string
	configVars     testConfigVars

	preTest func(*testing.T, *testEnvironment)

	timeout time.Duration
	ctx     context.Context
	log     log.Modular
	stats   metrics.Type

	// Ugly work arounds for slow connectors.
	sleepAfterInput  time.Duration
	sleepAfterOutput time.Duration
}

func newTestEnvironment(t *testing.T, confTemplate string) testEnvironment {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return testEnvironment{
		configTemplate: confTemplate,
		configVars: testConfigVars{
			id:          u4.String(),
			maxInFlight: 1,
		},
		timeout: time.Second * 60,
		ctx:     context.Background(),
		log:     log.Noop(),
		stats:   metrics.Noop(),
	}
}

func (e testEnvironment) RenderConfig() string {
	return strings.NewReplacer(
		"$ID", e.configVars.id,
		"$PORT_TWO", e.configVars.portTwo,
		"$PORT", e.configVars.port,
		"$VAR1", e.configVars.var1,
		"$INPUT_BATCH_COUNT", strconv.Itoa(e.configVars.inputBatchCount),
		"$OUTPUT_BATCH_COUNT", strconv.Itoa(e.configVars.outputBatchCount),
		"$MAX_IN_FLIGHT", strconv.Itoa(e.configVars.maxInFlight),
	).Replace(e.configTemplate)
}

//------------------------------------------------------------------------------

type testOptFunc func(*testEnvironment)

func testOptTimeout(timeout time.Duration) testOptFunc {
	return func(env *testEnvironment) {
		env.timeout = timeout
	}
}

func testOptMaxInFlight(n int) testOptFunc {
	return func(env *testEnvironment) {
		env.configVars.maxInFlight = n
	}
}

func testOptLogging(level string) testOptFunc {
	return func(env *testEnvironment) {
		logConf := log.NewConfig()
		logConf.LogLevel = level
		env.log = log.New(os.Stdout, logConf)
	}
}

func testOptPort(port string) testOptFunc {
	return func(env *testEnvironment) {
		env.configVars.port = port
	}
}

func testOptPortTwo(portTwo string) testOptFunc {
	return func(env *testEnvironment) {
		env.configVars.portTwo = portTwo
	}
}

func testOptVarOne(v string) testOptFunc {
	return func(env *testEnvironment) {
		env.configVars.var1 = v
	}
}

func testOptSleepAfterInput(t time.Duration) testOptFunc {
	return func(env *testEnvironment) {
		env.sleepAfterInput = t
	}
}

func testOptSleepAfterOutput(t time.Duration) testOptFunc {
	return func(env *testEnvironment) {
		env.sleepAfterOutput = t
	}
}

func testOptPreTest(fn func(*testing.T, *testEnvironment)) testOptFunc {
	return func(env *testEnvironment) {
		env.preTest = fn
	}
}

//------------------------------------------------------------------------------

type testDefinition func(*testing.T, *testEnvironment)

type integrationTestList []testDefinition

func integrationTests(tests ...testDefinition) integrationTestList {
	return tests
}

func (i integrationTestList) Run(t *testing.T, configTemplate string, opts ...testOptFunc) {
	for _, test := range i {
		env := newTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		var done func()
		env.ctx, done = context.WithTimeout(env.ctx, env.timeout)
		t.Cleanup(done)

		if env.preTest != nil {
			env.preTest(t, &env)
		}
		test(t, &env)
	}
}

//------------------------------------------------------------------------------

func namedTest(name string, test testDefinition) testDefinition {
	return func(t *testing.T, env *testEnvironment) {
		t.Run(name, func(t *testing.T) {
			test(t, env)
		})
	}
}

//------------------------------------------------------------------------------

func initConnectors(
	t *testing.T,
	trans <-chan types.Transaction,
	env *testEnvironment,
) (types.Input, types.Output) {
	t.Helper()

	confBytes := []byte(env.RenderConfig())

	s := config.New()
	dec := yaml.NewDecoder(bytes.NewReader(confBytes))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&s))

	lints, err := config.Lint(confBytes, s)
	require.NoError(t, err)
	assert.Empty(t, lints)

	output, err := output.New(s.Output, types.NoopMgr(), env.log, env.stats)
	require.NoError(t, err)

	require.NoError(t, output.Consume(trans))

	if env.sleepAfterOutput > 0 {
		time.Sleep(env.sleepAfterOutput)
	}

	input, err := input.New(s.Input, types.NoopMgr(), env.log, env.stats)
	require.NoError(t, err)

	if env.sleepAfterInput > 0 {
		time.Sleep(env.sleepAfterInput)
	}

	return input, output
}

func closeConnectors(t *testing.T, input types.Input, output types.Output) {
	output.CloseAsync()
	require.NoError(t, output.WaitForClose(time.Second*10))

	input.CloseAsync()
	require.NoError(t, input.WaitForClose(time.Second*10))
}

func sendMessage(
	ctx context.Context,
	t *testing.T,
	tranChan chan types.Transaction,
	content string,
	metadata ...string,
) error {
	t.Helper()

	p := message.NewPart([]byte(content))
	for i := 0; i < len(metadata); i += 2 {
		p.Metadata().Set(metadata[i], metadata[i+1])
	}
	msg := message.New(nil)
	msg.Append(p)

	resChan := make(chan types.Response)

	select {
	case tranChan <- types.NewTransaction(msg, resChan):
	case <-ctx.Done():
		t.Fatal("timed out on send")
	}

	select {
	case res := <-resChan:
		return res.Error()
	case <-ctx.Done():
	}
	t.Fatal("timed out on response")
	return nil
}

func sendBatch(
	ctx context.Context,
	t *testing.T,
	tranChan chan types.Transaction,
	content []string,
) error {
	t.Helper()

	msg := message.New(nil)
	for _, payload := range content {
		msg.Append(message.NewPart([]byte(payload)))
	}

	resChan := make(chan types.Response)

	select {
	case tranChan <- types.NewTransaction(msg, resChan):
	case <-ctx.Done():
		t.Fatal("timed out on send")
	}

	select {
	case res := <-resChan:
		return res.Error()
	case <-ctx.Done():
	}

	t.Fatal("timed out on response")
	return nil
}

func receiveMessage(
	ctx context.Context,
	t *testing.T,
	tranChan <-chan types.Transaction,
) types.Part {
	t.Helper()

	var tran types.Transaction
	select {
	case tran = <-tranChan:
	case <-ctx.Done():
		t.Fatal("timed out on receive")
	}

	require.Equal(t, tran.Payload.Len(), 1)

	select {
	case tran.ResponseChan <- response.NewAck():
	case <-ctx.Done():
		t.Fatal("timed out on response")
	}

	return tran.Payload.Get(0)
}

func messageMatch(t *testing.T, p types.Part, content string, metadata ...string) {
	t.Helper()

	assert.Equal(t, content, string(p.Get()))

	allMetadata := map[string]string{}
	p.Metadata().Iter(func(k, v string) error {
		allMetadata[k] = v
		return nil
	})

	for i := 0; i < len(metadata); i += 2 {
		assert.Equal(t, metadata[i+1], p.Metadata().Get(metadata[i]), fmt.Sprintf("metadata: %v", allMetadata))
	}
}

func messageInSet(t *testing.T, pop bool, p types.Part, set map[string][]string) {
	t.Helper()

	metadata, exists := set[string(p.Get())]
	require.True(t, exists, "in set: %v", string(p.Get()))

	for i := 0; i < len(metadata); i += 2 {
		assert.Equal(t, metadata[i+1], p.Metadata().Get(metadata[i]))
	}

	if pop {
		delete(set, string(p.Get()))
	}
}

//------------------------------------------------------------------------------

func integrationTestOpenClose() testDefinition {
	return namedTest(
		"can open and close",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)

			require.NoError(t, sendMessage(env.ctx, t, tranChan, "hello world"))
			messageMatch(t, receiveMessage(env.ctx, t, input.TransactionChan()), "hello world")

			closeConnectors(t, input, output)
		},
	)
}

func integrationTestMetadata() testDefinition {
	return namedTest(
		"can send and receive metadata",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)

			require.NoError(t, sendMessage(
				env.ctx, t, tranChan,
				"hello world",
				"foo", "foo_value",
				"bar", "bar_value",
			))
			messageMatch(
				t, receiveMessage(env.ctx, t, input.TransactionChan()),
				"hello world",
				"foo", "foo_value",
				"bar", "bar_value",
			)

			closeConnectors(t, input, output)
		},
	)
}

func integrationTestSendBatch(n int) testDefinition {
	return namedTest(
		"can send a message batch of a size, and receive them",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)

			set := map[string][]string{}
			payloads := []string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				payloads = append(payloads, payload)
			}
			sendBatch(env.ctx, t, tranChan, payloads)

			for i := 0; i < n; i++ {
				messageInSet(t, true, receiveMessage(env.ctx, t, input.TransactionChan()), set)
			}

			closeConnectors(t, input, output)
		},
	)
}

func integrationTestSendBatchCount(n int) testDefinition {
	return namedTest(
		"can send messages when configured with an output batch count",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			env.configVars.outputBatchCount = n

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)

			resChan := make(chan types.Response)

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world %v", i)
				set[payload] = nil
				msg := message.New(nil)
				msg.Append(message.NewPart([]byte(payload)))
				select {
				case tranChan <- types.NewTransaction(msg, resChan):
				case res := <-resChan:
					t.Fatalf("premature response: %v", res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on send")
				}
			}

			for i := 0; i < n; i++ {
				select {
				case res := <-resChan:
					assert.NoError(t, res.Error())
				case <-env.ctx.Done():
					t.Fatal("timed out on response")
				}
			}

			for i := 0; i < n; i++ {
				messageInSet(t, true, receiveMessage(env.ctx, t, input.TransactionChan()), set)
			}

			closeConnectors(t, input, output)
		},
	)
}

func integrationTestLotsOfDataSequential(n int) testDefinition {
	return namedTest(
		"can send and receive lots of data sequentially without loss or duplicates",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)

			set := map[string][]string{}

			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
				require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
			}

			for i := 0; i < n; i++ {
				messageInSet(t, true, receiveMessage(env.ctx, t, input.TransactionChan()), set)
			}

			closeConnectors(t, input, output)
		},
	)
}

func integrationTestLotsOfDataParallel(n int) testDefinition {
	return namedTest(
		"can send and receive lots of data in parallel without loss or duplicates",
		func(t *testing.T, env *testEnvironment) {
			t.Parallel()

			tranChan := make(chan types.Transaction)
			input, output := initConnectors(t, tranChan, env)

			set := map[string][]string{}
			for i := 0; i < n; i++ {
				payload := fmt.Sprintf("hello world: %v", i)
				set[payload] = nil
			}

			wg := sync.WaitGroup{}
			wg.Add(2)

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					payload := fmt.Sprintf("hello world: %v", i)
					require.NoError(t, sendMessage(env.ctx, t, tranChan, payload))
				}
			}()

			go func() {
				defer wg.Done()
				for i := 0; i < n; i++ {
					messageInSet(t, true, receiveMessage(env.ctx, t, input.TransactionChan()), set)
				}
			}()

			wg.Wait()

			closeConnectors(t, input, output)
		},
	)
}
