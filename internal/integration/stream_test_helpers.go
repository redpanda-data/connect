package integration

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/types"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// CheckSkip marks a test to be skipped unless the integration test has been
// specifically requested using the -run flag.
func CheckSkip(t *testing.T) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^TestIntegration$")
	}
}

// GetFreePort attempts to get a free port. This involves creating a bind and
// then immediately dropping it and so it's ever so slightly flakey.
func GetFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// StreamTestConfigVars defines variables that will be accessed by test
// definitions when generating components through the config template. The main
// value is the id, which is generated for each test for isolation, and the port
// which is injected into the config template.
type StreamTestConfigVars struct {
	// A unique identifier for separating this test configuration from others.
	// Usually used to access a different topic, consumer group, directory, etc.
	ID string

	// A port to use in connector URLs. Allowing tests to override this
	// potentially enables tests that check for faulty connections by bridging.
	port string

	// A second port to use in secondary connector URLs.
	portTwo string

	// A third port to use in tertiary connector URLs.
	portThree string

	// A fourth port to use in quarternary connector URLs.
	portFour string

	// Used by batching testers to check the input honours batching fields.
	InputBatchCount int

	// Used by batching testers to check the output honours batching fields.
	OutputBatchCount int

	// Used by metadata filter tests to check that filters work.
	OutputMetaExcludePrefix string

	// Used by testers to check the max in flight option of outputs.
	MaxInFlight int

	// Generic variables.
	Var1 string
	Var2 string
	Var3 string
	Var4 string
}

// StreamPreTestFn is an optional closure to be called before tests are run,
// this is an opportunity to mutate test config variables and mess with the
// environment.
type StreamPreTestFn func(t testing.TB, ctx context.Context, testID string, vars *StreamTestConfigVars)

type streamTestEnvironment struct {
	configTemplate string
	configVars     StreamTestConfigVars

	preTest StreamPreTestFn

	timeout time.Duration
	ctx     context.Context
	log     log.Modular
	stats   metrics.Type
	mgr     types.Manager

	allowDuplicateMessages bool

	// Ugly work arounds for slow connectors.
	sleepAfterInput  time.Duration
	sleepAfterOutput time.Duration
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func newStreamTestEnvironment(t testing.TB, confTemplate string) streamTestEnvironment {
	t.Helper()

	u4, err := uuid.NewV4()
	require.NoError(t, err)

	return streamTestEnvironment{
		configTemplate: confTemplate,
		configVars: StreamTestConfigVars{
			ID:          u4.String(),
			MaxInFlight: 1,
		},
		timeout: time.Second * 90,
		ctx:     context.Background(),
		log:     log.Noop(),
		stats:   metrics.Noop(),
	}
}

func (e streamTestEnvironment) RenderConfig() string {
	return strings.NewReplacer(
		"$ID", e.configVars.ID,
		"$PORT_TWO", e.configVars.portTwo,
		"$PORT_THREE", e.configVars.portThree,
		"$PORT_FOUR", e.configVars.portFour,
		"$PORT", e.configVars.port,
		"$VAR1", e.configVars.Var1,
		"$VAR2", e.configVars.Var2,
		"$VAR3", e.configVars.Var3,
		"$VAR4", e.configVars.Var4,
		"$INPUT_BATCH_COUNT", strconv.Itoa(e.configVars.InputBatchCount),
		"$OUTPUT_BATCH_COUNT", strconv.Itoa(e.configVars.OutputBatchCount),
		"$OUTPUT_META_EXCLUDE_PREFIX", e.configVars.OutputMetaExcludePrefix,
		"$MAX_IN_FLIGHT", strconv.Itoa(e.configVars.MaxInFlight),
	).Replace(e.configTemplate)
}

//------------------------------------------------------------------------------

// StreamTestOptFunc is an opt func for customizing the behaviour of stream
// tests, these are useful for things that are integration environment specific,
// such as the port of the service being interacted with.
type StreamTestOptFunc func(*streamTestEnvironment)

// StreamTestOptTimeout describes an optional timeout spanning the entirety of
// the test suite.
func StreamTestOptTimeout(timeout time.Duration) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.timeout = timeout
	}
}

// StreamTestOptAllowDupes specifies across all stream tests that in this
// environment we can expect duplicates and these are not considered errors.
func StreamTestOptAllowDupes() StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.allowDuplicateMessages = true
	}
}

// StreamTestOptMaxInFlight configures a maximum inflight (to be injected into
// the config template) for all tests.
func StreamTestOptMaxInFlight(n int) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.MaxInFlight = n
	}
}

// StreamTestOptLogging allows components to log with the given log level. This
// is useful for diagnosing issues.
func StreamTestOptLogging(level string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		logConf := log.NewConfig()
		logConf.LogLevel = level
		env.log = log.New(os.Stdout, logConf)
	}
}

// StreamTestOptPort defines the port of the integration service.
func StreamTestOptPort(port string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.port = port
	}
}

// StreamTestOptPortTwo defines a secondary port of the integration service.
func StreamTestOptPortTwo(portTwo string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.portTwo = portTwo
	}
}

// StreamTestOptVarOne sets an arbitrary variable for the test that can be
// injected into templated configs.
func StreamTestOptVarOne(v string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.Var1 = v
	}
}

// StreamTestOptVarTwo sets a second arbitrary variable for the test that can be
// injected into templated configs.
func StreamTestOptVarTwo(v string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.Var2 = v
	}
}

// StreamTestOptVarThree sets a third arbitrary variable for the test that can
// be injected into templated configs.
func StreamTestOptVarThree(v string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.Var3 = v
	}
}

// StreamTestOptSleepAfterInput adds a sleep to tests after the input has been
// created.
func StreamTestOptSleepAfterInput(t time.Duration) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.sleepAfterInput = t
	}
}

// StreamTestOptSleepAfterOutput adds a sleep to tests after the output has been
// created.
func StreamTestOptSleepAfterOutput(t time.Duration) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.sleepAfterOutput = t
	}
}

// StreamTestOptPreTest adds a closure to be executed before each test.
func StreamTestOptPreTest(fn StreamPreTestFn) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.preTest = fn
	}
}

//------------------------------------------------------------------------------

type streamTestDefinitionFn func(*testing.T, *streamTestEnvironment)

// StreamTestDefinition encompasses a unit test to be executed against an
// integration environment. These tests are generic and can be run against any
// configuration containing an input and an output that are connected.
type StreamTestDefinition struct {
	fn func(*testing.T, *streamTestEnvironment)
}

// StreamTestList is a list of stream definitions that can be run with a single
// template and function args.
type StreamTestList []StreamTestDefinition

// StreamTests creates a list of tests from variadic arguments.
func StreamTests(tests ...StreamTestDefinition) StreamTestList {
	return tests
}

// Run all the tests against a config template. Tests are run in parallel.
func (i StreamTestList) Run(t *testing.T, configTemplate string, opts ...StreamTestOptFunc) {
	envs := make([]streamTestEnvironment, len(i))

	wg := sync.WaitGroup{}
	for j := range i {
		envs[j] = newStreamTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&envs[j])
		}

		timeout := envs[j].timeout
		if deadline, ok := t.Deadline(); ok {
			timeout = time.Until(deadline) - (time.Second * 5)
		}

		var done func()
		envs[j].ctx, done = context.WithTimeout(envs[j].ctx, timeout)
		t.Cleanup(done)

		if envs[j].preTest != nil {
			wg.Add(1)
			env := &envs[j]
			go func() {
				defer wg.Done()
				env.preTest(t, env.ctx, env.configVars.ID, &env.configVars)
			}()
		}
	}
	wg.Wait()

	for j, test := range i {
		if envs[j].configVars.port == "" {
			p, err := getFreePort()
			if err != nil {
				t.Fatal(err)
			}
			envs[j].configVars.port = strconv.Itoa(p)
		}
		test.fn(t, &envs[j])
	}
}

// RunSequentially executes all the tests against a config template
// sequentially.
func (i StreamTestList) RunSequentially(t *testing.T, configTemplate string, opts ...StreamTestOptFunc) {
	for _, test := range i {
		env := newStreamTestEnvironment(t, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		timeout := env.timeout
		if deadline, ok := t.Deadline(); ok {
			timeout = time.Until(deadline) - (time.Second * 5)
		}

		var done func()
		env.ctx, done = context.WithTimeout(env.ctx, timeout)
		t.Cleanup(done)

		if env.preTest != nil {
			env.preTest(t, env.ctx, env.configVars.ID, &env.configVars)
		}
		t.Run("seq", func(t *testing.T) {
			test.fn(t, &env)
		})
	}
}

//------------------------------------------------------------------------------

func namedStreamTest(name string, test streamTestDefinitionFn) StreamTestDefinition {
	return StreamTestDefinition{
		fn: func(t *testing.T, env *streamTestEnvironment) {
			t.Run(name, func(t *testing.T) {
				test(t, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

type streamBenchDefinitionFn func(*testing.B, *streamTestEnvironment)

// StreamBenchDefinition encompasses a benchmark to be executed against an
// integration environment. These tests are generic and can be run against any
// configuration containing an input and an output that are connected.
type StreamBenchDefinition struct {
	fn streamBenchDefinitionFn
}

// StreamBenchList is a list of stream benchmark definitions that can be run
// with a single template and function args.
type StreamBenchList []StreamBenchDefinition

// StreamBenchs creates a list of benchmarks from variadic arguments.
func StreamBenchs(tests ...StreamBenchDefinition) StreamBenchList {
	return tests
}

// Run the benchmarks against a config template.
func (i StreamBenchList) Run(b *testing.B, configTemplate string, opts ...StreamTestOptFunc) {
	for _, bench := range i {
		env := newStreamTestEnvironment(b, configTemplate)
		for _, opt := range opts {
			opt(&env)
		}

		if env.preTest != nil {
			env.preTest(b, env.ctx, env.configVars.ID, &env.configVars)
		}
		bench.fn(b, &env)
	}
}

func namedBench(name string, test streamBenchDefinitionFn) StreamBenchDefinition {
	return StreamBenchDefinition{
		fn: func(b *testing.B, env *streamTestEnvironment) {
			b.Run(name, func(b *testing.B) {
				test(b, env)
			})
		},
	}
}

//------------------------------------------------------------------------------

func initConnectors(
	t testing.TB,
	trans <-chan types.Transaction,
	env *streamTestEnvironment,
) (types.Input, types.Output) {
	t.Helper()

	out := initOutput(t, trans, env)
	in := initInput(t, env)
	return in, out
}

func initInput(t testing.TB, env *streamTestEnvironment) types.Input {
	t.Helper()

	confBytes := []byte(env.RenderConfig())

	s := config.New()
	dec := yaml.NewDecoder(bytes.NewReader(confBytes))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&s))

	lints, err := config.LintV2(docs.NewLintContext(), confBytes)
	require.NoError(t, err)
	assert.Empty(t, lints)

	if env.mgr == nil {
		env.mgr, err = manager.NewV2(s.ResourceConfig, nil, env.log, env.stats)
		require.NoError(t, err)
	}

	input, err := input.New(s.Input, env.mgr, env.log, env.stats)
	require.NoError(t, err)

	if env.sleepAfterInput > 0 {
		time.Sleep(env.sleepAfterInput)
	}

	return input
}

func initOutput(t testing.TB, trans <-chan types.Transaction, env *streamTestEnvironment) types.Output {
	t.Helper()

	confBytes := []byte(env.RenderConfig())

	s := config.New()
	dec := yaml.NewDecoder(bytes.NewReader(confBytes))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&s))

	lints, err := config.LintV2(docs.NewLintContext(), confBytes)
	require.NoError(t, err)
	assert.Empty(t, lints)

	if env.mgr == nil {
		env.mgr, err = manager.NewV2(s.ResourceConfig, nil, env.log, env.stats)
		require.NoError(t, err)
	}

	output, err := output.New(s.Output, env.mgr, env.log, env.stats)
	require.NoError(t, err)

	require.NoError(t, output.Consume(trans))

	require.Error(t, output.WaitForClose(time.Millisecond*100))
	if env.sleepAfterOutput > 0 {
		time.Sleep(env.sleepAfterOutput)
	}

	return output
}

func closeConnectors(t testing.TB, input types.Input, output types.Output) {
	if output != nil {
		output.CloseAsync()
		require.NoError(t, output.WaitForClose(time.Second*10))
	}
	if input != nil {
		input.CloseAsync()
		require.NoError(t, input.WaitForClose(time.Second*10))
	}
}

func sendMessage(
	ctx context.Context,
	t testing.TB,
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
	t testing.TB,
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
	t testing.TB,
	tranChan <-chan types.Transaction,
	err error,
) types.Part {
	t.Helper()

	b, resChan := receiveMessageNoRes(ctx, t, tranChan)
	sendResponse(ctx, t, resChan, err)
	return b
}

func sendResponse(ctx context.Context, t testing.TB, resChan chan<- types.Response, err error) {
	var res types.Response = response.NewAck()
	if err != nil {
		res = response.NewError(err)
	}

	select {
	case resChan <- res:
	case <-ctx.Done():
		t.Fatal("timed out on response")
	}
}

// nolint:gocritic // Ignore unnamedResult false positive
func receiveMessageNoRes(ctx context.Context, t testing.TB, tranChan <-chan types.Transaction) (types.Part, chan<- types.Response) {
	t.Helper()

	var tran types.Transaction
	var open bool
	select {
	case tran, open = <-tranChan:
	case <-ctx.Done():
		t.Fatal("timed out on receive")
	}

	require.True(t, open)
	require.Equal(t, tran.Payload.Len(), 1)

	return tran.Payload.Get(0), tran.ResponseChan
}

func messageMatch(t testing.TB, p types.Part, content string, metadata ...string) {
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

func messageInSet(t testing.TB, pop, allowDupes bool, p types.Part, set map[string][]string) {
	t.Helper()

	metadata, exists := set[string(p.Get())]
	if allowDupes && !exists {
		return
	}
	require.True(t, exists, "in set: %v, set: %v", string(p.Get()), set)

	for i := 0; i < len(metadata); i += 2 {
		assert.Equal(t, metadata[i+1], p.Metadata().Get(metadata[i]))
	}

	if pop {
		delete(set, string(p.Get()))
	}
}
