package integration

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/message"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// CheckSkip marks a test to be skipped unless the integration test has been
// specifically requested using the -run flag.
func CheckSkip(t testing.TB) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || regexp.MustCompile(strings.Split(m, "/")[0]).FindString(t.Name()) == "" {
		t.Skip("Skipping as execution was not requested explicitly using go test -run ^Test.*Integration.*$")
	}
}

// CheckSkipExact skips a test unless the -run flag specifically targets it.
func CheckSkipExact(t testing.TB) {
	if m := flag.Lookup("test.run").Value.String(); m == "" || m != t.Name() {
		t.Skipf("Skipping as execution was not requested explicitly using go test -run %v", t.Name())
	}
}

// GetFreePort attempts to get a free port. This involves creating a bind and
// then immediately dropping it and so it's ever so slightly flakey.
func GetFreePort() (int, error) {
	listener, err := net.Listen("tcp", ":0")
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
	Var5 string
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
	stats   *metrics.Namespaced
	mgr     bundle.NewManagement

	allowDuplicateMessages bool

	// Ugly work arounds for slow connectors.
	sleepAfterInput  time.Duration
	sleepAfterOutput time.Duration
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
		"$VAR5", e.configVars.Var5,
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
		var err error
		env.log, err = log.New(os.Stdout, ifs.OS(), logConf)
		if err != nil {
			panic(err)
		}
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

// StreamTestOptVarFour sets a third arbitrary variable for the test that can
// be injected into templated configs.
func StreamTestOptVarFour(v string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.Var4 = v
	}
}

// StreamTestOptVarFive sets a third arbitrary variable for the test that can
// be injected into templated configs.
func StreamTestOptVarFive(v string) StreamTestOptFunc {
	return func(env *streamTestEnvironment) {
		env.configVars.Var5 = v
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
	}

	for j, test := range i {
		if envs[j].configVars.port == "" {
			p, err := GetFreePort()
			if err != nil {
				t.Fatal(err)
			}
			envs[j].configVars.port = strconv.Itoa(p)
		}
		test.fn(t, &envs[j])
	}
}

//------------------------------------------------------------------------------

func namedStreamTest(name string, test streamTestDefinitionFn) StreamTestDefinition {
	return StreamTestDefinition{
		fn: func(t *testing.T, env *streamTestEnvironment) {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				if env.preTest != nil {
					env.preTest(t, env.ctx, env.configVars.ID, &env.configVars)
				}
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
	trans <-chan message.Transaction,
	env *streamTestEnvironment,
) (input input.Streamed, output output.Streamed) {
	t.Helper()

	out := initOutput(t, trans, env)
	in := initInput(t, env)
	return in, out
}

func initInput(t testing.TB, env *streamTestEnvironment) input.Streamed {
	t.Helper()

	node, err := docs.UnmarshalYAML([]byte(env.RenderConfig()))
	require.NoError(t, err)

	spec := docs.FieldSpecs{
		docs.FieldAnything("output", "").Optional(),
		docs.FieldInput("input", "An input to source messages from."),
	}
	spec = append(spec, manager.Spec()...)

	lints := spec.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node)
	assert.Empty(t, lints)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	pVal, err := pConf.FieldAny("input")
	require.NoError(t, err)

	iConf, err := input.FromAny(bundle.GlobalEnvironment, pVal)
	require.NoError(t, err)

	mConf, err := manager.FromParsed(bundle.GlobalEnvironment, pConf)
	require.NoError(t, err)

	if env.mgr == nil {
		env.mgr, err = manager.New(mConf, manager.OptSetLogger(env.log), manager.OptSetMetrics(env.stats))
		require.NoError(t, err)
	}

	input, err := env.mgr.NewInput(iConf)
	require.NoError(t, err)

	if env.sleepAfterInput > 0 {
		time.Sleep(env.sleepAfterInput)
	}

	return input
}

func initOutput(t testing.TB, trans <-chan message.Transaction, env *streamTestEnvironment) output.Streamed {
	t.Helper()

	node, err := docs.UnmarshalYAML([]byte(env.RenderConfig()))
	require.NoError(t, err)

	spec := docs.FieldSpecs{
		docs.FieldAnything("input", "").Optional(),
		docs.FieldOutput("output", "An output to source messages to."),
	}
	spec = append(spec, manager.Spec()...)

	lints := spec.LintYAML(docs.NewLintContext(docs.NewLintConfig(bundle.GlobalEnvironment)), node)
	assert.Empty(t, lints)

	pConf, err := spec.ParsedConfigFromAny(node)
	require.NoError(t, err)

	pVal, err := pConf.FieldAny("output")
	require.NoError(t, err)

	oConf, err := output.FromAny(bundle.GlobalEnvironment, pVal)
	require.NoError(t, err)

	mConf, err := manager.FromParsed(bundle.GlobalEnvironment, pConf)
	require.NoError(t, err)

	if env.mgr == nil {
		env.mgr, err = manager.New(mConf, manager.OptSetLogger(env.log), manager.OptSetMetrics(env.stats))
		require.NoError(t, err)
	}

	output, err := env.mgr.NewOutput(oConf)
	require.NoError(t, err)

	require.NoError(t, output.Consume(trans))

	if env.sleepAfterOutput > 0 {
		time.Sleep(env.sleepAfterOutput)
	}

	return output
}

func closeConnectors(t testing.TB, env *streamTestEnvironment, input input.Streamed, output output.Streamed) {
	if output != nil {
		output.TriggerCloseNow()
		require.NoError(t, output.WaitForClose(env.ctx))
	}
	if input != nil {
		input.TriggerStopConsuming()
		require.NoError(t, input.WaitForClose(env.ctx))
	}
}

func sendMessage(
	ctx context.Context,
	t testing.TB,
	tranChan chan message.Transaction,
	content string,
	metadata ...string,
) error {
	t.Helper()

	p := message.NewPart([]byte(content))
	for i := 0; i < len(metadata); i += 2 {
		p.MetaSetMut(metadata[i], metadata[i+1])
	}
	msg := message.Batch{p}
	resChan := make(chan error)

	select {
	case tranChan <- message.NewTransaction(msg, resChan):
	case <-ctx.Done():
		t.Fatal("timed out on send")
	}

	select {
	case res := <-resChan:
		return res
	case <-ctx.Done():
	}
	t.Fatal("timed out on response")
	return nil
}

func sendBatch(
	ctx context.Context,
	t testing.TB,
	tranChan chan message.Transaction,
	content []string,
) error {
	t.Helper()

	msg := message.QuickBatch(nil)
	for _, payload := range content {
		msg = append(msg, message.NewPart([]byte(payload)))
	}

	resChan := make(chan error)

	select {
	case tranChan <- message.NewTransaction(msg, resChan):
	case <-ctx.Done():
		t.Fatal("timed out on send")
	}

	select {
	case res := <-resChan:
		return res
	case <-ctx.Done():
	}

	t.Fatal("timed out on response")
	return nil
}

func receiveMessage(
	ctx context.Context,
	t testing.TB,
	tranChan <-chan message.Transaction,
	err error,
) *message.Part {
	t.Helper()

	b, ackFn := receiveBatchNoRes(ctx, t, tranChan)
	require.NoError(t, ackFn(ctx, err))
	require.Len(t, b, 1)

	return b.Get(0)
}

func receiveBatch(
	ctx context.Context,
	t testing.TB,
	tranChan <-chan message.Transaction,
	err error,
) message.Batch {
	t.Helper()

	b, ackFn := receiveBatchNoRes(ctx, t, tranChan)
	require.NoError(t, ackFn(ctx, err))
	return b
}

func receiveBatchNoRes(ctx context.Context, t testing.TB, tranChan <-chan message.Transaction) (message.Batch, func(context.Context, error) error) { //nolint: gocritic // Ignore unnamedResult false positive
	t.Helper()

	var tran message.Transaction
	var open bool
	select {
	case tran, open = <-tranChan:
	case <-ctx.Done():
		t.Fatal("timed out on receive")
	}

	require.True(t, open)
	return tran.Payload, tran.Ack
}

func receiveMessageNoRes(ctx context.Context, t testing.TB, tranChan <-chan message.Transaction) (*message.Part, func(context.Context, error) error) { //nolint: gocritic // Ignore unnamedResult false positive
	t.Helper()

	b, fn := receiveBatchNoRes(ctx, t, tranChan)
	require.Len(t, b, 1)

	return b.Get(0), fn
}

func messageMatch(t testing.TB, p *message.Part, content string, metadata ...string) {
	t.Helper()

	assert.Equal(t, content, string(p.AsBytes()))

	allMetadata := map[string]string{}
	_ = p.MetaIterStr(func(k, v string) error {
		allMetadata[k] = v
		return nil
	})

	for i := 0; i < len(metadata); i += 2 {
		assert.Equal(t, metadata[i+1], p.MetaGetStr(metadata[i]), fmt.Sprintf("metadata: %v", allMetadata))
	}
}

func messagesInSet(t testing.TB, pop, allowDupes bool, b message.Batch, set map[string][]string) {
	t.Helper()

	for _, p := range b {
		metadata, exists := set[string(p.AsBytes())]
		if allowDupes && !exists {
			return
		}
		require.True(t, exists, "in set: %v, set: %v", string(p.AsBytes()), set)

		for i := 0; i < len(metadata); i += 2 {
			assert.Equal(t, metadata[i+1], p.MetaGetStr(metadata[i]))
		}

		if pop {
			delete(set, string(p.AsBytes()))
		}
	}
}
