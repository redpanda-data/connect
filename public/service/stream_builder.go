package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/Jeffail/benthos/v3/internal/bundle"
	"github.com/Jeffail/benthos/v3/internal/bundle/tracing"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/util/text"
	"github.com/Jeffail/gabs/v2"
	"github.com/gofrs/uuid"
	"gopkg.in/yaml.v3"
)

// StreamBuilder provides methods for building a Benthos stream configuration.
// When parsing Benthos configs this builder follows the schema and field
// defaults of a standard Benthos configuration. Environment variable
// interpolations are also parsed and resolved the same as regular configs.
//
// Benthos streams register HTTP endpoints by default that expose metrics and
// ready checks. If your intention is to execute multiple streams in the same
// process then it is recommended that you disable the HTTP server in config, or
// use `SetHTTPMux` with prefixed multiplexers in order to share it across the
// streams.
type StreamBuilder struct {
	http       api.Config
	threads    int
	inputs     []input.Config
	buffer     buffer.Config
	processors []processor.Config
	outputs    []output.Config
	resources  manager.ResourceConfig
	metrics    metrics.Config
	logger     log.Config

	producerChan chan message.Transaction
	producerID   string
	consumerFunc MessageBatchHandlerFunc
	consumerID   string

	apiMut       manager.APIReg
	customLogger log.Modular

	env             *Environment
	lintingDisabled bool
}

// NewStreamBuilder creates a new StreamBuilder.
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		http:      api.NewConfig(),
		buffer:    buffer.NewConfig(),
		resources: manager.NewResourceConfig(),
		metrics:   metrics.NewConfig(),
		logger:    log.NewConfig(),
		env:       globalEnvironment,
	}
}

func (s *StreamBuilder) getLintContext() docs.LintContext {
	ctx := docs.NewLintContext()
	ctx.DocsProvider = s.env.internal
	ctx.BloblangEnv = s.env.getBloblangParserEnv().Deactivated()
	return ctx
}

//------------------------------------------------------------------------------

// DisableLinting configures the stream builder to no longer lint YAML configs,
// allowing you to add snippets of config to the builder without failing on
// linting rules.
func (s *StreamBuilder) DisableLinting() {
	s.lintingDisabled = true
}

// SetThreads configures the number of pipeline processor threads should be
// configured. By default the number will be zero, which means the thread count
// will match the number of logical CPUs on the machine.
func (s *StreamBuilder) SetThreads(n int) {
	s.threads = n
}

// PrintLogger is a simple Print based interface implemented by custom loggers.
type PrintLogger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

// SetPrintLogger sets a custom logger supporting a simple Print based interface
// to be used by stream components. This custom logger will override any logging
// fields set via config.
func (s *StreamBuilder) SetPrintLogger(l PrintLogger) {
	s.customLogger = log.Wrap(l)
}

// HTTPMultiplexer is an interface supported by most HTTP multiplexers.
type HTTPMultiplexer interface {
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

type muxWrapper struct {
	m HTTPMultiplexer
}

func (w *muxWrapper) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	w.m.HandleFunc(path, h)
}

// SetHTTPMux sets an HTTP multiplexer to be used by stream components when
// registering endpoints instead of a new server spawned following the `http`
// fields of a Benthos config.
func (s *StreamBuilder) SetHTTPMux(m HTTPMultiplexer) {
	s.apiMut = &muxWrapper{m}
}

//------------------------------------------------------------------------------

// AddProducerFunc adds an input to the builder that allows you to write
// messages directly into the stream with a closure function. If any other input
// has or will be added to the stream builder they will be automatically
// composed within a broker when the pipeline is built.
//
// The returned MessageHandlerFunc can be called concurrently from any number of
// goroutines, and each call will block until the message is successfully
// delivered downstream, was rejected (or otherwise could not be delivered) or
// the context is cancelled.
//
// Only one producer func can be added to a stream builder, and subsequent calls
// will return an error.
func (s *StreamBuilder) AddProducerFunc() (MessageHandlerFunc, error) {
	if s.producerChan != nil {
		return nil, errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate a producer uuid: %w", err)
	}

	tChan := make(chan message.Transaction)
	s.producerChan = tChan
	s.producerID = uuid.String()

	conf := input.NewConfig()
	conf.Type = input.TypeInproc
	conf.Inproc = input.InprocConfig(s.producerID)
	s.inputs = append(s.inputs, conf)

	return func(ctx context.Context, m *Message) error {
		tmpMsg := message.QuickBatch(nil)
		tmpMsg.Append(m.part)
		resChan := make(chan response.Error)
		select {
		case tChan <- message.NewTransaction(tmpMsg, resChan):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case res := <-resChan:
			return res.AckError()
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

// AddBatchProducerFunc adds an input to the builder that allows you to write
// message batches directly into the stream with a closure function. If any
// other input has or will be added to the stream builder they will be
// automatically composed within a broker when the pipeline is built.
//
// The returned MessageBatchHandlerFunc can be called concurrently from any
// number of goroutines, and each call will block until all messages within the
// batch are successfully delivered downstream, were rejected (or otherwise
// could not be delivered) or the context is cancelled.
//
// Only one producer func can be added to a stream builder, and subsequent calls
// will return an error.
func (s *StreamBuilder) AddBatchProducerFunc() (MessageBatchHandlerFunc, error) {
	if s.producerChan != nil {
		return nil, errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate a producer uuid: %w", err)
	}

	tChan := make(chan message.Transaction)
	s.producerChan = tChan
	s.producerID = uuid.String()

	conf := input.NewConfig()
	conf.Type = input.TypeInproc
	conf.Inproc = input.InprocConfig(s.producerID)
	s.inputs = append(s.inputs, conf)

	return func(ctx context.Context, b MessageBatch) error {
		tmpMsg := message.QuickBatch(nil)
		for _, m := range b {
			tmpMsg.Append(m.part)
		}
		resChan := make(chan response.Error)
		select {
		case tChan <- message.NewTransaction(tmpMsg, resChan):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case res := <-resChan:
			return res.AckError()
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

// AddInputYAML parses an input YAML configuration and adds it to the builder.
// If more than one input configuration is added they will automatically be
// composed within a broker when the pipeline is built.
func (s *StreamBuilder) AddInputYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeInput); err != nil {
		return err
	}

	iconf := input.NewConfig()
	if err := nconf.Decode(&iconf); err != nil {
		return err
	}

	s.inputs = append(s.inputs, iconf)
	return nil
}

// AddProcessorYAML parses a processor YAML configuration and adds it to the
// builder to be executed within the pipeline.processors section, after all
// prior added processor configs.
func (s *StreamBuilder) AddProcessorYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeProcessor); err != nil {
		return err
	}

	pconf := processor.NewConfig()
	if err := nconf.Decode(&pconf); err != nil {
		return err
	}

	s.processors = append(s.processors, pconf)
	return nil
}

// AddConsumerFunc adds an output to the builder that executes a closure
// function argument for each message. If more than one output configuration is
// added they will automatically be composed within a fan out broker when the
// pipeline is built.
//
// The provided MessageHandlerFunc may be called from any number of goroutines,
// and therefore it is recommended to implement some form of throttling or mutex
// locking in cases where the call is non-blocking.
//
// Only one consumer can be added to a stream builder, and subsequent calls will
// return an error.
func (s *StreamBuilder) AddConsumerFunc(fn MessageHandlerFunc) error {
	if s.consumerFunc != nil {
		return errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("failed to generate a consumer uuid: %w", err)
	}

	s.consumerFunc = func(c context.Context, mb MessageBatch) error {
		for _, m := range mb {
			if err := fn(c, m); err != nil {
				return err
			}
		}
		return nil
	}
	s.consumerID = uuid.String()

	conf := output.NewConfig()
	conf.Type = output.TypeInproc
	conf.Inproc = output.InprocConfig(s.consumerID)
	s.outputs = append(s.outputs, conf)

	return nil
}

// AddBatchConsumerFunc adds an output to the builder that executes a closure
// function argument for each message batch. If more than one output
// configuration is added they will automatically be composed within a fan out
// broker when the pipeline is built.
//
// The provided MessageBatchHandlerFunc may be called from any number of
// goroutines, and therefore it is recommended to implement some form of
// throttling or mutex locking in cases where the call is non-blocking.
//
// Only one consumer can be added to a stream builder, and subsequent calls will
// return an error.
//
// Message batches must be created by upstream components (inputs, buffers, etc)
// otherwise message batches received by this consumer will have a single
// message contents.
func (s *StreamBuilder) AddBatchConsumerFunc(fn MessageBatchHandlerFunc) error {
	if s.consumerFunc != nil {
		return errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return fmt.Errorf("failed to generate a consumer uuid: %w", err)
	}

	s.consumerFunc = fn
	s.consumerID = uuid.String()

	conf := output.NewConfig()
	conf.Type = output.TypeInproc
	conf.Inproc = output.InprocConfig(s.consumerID)
	s.outputs = append(s.outputs, conf)

	return nil
}

// AddOutputYAML parses an output YAML configuration and adds it to the builder.
// If more than one output configuration is added they will automatically be
// composed within a fan out broker when the pipeline is built.
func (s *StreamBuilder) AddOutputYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeOutput); err != nil {
		return err
	}

	oconf := output.NewConfig()
	if err := nconf.Decode(&oconf); err != nil {
		return err
	}

	s.outputs = append(s.outputs, oconf)
	return nil
}

// AddCacheYAML parses a cache YAML configuration and adds it to the builder as
// a resource.
func (s *StreamBuilder) AddCacheYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeCache); err != nil {
		return err
	}

	cconf := cache.NewConfig()
	if err := nconf.Decode(&cconf); err != nil {
		return err
	}
	if cconf.Label == "" {
		return errors.New("a label must be specified for cache resources")
	}
	for _, cc := range s.resources.ResourceCaches {
		if cc.Label == cconf.Label {
			return fmt.Errorf("label %v collides with a previously defined resource", cc.Label)
		}
	}

	s.resources.ResourceCaches = append(s.resources.ResourceCaches, cconf)
	return nil
}

// AddRateLimitYAML parses a rate limit YAML configuration and adds it to the
// builder as a resource.
func (s *StreamBuilder) AddRateLimitYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeRateLimit); err != nil {
		return err
	}

	rconf := ratelimit.NewConfig()
	if err := nconf.Decode(&rconf); err != nil {
		return err
	}
	if rconf.Label == "" {
		return errors.New("a label must be specified for rate limit resources")
	}
	for _, rl := range s.resources.ResourceRateLimits {
		if rl.Label == rconf.Label {
			return fmt.Errorf("label %v collides with a previously defined resource", rl.Label)
		}
	}

	s.resources.ResourceRateLimits = append(s.resources.ResourceRateLimits, rconf)
	return nil
}

// AddResourcesYAML parses resource configurations and adds them to the config.
func (s *StreamBuilder) AddResourcesYAML(conf string) error {
	node, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLSpec(manager.Spec(), node); err != nil {
		return err
	}

	rconf := manager.NewResourceConfig()
	if err := node.Decode(&rconf); err != nil {
		return err
	}

	return s.resources.AddFrom(&rconf)
}

//------------------------------------------------------------------------------

// SetYAML parses a full Benthos config and uses it to configure the builder. If
// any inputs, processors, outputs, resources, etc, have previously been added
// to the builder they will be overridden by this new config.
func (s *StreamBuilder) SetYAML(conf string) error {
	if s.producerChan != nil {
		return errors.New("attempted to override inputs config after adding a func producer")
	}
	if s.consumerFunc != nil {
		return errors.New("attempted to override outputs config after adding a func consumer")
	}

	node, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLSpec(config.Spec(), node); err != nil {
		return err
	}

	sconf := config.New()
	if err := node.Decode(&sconf); err != nil {
		return err
	}

	s.setFromConfig(sconf)
	return nil
}

// SetFields modifies the config by setting one or more fields identified by a
// dot path to a value. The argument must be a variadic list of pairs, where the
// first element is a string containing the target field dot path, and the
// second element is a typed value to set the field to.
func (s *StreamBuilder) SetFields(pathValues ...interface{}) error {
	if s.producerChan != nil {
		return errors.New("attempted to override config after adding a func producer")
	}
	if s.consumerFunc != nil {
		return errors.New("attempted to override config after adding a func consumer")
	}
	if len(pathValues)%2 != 0 {
		return errors.New("invalid odd number of pathValues provided")
	}

	var rootNode yaml.Node
	if err := rootNode.Encode(s.buildConfig()); err != nil {
		return err
	}

	if err := config.Spec().SanitiseYAML(&rootNode, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: false,
		DocsProvider:     s.env.internal,
	}); err != nil {
		return err
	}

	for i := 0; i < len(pathValues)-1; i += 2 {
		var valueNode yaml.Node
		if err := valueNode.Encode(pathValues[i+1]); err != nil {
			return err
		}
		pathString, ok := pathValues[i].(string)
		if !ok {
			return fmt.Errorf("variadic pair element %v should be a string, got a %T", i, pathValues[i])
		}
		if err := config.Spec().SetYAMLPath(s.env.internal, &rootNode, &valueNode, gabs.DotPathToSlice(pathString)...); err != nil {
			return err
		}
	}

	if err := s.lintYAMLSpec(config.Spec(), &rootNode); err != nil {
		return err
	}

	sconf := config.New()
	if err := rootNode.Decode(&sconf); err != nil {
		return err
	}

	s.setFromConfig(sconf)
	return nil
}

func (s *StreamBuilder) setFromConfig(sconf config.Type) {
	s.http = sconf.HTTP
	s.inputs = []input.Config{sconf.Input}
	s.buffer = sconf.Buffer
	s.processors = sconf.Pipeline.Processors
	s.threads = sconf.Pipeline.Threads
	s.outputs = []output.Config{sconf.Output}
	s.resources = sconf.ResourceConfig
	s.logger = sconf.Logger
	s.metrics = sconf.Metrics
}

// SetBufferYAML parses a buffer YAML configuration and sets it to the builder
// to be placed between the input and the pipeline (processors) sections. This
// config will replace any prior configured buffer.
func (s *StreamBuilder) SetBufferYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeBuffer); err != nil {
		return err
	}

	bconf := buffer.NewConfig()
	if err := nconf.Decode(&bconf); err != nil {
		return err
	}

	s.buffer = bconf
	return nil
}

// SetMetricsYAML parses a metrics YAML configuration and adds it to the builder
// such that all stream components emit metrics through it.
func (s *StreamBuilder) SetMetricsYAML(conf string) error {
	nconf, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLComponent(nconf, docs.TypeMetrics); err != nil {
		return err
	}

	mconf := metrics.NewConfig()
	if err := nconf.Decode(&mconf); err != nil {
		return err
	}

	s.metrics = mconf
	return nil
}

// SetLoggerYAML parses a logger YAML configuration and adds it to the builder
// such that all stream components emit logs through it.
func (s *StreamBuilder) SetLoggerYAML(conf string) error {
	node, err := getYAMLNode([]byte(conf))
	if err != nil {
		return err
	}

	if err := s.lintYAMLSpec(log.Spec(), node); err != nil {
		return err
	}

	lconf := log.NewConfig()
	if err := node.Decode(&lconf); err != nil {
		return err
	}

	s.logger = lconf
	return nil
}

//------------------------------------------------------------------------------

// AsYAML prints a YAML representation of the stream config as it has been
// currently built.
func (s *StreamBuilder) AsYAML() (string, error) {
	conf := s.buildConfig()

	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return "", err
	}

	if err := config.Spec().SanitiseYAML(&node, docs.SanitiseConfig{
		RemoveTypeField:  true,
		RemoveDeprecated: false,
		DocsProvider:     s.env.internal,
	}); err != nil {
		return "", err
	}

	b, err := yaml.Marshal(node)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

//------------------------------------------------------------------------------

func (s *StreamBuilder) runConsumerFunc(mgr *manager.Type) error {
	if s.consumerFunc == nil {
		return nil
	}
	tChan, err := mgr.GetPipe(s.consumerID)
	if err != nil {
		return err
	}
	go func() {
		for {
			tran, open := <-tChan
			if !open {
				return
			}
			batch := make(MessageBatch, tran.Payload.Len())
			_ = tran.Payload.Iter(func(i int, part *message.Part) error {
				batch[i] = newMessageFromPart(part)
				return nil
			})
			err := s.consumerFunc(context.Background(), batch)
			var res response.Error
			if err != nil {
				res = response.NewError(err)
			} else {
				res = response.NewError(nil)
			}
			tran.ResponseChan <- res
		}
	}()
	return nil
}

// Build a Benthos stream pipeline according to the components specified by this
// stream builder.
func (s *StreamBuilder) Build() (*Stream, error) {
	return s.buildWithEnv(s.env.internal)
}

// BuildTraced creates a Benthos stream pipeline according to the components
// specified by this stream builder, where each major component (input,
// processor, output) is wrapped with a tracing module that, during the lifetime
// of the stream, aggregates tracing events into the returned *TracingSummary.
// Once the stream has ended the TracingSummary can be queried for events that
// occurred.
//
// Experimental: The behaviour of this method could change outside of major
// version releases.
func (s *StreamBuilder) BuildTraced() (*Stream, *TracingSummary, error) {
	tenv, summary := tracing.TracedBundle(s.env.internal)
	strm, err := s.buildWithEnv(tenv)
	return strm, &TracingSummary{summary}, err
}

func (s *StreamBuilder) buildWithEnv(env *bundle.Environment) (*Stream, error) {
	conf := s.buildConfig()

	logger := s.customLogger
	if logger == nil {
		var err error
		if logger, err = log.NewV2(os.Stdout, s.logger); err != nil {
			return nil, err
		}
	}

	stats, err := bundle.AllMetrics.Init(s.metrics, logger)
	if err != nil {
		return nil, err
	}

	apiMut := s.apiMut
	if apiMut == nil {
		var sanitNode yaml.Node
		err := sanitNode.Encode(conf)
		if err == nil {
			_ = config.Spec().SanitiseYAML(&sanitNode, docs.SanitiseConfig{
				RemoveTypeField: true,
				DocsProvider:    env,
			})
		}
		if apiMut, err = api.New("", "", s.http, sanitNode, logger, stats); err != nil {
			return nil, fmt.Errorf("unable to create stream HTTP server due to: %w. Tip: you can disable the server with `http.enabled` set to `false`, or override the configured server with SetHTTPMux", err)
		}
	} else if hler := stats.HandlerFunc(); hler != nil {
		apiMut.RegisterEndpoint("/stats", "Exposes service-wide metrics in the format configured.", hler)
		apiMut.RegisterEndpoint("/metrics", "Exposes service-wide metrics in the format configured.", hler)
	}

	mgr, err := manager.NewV2(
		conf.ResourceConfig, apiMut, logger, stats,
		manager.OptSetEnvironment(env),
		manager.OptSetBloblangEnvironment(s.env.getBloblangParserEnv()),
	)
	if err != nil {
		return nil, err
	}

	if s.producerChan != nil {
		mgr.SetPipe(s.producerID, s.producerChan)
	}

	return newStream(conf.Config, mgr, stats, logger, func() {
		if err := s.runConsumerFunc(mgr); err != nil {
			logger.Errorf("Failed to run func consumer: %v", err)
		}
	}), nil
}

type builderConfig struct {
	HTTP                   *api.Config `yaml:"http,omitempty"`
	stream.Config          `yaml:",inline"`
	manager.ResourceConfig `yaml:",inline"`
	Metrics                metrics.Config `yaml:"metrics"`
	Logger                 *log.Config    `yaml:"logger,omitempty"`
}

func (s *StreamBuilder) buildConfig() builderConfig {
	conf := builderConfig{
		Config: stream.NewConfig(),
	}

	if s.apiMut == nil {
		conf.HTTP = &s.http
	}

	if len(s.inputs) == 1 {
		conf.Input = s.inputs[0]
	} else if len(s.inputs) > 1 {
		conf.Input.Type = input.TypeBroker
		conf.Input.Broker.Inputs = s.inputs
	}

	conf.Buffer = s.buffer

	conf.Pipeline.Threads = s.threads
	conf.Pipeline.Processors = s.processors

	if len(s.outputs) == 1 {
		conf.Output = s.outputs[0]
	} else if len(s.outputs) > 1 {
		conf.Output.Type = output.TypeBroker
		conf.Output.Broker.Outputs = s.outputs
	}

	conf.ResourceConfig = s.resources
	conf.Metrics = s.metrics
	if s.customLogger == nil {
		conf.Logger = &s.logger
	}
	return conf
}

//------------------------------------------------------------------------------

func getYAMLNode(b []byte) (*yaml.Node, error) {
	b = text.ReplaceEnvVariables(b)
	var nconf yaml.Node
	if err := yaml.Unmarshal(b, &nconf); err != nil {
		return nil, err
	}
	if nconf.Kind == yaml.DocumentNode && len(nconf.Content) > 0 {
		return nconf.Content[0], nil
	}
	return &nconf, nil
}

// Lint represents a configuration file linting error.
type Lint struct {
	Line int
	What string
}

// LintError is an error type that represents one or more configuration file
// linting errors that were encountered.
type LintError []Lint

// Error returns an error string.
func (e LintError) Error() string {
	var lintsCollapsed bytes.Buffer
	for i, l := range e {
		if i > 0 {
			lintsCollapsed.WriteString("\n")
		}
		fmt.Fprintf(&lintsCollapsed, "line %v: %v", l.Line, l.What)
	}
	return fmt.Sprintf("lint errors: %v", lintsCollapsed.String())
}

func lintsToErr(lints []docs.Lint) error {
	if len(lints) == 0 {
		return nil
	}
	var e LintError
	for _, l := range lints {
		e = append(e, Lint{Line: l.Line, What: l.What})
	}
	return e
}

func (s *StreamBuilder) lintYAMLSpec(spec docs.FieldSpecs, node *yaml.Node) error {
	if s.lintingDisabled {
		return nil
	}
	return lintsToErr(spec.LintYAML(s.getLintContext(), node))
}

func (s *StreamBuilder) lintYAMLComponent(node *yaml.Node, ctype docs.Type) error {
	if s.lintingDisabled {
		return nil
	}
	return lintsToErr(docs.LintYAML(s.getLintContext(), ctype, node))
}
