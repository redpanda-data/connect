package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"

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
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/gofrs/uuid"
	"gopkg.in/yaml.v3"
)

// StreamBuilder provides methods for building a Benthos stream configuration.
// When parsing Benthos configs this builder follows the schema and field
// defaults of a standard Benthos configuration.
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

	producerChan chan types.Transaction
	producerID   string
	consumerFunc MessageHandlerFunc
	consumerID   string

	apiMut       manager.APIReg
	customLogger log.Modular
}

// NewStreamBuilder creates a new StreamBuilder.
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		http:      api.NewConfig(),
		buffer:    buffer.NewConfig(),
		resources: manager.NewResourceConfig(),
		metrics:   metrics.NewConfig(),
		logger:    log.NewConfig(),
	}
}

//------------------------------------------------------------------------------

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
// This method can only be called once per stream builder, and subsequent calls
// will return an error.
func (s *StreamBuilder) AddProducerFunc() (MessageHandlerFunc, error) {
	if s.producerChan != nil {
		return nil, errors.New("unable to add multiple producer funcs to a stream builder")
	}

	uuid, err := uuid.NewV4()
	if err != nil {
		return nil, fmt.Errorf("failed to generate a producer uuid: %w", err)
	}

	tChan := make(chan types.Transaction)
	s.producerChan = tChan
	s.producerID = uuid.String()

	conf := input.NewConfig()
	conf.Type = input.TypeInproc
	conf.Inproc = input.InprocConfig(s.producerID)
	s.inputs = append(s.inputs, conf)

	return func(ctx context.Context, m *Message) error {
		tmpMsg := message.New(nil)
		tmpMsg.Append(m.part)
		resChan := make(chan types.Response)
		select {
		case tChan <- types.NewTransaction(tmpMsg, resChan):
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case res := <-resChan:
			return res.Error()
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

// AddInputYAML parses an input YAML configuration and adds it to the builder.
// If more than one input configuration is added they will automatically be
// composed within a broker when the pipeline is built.
func (s *StreamBuilder) AddInputYAML(conf string) error {
	confBytes := []byte(conf)

	iconf := input.NewConfig()
	if err := yaml.Unmarshal(confBytes, &iconf); err != nil {
		return err
	}

	if err := lintYAMLComponent(confBytes, docs.TypeInput); err != nil {
		return err
	}
	s.inputs = append(s.inputs, iconf)
	return nil
}

// AddProcessorYAML parses a processor YAML configuration and adds it to the
// builder to be executed within the pipeline.processors section, after all
// prior added processor configs.
func (s *StreamBuilder) AddProcessorYAML(conf string) error {
	confBytes := []byte(conf)

	pconf := processor.NewConfig()
	if err := yaml.Unmarshal(confBytes, &pconf); err != nil {
		return err
	}

	if err := lintYAMLComponent(confBytes, docs.TypeProcessor); err != nil {
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
// This method can only be called once per stream builder, and subsequent calls
// will return an error.
func (s *StreamBuilder) AddConsumerFunc(fn MessageHandlerFunc) error {
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
	conf.Type = input.TypeInproc
	conf.Inproc = output.InprocConfig(s.consumerID)
	s.outputs = append(s.outputs, conf)

	return nil
}

// AddOutputYAML parses an output YAML configuration and adds it to the builder.
// If more than one output configuration is added they will automatically be
// composed within a fan out broker when the pipeline is built.
func (s *StreamBuilder) AddOutputYAML(conf string) error {
	confBytes := []byte(conf)

	oconf := output.NewConfig()
	if err := yaml.Unmarshal(confBytes, &oconf); err != nil {
		return err
	}

	if err := lintYAMLComponent(confBytes, docs.TypeOutput); err != nil {
		return err
	}
	s.outputs = append(s.outputs, oconf)
	return nil
}

// AddCacheYAML parses a cache YAML configuration and adds it to the builder as
// a resource.
func (s *StreamBuilder) AddCacheYAML(conf string) error {
	confBytes := []byte(conf)

	cconf := cache.NewConfig()
	if err := yaml.Unmarshal(confBytes, &cconf); err != nil {
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

	if err := lintYAMLComponent(confBytes, docs.TypeCache); err != nil {
		return err
	}
	s.resources.ResourceCaches = append(s.resources.ResourceCaches, cconf)
	return nil
}

// AddRateLimitYAML parses a rate limit YAML configuration and adds it to the
// builder as a resource.
func (s *StreamBuilder) AddRateLimitYAML(conf string) error {
	confBytes := []byte(conf)

	rconf := ratelimit.NewConfig()
	if err := yaml.Unmarshal(confBytes, &rconf); err != nil {
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

	if err := lintYAMLComponent(confBytes, docs.TypeRateLimit); err != nil {
		return err
	}
	s.resources.ResourceRateLimits = append(s.resources.ResourceRateLimits, rconf)
	return nil
}

// AddResourcesYAML parses resource configurations and adds them to the config.
func (s *StreamBuilder) AddResourcesYAML(conf string) error {
	confBytes := []byte(conf)

	rconf := manager.NewResourceConfig()
	if err := yaml.Unmarshal(confBytes, &rconf); err != nil {
		return err
	}

	node, err := getYAMLNode(confBytes)
	if err != nil {
		return err
	}

	if err := lintsToErr(manager.Spec().LintYAML(docs.NewLintContext(), node)); err != nil {
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

	confBytes := []byte(conf)

	sconf := config.New()
	if err := yaml.Unmarshal(confBytes, &sconf); err != nil {
		return err
	}

	node, err := getYAMLNode(confBytes)
	if err != nil {
		return err
	}

	if err := lintsToErr(config.Spec().LintYAML(docs.NewLintContext(), node)); err != nil {
		return err
	}

	s.http = sconf.HTTP
	s.inputs = []input.Config{sconf.Input}
	s.buffer = sconf.Buffer
	s.processors = sconf.Pipeline.Processors
	s.threads = sconf.Pipeline.Threads
	s.outputs = []output.Config{sconf.Output}
	s.resources = sconf.ResourceConfig
	s.logger = sconf.Logger
	s.metrics = sconf.Metrics
	return nil
}

// SetMetricsYAML parses a metrics YAML configuration and adds it to the builder
// such that all stream components emit metrics through it.
func (s *StreamBuilder) SetMetricsYAML(conf string) error {
	confBytes := []byte(conf)

	mconf := metrics.NewConfig()
	if err := yaml.Unmarshal(confBytes, &mconf); err != nil {
		return err
	}

	if err := lintYAMLComponent(confBytes, docs.TypeMetrics); err != nil {
		return err
	}
	s.metrics = mconf
	return nil
}

// SetLoggerYAML parses a logger YAML configuration and adds it to the builder
// such that all stream components emit logs through it.
func (s *StreamBuilder) SetLoggerYAML(conf string) error {
	confBytes := []byte(conf)

	lconf := log.NewConfig()
	if err := yaml.Unmarshal(confBytes, &lconf); err != nil {
		return err
	}

	node, err := getYAMLNode(confBytes)
	if err != nil {
		return err
	}

	if err := lintsToErr(log.Spec().LintYAML(docs.NewLintContext(), node)); err != nil {
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
			err = tran.Payload.Iter(func(i int, part types.Part) error {
				return s.consumerFunc(context.Background(), newMessageFromPart(part))
			})
			var res types.Response
			if err != nil {
				res = response.NewError(err)
			} else {
				res = response.NewAck()
			}
			tran.ResponseChan <- res
		}
	}()
	return nil
}

// Build a Benthos stream pipeline according to the components specified by this
// stream builder.
func (s *StreamBuilder) Build() (*Stream, error) {
	conf := s.buildConfig()

	logger := s.customLogger
	if logger == nil {
		var err error
		if logger, err = log.NewV2(os.Stdout, s.logger); err != nil {
			return nil, err
		}
	}

	stats, err := metrics.New(s.metrics, metrics.OptSetLogger(logger))
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
			})
		}
		if apiMut, err = api.New("", "", s.http, sanitNode, logger, stats); err != nil {
			return nil, fmt.Errorf("unable to create stream HTTP server due to: %w. Tip: you can disable the server with `http.enabled` set to `false`, or override the configured server with SetHTTPMux", err)
		}
	}

	if wHandlerFunc, ok := stats.(metrics.WithHandlerFunc); ok {
		apiMut.RegisterEndpoint(
			"/stats", "Returns service metrics.",
			wHandlerFunc.HandlerFunc(),
		)
		apiMut.RegisterEndpoint(
			"/metrics", "Returns service metrics.",
			wHandlerFunc.HandlerFunc(),
		)
	}

	mgr, err := manager.NewV2(conf.ResourceConfig, apiMut, logger, stats)
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
	var nconf yaml.Node
	if err := yaml.Unmarshal(b, &nconf); err != nil {
		return nil, err
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

func lintYAMLComponent(b []byte, ctype docs.Type) error {
	nconf, err := getYAMLNode(b)
	if err != nil {
		return err
	}
	return lintsToErr(docs.LintYAML(docs.NewLintContext(), ctype, nconf))
}
