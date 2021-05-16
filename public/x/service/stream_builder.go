package service

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/buffer"
	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/stream"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

// StreamBuilder provides methods for building a Benthos stream configuration.
type StreamBuilder struct {
	threads    int
	inputs     []input.Config
	buffer     buffer.Config
	processors []processor.Config
	outputs    []output.Config
	resources  manager.ResourceConfig
	metrics    metrics.Config
	logger     log.Config

	apiMut       manager.APIReg
	customLogger log.Modular
}

// NewStreamBuilder creates a new StreamBuilder.
func NewStreamBuilder() *StreamBuilder {
	return &StreamBuilder{
		buffer:    buffer.NewConfig(),
		resources: manager.NewResourceConfig(),
		metrics:   metrics.NewConfig(),
		logger:    log.NewConfig(),
		apiMut:    types.NoopMgr(),
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
// registering endpoints.
func (s *StreamBuilder) SetHTTPMux(m HTTPMultiplexer) {
	s.apiMut = &muxWrapper{m}
}

//------------------------------------------------------------------------------

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

	if err := lintsToErr(manager.Spec().LintNode(docs.NewLintContext(), node)); err != nil {
		return err
	}

	return s.resources.AddFrom(&rconf)
}

//------------------------------------------------------------------------------

// SetCoreYAML parses a config snippet containing input, buffer, pipeline and
// output sections and adds them to the builder. If any inputs, processors or
// outputs, etc, have previously been added to the builder they will be
// overridden by this new config, as well as the number of processor threads.
func (s *StreamBuilder) SetCoreYAML(conf string) error {
	confBytes := []byte(conf)

	sconf := stream.NewConfig()
	if err := yaml.Unmarshal(confBytes, &sconf); err != nil {
		return err
	}

	node, err := getYAMLNode(confBytes)
	if err != nil {
		return err
	}

	if err := lintsToErr(stream.Spec().LintNode(docs.NewLintContext(), node)); err != nil {
		return err
	}

	s.inputs = []input.Config{sconf.Input}
	s.buffer = sconf.Buffer
	s.processors = sconf.Pipeline.Processors
	s.threads = sconf.Pipeline.Threads
	s.outputs = []output.Config{sconf.Output}
	return nil
}

// SetMetricsYAML parses a metrics YAML configuration and adds it to the builder
// such that all stream components emit metrics through it.
//
// Any metrics type that emits metrics by a scraped HTTP endpoint will use the
// HTTP mux provided with SetHTTPMux.
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

	if err := lintsToErr(log.Spec().LintNode(docs.NewLintContext(), node)); err != nil {
		return err
	}

	s.logger = lconf
	return nil
}

// AsYAML prints a YAML representation of the stream config as it has been
// currently built.
func (s *StreamBuilder) AsYAML() (string, error) {
	conf := s.buildConfig()

	var node yaml.Node
	if err := node.Encode(conf); err != nil {
		return "", err
	}

	if err := config.Spec().SanitiseNode(&node, docs.SanitiseConfig{
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

// Build a stream that will immediately begin running in the background, the
// returned stream type can be used to check whether it's actively connected and
// begin shutting it down gracefully.
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

	if wHandlerFunc, ok := stats.(metrics.WithHandlerFunc); ok {
		s.apiMut.RegisterEndpoint(
			"/stats", "Returns service metrics.",
			wHandlerFunc.HandlerFunc(),
		)
		s.apiMut.RegisterEndpoint(
			"/metrics", "Returns service metrics.",
			wHandlerFunc.HandlerFunc(),
		)
	}

	mgr, err := manager.NewV2(conf.ResourceConfig, s.apiMut, logger, stats)
	if err != nil {
		return nil, err
	}

	return newStream(conf.Config, mgr, stats, logger), nil
}

type builderConfig struct {
	stream.Config          `yaml:",inline"`
	manager.ResourceConfig `yaml:",inline"`
	Metrics                metrics.Config `yaml:"metrics"`
	Logger                 *log.Config    `yaml:"logger,omitempty"`
}

func (s *StreamBuilder) buildConfig() builderConfig {
	conf := builderConfig{
		Config: stream.NewConfig(),
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

	if nconf.Kind == yaml.DocumentNode && nconf.Content[0].Kind == yaml.MappingNode {
		nconf = *nconf.Content[0]
	}

	return &nconf, nil
}

func lintsToErr(lints []docs.Lint) error {
	if len(lints) > 0 {
		var lintsCollapsed bytes.Buffer
		for i, l := range lints {
			if i > 0 {
				lintsCollapsed.WriteString("\n")
			}
			fmt.Fprintf(&lintsCollapsed, "line %v: %v", l.Line, l.What)
		}
		return fmt.Errorf("lint errors: %v", lintsCollapsed.String())
	}
	return nil
}

func lintYAMLComponent(b []byte, ctype docs.Type) error {
	nconf, err := getYAMLNode(b)
	if err != nil {
		return err
	}
	return lintsToErr(docs.LintNode(docs.NewLintContext(), ctype, nconf))
}
