package processor

import (
	"fmt"
	"strings"

	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
)

type procConstructor func(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
) (processor.V1, error)

// TypeSpec Constructor and a usage description for each processor type.
type TypeSpec struct {
	constructor procConstructor

	// UsesBatches indicates whether this processors functionality is best
	// applied on messages that are already batched.
	UsesBatches bool

	Status      docs.Status
	Version     string
	Summary     string
	Description string
	Categories  []string
	Footnotes   string
	Config      docs.FieldSpec
	Examples    []docs.AnnotatedExample
}

// ConstructorFunc is a func signature able to construct a processor.
type ConstructorFunc func(Config, interop.Manager, log.Modular, metrics.Type) (processor.V1, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		conf := v.Config
		conf.Children = conf.Children.DefaultAndTypeFrom(inferred[k])
		spec := docs.ComponentSpec{
			Type:        docs.TypeProcessor,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Categories:  v.Categories,
			Examples:    v.Examples,
			Footnotes:   v.Footnotes,
			Config:      conf,
			Status:      v.Status,
			Version:     v.Version,
		}
		if v.UsesBatches {
			spec.Description = spec.Description + "\n" + DocsUsesBatches
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all processor types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each processor type.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
const (
	TypeArchive      = "archive"
	TypeAvro         = "avro"
	TypeAWK          = "awk"
	TypeBloblang     = "bloblang"
	TypeBoundsCheck  = "bounds_check"
	TypeBranch       = "branch"
	TypeCache        = "cache"
	TypeCatch        = "catch"
	TypeCompress     = "compress"
	TypeDecompress   = "decompress"
	TypeDedupe       = "dedupe"
	TypeForEach      = "for_each"
	TypeGrok         = "grok"
	TypeGroupBy      = "group_by"
	TypeGroupByValue = "group_by_value"
	TypeHTTP         = "http"
	TypeInsertPart   = "insert_part"
	TypeJMESPath     = "jmespath"
	TypeJQ           = "jq"
	TypeJSONSchema   = "json_schema"
	TypeLog          = "log"
	TypeMetric       = "metric"
	TypeMongoDB      = "mongodb"
	TypeNoop         = "noop"
	TypeParallel     = "parallel"
	TypeParseLog     = "parse_log"
	TypeProtobuf     = "protobuf"
	TypeRateLimit    = "rate_limit"
	TypeRedis        = "redis"
	TypeResource     = "resource"
	TypeSelectParts  = "select_parts"
	TypeSleep        = "sleep"
	TypeSplit        = "split"
	TypeSubprocess   = "subprocess"
	TypeSwitch       = "switch"
	TypeSyncResponse = "sync_response"
	TypeTry          = "try"
	TypeThrottle     = "throttle"
	TypeWhile        = "while"
	TypeWorkflow     = "workflow"
	TypeXML          = "xml"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all processor types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Label        string             `json:"label" yaml:"label"`
	Type         string             `json:"type" yaml:"type"`
	Avro         AvroConfig         `json:"avro" yaml:"avro"`
	AWK          AWKConfig          `json:"awk" yaml:"awk"`
	Bloblang     string             `json:"bloblang" yaml:"bloblang"`
	BoundsCheck  BoundsCheckConfig  `json:"bounds_check" yaml:"bounds_check"`
	Branch       BranchConfig       `json:"branch" yaml:"branch"`
	Cache        CacheConfig        `json:"cache" yaml:"cache"`
	Catch        []Config           `json:"catch" yaml:"catch"`
	Compress     CompressConfig     `json:"compress" yaml:"compress"`
	Decompress   DecompressConfig   `json:"decompress" yaml:"decompress"`
	Dedupe       DedupeConfig       `json:"dedupe" yaml:"dedupe"`
	ForEach      []Config           `json:"for_each" yaml:"for_each"`
	Grok         GrokConfig         `json:"grok" yaml:"grok"`
	GroupBy      GroupByConfig      `json:"group_by" yaml:"group_by"`
	GroupByValue GroupByValueConfig `json:"group_by_value" yaml:"group_by_value"`
	HTTP         HTTPConfig         `json:"http" yaml:"http"`
	InsertPart   InsertPartConfig   `json:"insert_part" yaml:"insert_part"`
	JMESPath     JMESPathConfig     `json:"jmespath" yaml:"jmespath"`
	JQ           JQConfig           `json:"jq" yaml:"jq"`
	JSONSchema   JSONSchemaConfig   `json:"json_schema" yaml:"json_schema"`
	Log          LogConfig          `json:"log" yaml:"log"`
	Metric       MetricConfig       `json:"metric" yaml:"metric"`
	MongoDB      MongoDBConfig      `json:"mongodb" yaml:"mongodb"`
	Noop         struct{}           `json:"noop" yaml:"noop"`
	Plugin       interface{}        `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Parallel     ParallelConfig     `json:"parallel" yaml:"parallel"`
	ParseLog     ParseLogConfig     `json:"parse_log" yaml:"parse_log"`
	Protobuf     ProtobufConfig     `json:"protobuf" yaml:"protobuf"`
	RateLimit    RateLimitConfig    `json:"rate_limit" yaml:"rate_limit"`
	Redis        RedisConfig        `json:"redis" yaml:"redis"`
	Resource     string             `json:"resource" yaml:"resource"`
	SelectParts  SelectPartsConfig  `json:"select_parts" yaml:"select_parts"`
	Sleep        SleepConfig        `json:"sleep" yaml:"sleep"`
	Split        SplitConfig        `json:"split" yaml:"split"`
	Subprocess   SubprocessConfig   `json:"subprocess" yaml:"subprocess"`
	Switch       SwitchConfig       `json:"switch" yaml:"switch"`
	SyncResponse struct{}           `json:"sync_response" yaml:"sync_response"`
	Try          []Config           `json:"try" yaml:"try"`
	While        WhileConfig        `json:"while" yaml:"while"`
	Workflow     WorkflowConfig     `json:"workflow" yaml:"workflow"`
	XML          XMLConfig          `json:"xml" yaml:"xml"`
}

// NewConfig returns a configuration struct fully populated with default values.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
func NewConfig() Config {
	return Config{
		Label:        "",
		Type:         "bounds_check",
		Avro:         NewAvroConfig(),
		AWK:          NewAWKConfig(),
		Bloblang:     "",
		BoundsCheck:  NewBoundsCheckConfig(),
		Branch:       NewBranchConfig(),
		Cache:        NewCacheConfig(),
		Catch:        []Config{},
		Compress:     NewCompressConfig(),
		Decompress:   NewDecompressConfig(),
		Dedupe:       NewDedupeConfig(),
		ForEach:      []Config{},
		Grok:         NewGrokConfig(),
		GroupBy:      NewGroupByConfig(),
		GroupByValue: NewGroupByValueConfig(),
		HTTP:         NewHTTPConfig(),
		InsertPart:   NewInsertPartConfig(),
		JMESPath:     NewJMESPathConfig(),
		JQ:           NewJQConfig(),
		JSONSchema:   NewJSONSchemaConfig(),
		Log:          NewLogConfig(),
		Metric:       NewMetricConfig(),
		MongoDB:      NewMongoDBConfig(),
		Noop:         struct{}{},
		Plugin:       nil,
		Parallel:     NewParallelConfig(),
		ParseLog:     NewParseLogConfig(),
		Protobuf:     NewProtobufConfig(),
		RateLimit:    NewRateLimitConfig(),
		Redis:        NewRedisConfig(),
		Resource:     "",
		SelectParts:  NewSelectPartsConfig(),
		Sleep:        NewSleepConfig(),
		Split:        NewSplitConfig(),
		Subprocess:   NewSubprocessConfig(),
		Switch:       NewSwitchConfig(),
		SyncResponse: struct{}{},
		Try:          []Config{},
		While:        NewWhileConfig(),
		Workflow:     NewWorkflowConfig(),
		XML:          NewXMLConfig(),
	}
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		if strings.HasPrefix(err.Error(), "line ") {
			return err
		}
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeProcessor, value); err != nil {
		return fmt.Errorf("line %v: %w", value.Line, err)
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

// New creates a processor type based on a processor configuration.
func New(
	conf Config,
	mgr interop.Manager,
	log log.Modular,
	stats metrics.Type,
) (processor.V1, error) {
	if mgrV2, ok := mgr.(interface {
		NewProcessor(conf Config) (processor.V1, error)
	}); ok {
		return mgrV2.NewProcessor(conf)
	}
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats)
	}
	return nil, component.ErrInvalidType("processor", conf.Type)
}
