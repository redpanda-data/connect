package processor

import (
	"fmt"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

// Category describes the general purpose of a processor.
type Category string

// Processor categories
var (
	CategoryMapping     Category = "Mapping"
	CategoryParsing     Category = "Parsing"
	CategoryIntegration Category = "Integration"
	CategoryComposition Category = "Composition"
	CategoryUtility     Category = "Utility"
)

type procConstructor func(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error)

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
	Categories  []Category
	Footnotes   string
	config      docs.FieldSpec
	FieldSpecs  docs.FieldSpecs
	Examples    []docs.AnnotatedExample
}

// ConstructorFunc is a func signature able to construct a processor.
type ConstructorFunc func(Config, types.Manager, log.Modular, metrics.Type) (Type, error)

// WalkConstructors iterates each component constructor.
func WalkConstructors(fn func(ConstructorFunc, docs.ComponentSpec)) {
	inferred := docs.ComponentFieldsFromConf(NewConfig())
	for k, v := range Constructors {
		conf := v.config
		if len(v.FieldSpecs) > 0 {
			conf = docs.FieldComponent().WithChildren(v.FieldSpecs.DefaultAndTypeFrom(inferred[k])...)
		} else {
			conf.Children = conf.Children.DefaultAndTypeFrom(inferred[k])
		}
		spec := docs.ComponentSpec{
			Type:        docs.TypeProcessor,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Examples:    v.Examples,
			Footnotes:   v.Footnotes,
			Config:      conf,
			Status:      v.Status,
			Version:     v.Version,
		}
		if len(v.Categories) > 0 {
			spec.Categories = make([]string, 0, len(v.Categories))
			for _, cat := range v.Categories {
				spec.Categories = append(spec.Categories, string(cat))
			}
		}
		if v.UsesBatches {
			spec.Description = spec.Description + "\n" + DocsUsesBatches
		}
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all processor types with their specs.
var Constructors = map[string]TypeSpec{}

// Block replaces the constructor of a Benthos processor such that its
// construction will always return an error. This is useful for building strict
// pipelines where certain processors should not be available. NOTE: This does
// not remove the processor from the configuration spec, and normalisation will
// still work the same for blocked processors.
//
// EXPERIMENTAL: This function is experimental and therefore subject to change
// outside of major version releases.
func Block(typeStr, reason string) {
	ctor, ok := Constructors[typeStr]
	if !ok {
		return
	}
	ctor.constructor = func(
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
	) (Type, error) {
		return nil, fmt.Errorf("processor '%v' is blocked due to: %v", typeStr, reason)
	}
	Constructors[typeStr] = ctor
}

//------------------------------------------------------------------------------

// String constants representing each processor type.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
const (
	TypeArchive      = "archive"
	TypeAvro         = "avro"
	TypeAWK          = "awk"
	TypeAWSLambda    = "aws_lambda"
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
	TypeUnarchive    = "unarchive"
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
	Archive      ArchiveConfig      `json:"archive" yaml:"archive"`
	Avro         AvroConfig         `json:"avro" yaml:"avro"`
	AWK          AWKConfig          `json:"awk" yaml:"awk"`
	AWSLambda    LambdaConfig       `json:"aws_lambda" yaml:"aws_lambda"`
	Bloblang     BloblangConfig     `json:"bloblang" yaml:"bloblang"`
	BoundsCheck  BoundsCheckConfig  `json:"bounds_check" yaml:"bounds_check"`
	Branch       BranchConfig       `json:"branch" yaml:"branch"`
	Cache        CacheConfig        `json:"cache" yaml:"cache"`
	Catch        CatchConfig        `json:"catch" yaml:"catch"`
	Compress     CompressConfig     `json:"compress" yaml:"compress"`
	Decompress   DecompressConfig   `json:"decompress" yaml:"decompress"`
	Dedupe       DedupeConfig       `json:"dedupe" yaml:"dedupe"`
	ForEach      ForEachConfig      `json:"for_each" yaml:"for_each"`
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
	Noop         NoopConfig         `json:"noop" yaml:"noop"`
	Plugin       interface{}        `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Parallel     ParallelConfig     `json:"parallel" yaml:"parallel"`
	ParseLog     ParseLogConfig     `json:"parse_log" yaml:"parse_log"`
	ProcessBatch ForEachConfig      `json:"process_batch" yaml:"process_batch"`
	Protobuf     ProtobufConfig     `json:"protobuf" yaml:"protobuf"`
	RateLimit    RateLimitConfig    `json:"rate_limit" yaml:"rate_limit"`
	Redis        RedisConfig        `json:"redis" yaml:"redis"`
	Resource     string             `json:"resource" yaml:"resource"`
	SelectParts  SelectPartsConfig  `json:"select_parts" yaml:"select_parts"`
	Sleep        SleepConfig        `json:"sleep" yaml:"sleep"`
	Split        SplitConfig        `json:"split" yaml:"split"`
	Subprocess   SubprocessConfig   `json:"subprocess" yaml:"subprocess"`
	Switch       SwitchConfig       `json:"switch" yaml:"switch"`
	SyncResponse SyncResponseConfig `json:"sync_response" yaml:"sync_response"`
	Try          TryConfig          `json:"try" yaml:"try"`
	Throttle     ThrottleConfig     `json:"throttle" yaml:"throttle"`
	Unarchive    UnarchiveConfig    `json:"unarchive" yaml:"unarchive"`
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
		Archive:      NewArchiveConfig(),
		Avro:         NewAvroConfig(),
		AWK:          NewAWKConfig(),
		AWSLambda:    NewLambdaConfig(),
		Bloblang:     NewBloblangConfig(),
		BoundsCheck:  NewBoundsCheckConfig(),
		Branch:       NewBranchConfig(),
		Cache:        NewCacheConfig(),
		Catch:        NewCatchConfig(),
		Compress:     NewCompressConfig(),
		Decompress:   NewDecompressConfig(),
		Dedupe:       NewDedupeConfig(),
		ForEach:      NewForEachConfig(),
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
		Noop:         NewNoopConfig(),
		Plugin:       nil,
		Parallel:     NewParallelConfig(),
		ParseLog:     NewParseLogConfig(),
		ProcessBatch: NewForEachConfig(),
		Protobuf:     NewProtobufConfig(),
		RateLimit:    NewRateLimitConfig(),
		Redis:        NewRedisConfig(),
		Resource:     "",
		SelectParts:  NewSelectPartsConfig(),
		Sleep:        NewSleepConfig(),
		Split:        NewSplitConfig(),
		Subprocess:   NewSubprocessConfig(),
		Switch:       NewSwitchConfig(),
		SyncResponse: NewSyncResponseConfig(),
		Try:          NewTryConfig(),
		Throttle:     NewThrottleConfig(),
		Unarchive:    NewUnarchiveConfig(),
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
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeProcessor, aliased.Type, value); err != nil {
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
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	if mgrV2, ok := mgr.(interface {
		NewProcessor(conf Config) (types.Processor, error)
	}); ok {
		return mgrV2.NewProcessor(conf)
	}
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats)
	}
	return nil, types.ErrInvalidProcessorType
}
