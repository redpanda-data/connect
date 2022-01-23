package processor

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

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
	for k, v := range pluginSpecs {
		spec := docs.ComponentSpec{
			Type:   docs.TypeProcessor,
			Name:   k,
			Status: docs.StatusExperimental,
			Plugin: true,
			Config: docs.FieldComponent().Unlinted(),
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
	TypeBatch        = "batch"
	TypeBloblang     = "bloblang"
	TypeBoundsCheck  = "bounds_check"
	TypeBranch       = "branch"
	TypeCache        = "cache"
	TypeCatch        = "catch"
	TypeCompress     = "compress"
	TypeConditional  = "conditional"
	TypeDecode       = "decode"
	TypeDecompress   = "decompress"
	TypeDedupe       = "dedupe"
	TypeEncode       = "encode"
	TypeFilter       = "filter"
	TypeFilterParts  = "filter_parts"
	TypeForEach      = "for_each"
	TypeGrok         = "grok"
	TypeGroupBy      = "group_by"
	TypeGroupByValue = "group_by_value"
	TypeHash         = "hash"
	TypeHashSample   = "hash_sample"
	TypeHTTP         = "http"
	TypeInsertPart   = "insert_part"
	TypeJMESPath     = "jmespath"
	TypeJQ           = "jq"
	TypeJSON         = "json"
	TypeJSONSchema   = "json_schema"
	TypeLambda       = "lambda"
	TypeLog          = "log"
	TypeMergeJSON    = "merge_json"
	TypeMetadata     = "metadata"
	TypeMetric       = "metric"
	TypeMongoDB      = "mongodb"
	TypeNoop         = "noop"
	TypeNumber       = "number"
	TypeParallel     = "parallel"
	TypeParseLog     = "parse_log"
	TypeProcessBatch = "process_batch"
	TypeProcessDAG   = "process_dag"
	TypeProcessField = "process_field"
	TypeProcessMap   = "process_map"
	TypeProtobuf     = "protobuf"
	TypeRateLimit    = "rate_limit"
	TypeRedis        = "redis"
	TypeResource     = "resource"
	TypeSample       = "sample"
	TypeSelectParts  = "select_parts"
	TypeSleep        = "sleep"
	TypeSplit        = "split"
	TypeSQL          = "sql"
	TypeSubprocess   = "subprocess"
	TypeSwitch       = "switch"
	TypeSyncResponse = "sync_response"
	TypeText         = "text"
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
	Lambda       LambdaConfig       `json:"lambda" yaml:"lambda"`
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
		Lambda:       NewLambdaConfig(),
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

// SanitiseConfig returns a sanitised version of the Config, meaning sections
// that aren't relevant to behaviour are removed.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
	outputMap, err := config.SanitizeComponent(conf)
	if err != nil {
		return nil, err
	}
	if spec, exists := pluginSpecs[conf.Type]; exists {
		if spec.confSanitiser != nil {
			outputMap["plugin"] = spec.confSanitiser(conf.Plugin)
		}
	}
	if err := docs.SanitiseComponentConfig(
		docs.TypeProcessor,
		map[string]interface{}(outputMap),
		docs.ShouldDropDeprecated(removeDeprecated),
	); err != nil {
		return nil, err
	}
	return outputMap, nil
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
		if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
			conf := spec.confConstructor()
			if err = pluginNode.Decode(conf); err != nil {
				return fmt.Errorf("line %v: %v", value.Line, err)
			}
			aliased.Plugin = conf
		} else {
			aliased.Plugin = &pluginNode
		}
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-processors`." + `

Benthos processors are functions applied to messages passing through a pipeline.
The function signature allows a processor to mutate or drop messages depending
on the content of the message.

Processors are set via config, and depending on where in the config they are
placed they will be run either immediately after a specific input (set in the
input section), on all messages (set in the pipeline section) or before a
specific output (set in the output section). Most processors apply to all
messages and can be placed in the pipeline section:

` + "``` yaml" + `
pipeline:
  threads: 1
  processors:
   - jmespath:
       query: '{ message: @, meta: { link_count: length(links) } }'
` + "```" + `

The ` + "`threads`" + ` field in the pipeline section determines how many
parallel processing threads are created. You can read more about parallel
processing in the [pipeline guide][1].

By organising processors you can configure complex behaviours in your pipeline.
You can [find some examples here][0].

### Error Handling

Some processors have conditions whereby they might fail. Benthos has mechanisms
for detecting and recovering from these failures which can be read about
[here](/docs/configuration/error_handling).

### Batching and Multiple Part Messages

All Benthos processors support multiple part messages, which are synonymous with
batches. Some processors such as [split](/docs/components/processors/split) are able to create, expand and
break down batches.

Many processors are able to perform their behaviours on specific parts of a
message batch, or on all parts, and have a field ` + "`parts`" + ` for
specifying an array of part indexes they should apply to. If the list of target
parts is empty these processors will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the last
element will be selected, and so on.

Some processors such as ` + "[`filter`](/docs/components/processors/filter) and [`dedupe`](/docs/components/processors/dedupe)" + `
act across an entire batch, when instead we'd like to perform them on individual
messages of a batch. In this case the ` + "[`for_each`](/docs/components/processors/for_each)" + `
processor can be used.`

var footer = `
[0]: /cookbooks
[1]: /docs/configuration/processing_pipelines`

// Descriptions returns a formatted string of collated descriptions of each
// type.
func Descriptions() string {
	// Order our buffer types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Processors\n")
	buf.WriteString(strings.Repeat("=", 10))
	buf.WriteString("\n\n")
	buf.WriteString(header)
	buf.WriteString("\n\n")

	buf.WriteString("### Contents\n\n")
	i := 0
	for _, name := range names {
		if Constructors[name].Status == docs.StatusDeprecated {
			continue
		}
		i++
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
		def := Constructors[name]
		if def.Status == docs.StatusDeprecated {
			continue
		}

		var confBytes []byte

		conf := NewConfig()
		conf.Type = name
		if confSanit, err := SanitiseConfig(conf); err == nil {
			confBytes, _ = config.MarshalYAML(confSanit)
		}

		buf.WriteString("## ")
		buf.WriteString("`" + name + "`")
		buf.WriteString("\n")
		if confBytes != nil {
			buf.WriteString("\n``` yaml\n")
			buf.Write(confBytes)
			buf.WriteString("```\n")
		}
		buf.WriteString(def.Description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n---\n")
		}
	}
	buf.WriteString(footer)
	return buf.String()
}

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
	if c, ok := pluginSpecs[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats)
	}
	return nil, types.ErrInvalidProcessorType
}

//------------------------------------------------------------------------------
