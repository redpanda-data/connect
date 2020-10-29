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

// TypeSpec Constructor and a usage description for each processor type.
type TypeSpec struct {
	constructor func(
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
	) (Type, error)
	sanitiseConfigFunc func(conf Config) (interface{}, error)

	// UsesBatches indicates whether this processors functionality is best
	// applied on messages that are already batched.
	UsesBatches bool

	Status      docs.Status
	Summary     string
	Description string
	Categories  []Category
	Footnotes   string
	FieldSpecs  docs.FieldSpecs
	Examples    []docs.AnnotatedExample
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
const (
	TypeArchive      = "archive"
	TypeAvro         = "avro"
	TypeAWK          = "awk"
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
type Config struct {
	Type         string             `json:"type" yaml:"type"`
	Archive      ArchiveConfig      `json:"archive" yaml:"archive"`
	Avro         AvroConfig         `json:"avro" yaml:"avro"`
	AWK          AWKConfig          `json:"awk" yaml:"awk"`
	Batch        BatchConfig        `json:"batch" yaml:"batch"`
	Bloblang     BloblangConfig     `json:"bloblang" yaml:"bloblang"`
	BoundsCheck  BoundsCheckConfig  `json:"bounds_check" yaml:"bounds_check"`
	Branch       BranchConfig       `json:"branch" yaml:"branch"`
	Cache        CacheConfig        `json:"cache" yaml:"cache"`
	Catch        CatchConfig        `json:"catch" yaml:"catch"`
	Compress     CompressConfig     `json:"compress" yaml:"compress"`
	Conditional  ConditionalConfig  `json:"conditional" yaml:"conditional"`
	Decode       DecodeConfig       `json:"decode" yaml:"decode"`
	Decompress   DecompressConfig   `json:"decompress" yaml:"decompress"`
	Dedupe       DedupeConfig       `json:"dedupe" yaml:"dedupe"`
	Encode       EncodeConfig       `json:"encode" yaml:"encode"`
	Filter       FilterConfig       `json:"filter" yaml:"filter"`
	FilterParts  FilterPartsConfig  `json:"filter_parts" yaml:"filter_parts"`
	ForEach      ForEachConfig      `json:"for_each" yaml:"for_each"`
	Grok         GrokConfig         `json:"grok" yaml:"grok"`
	GroupBy      GroupByConfig      `json:"group_by" yaml:"group_by"`
	GroupByValue GroupByValueConfig `json:"group_by_value" yaml:"group_by_value"`
	Hash         HashConfig         `json:"hash" yaml:"hash"`
	HashSample   HashSampleConfig   `json:"hash_sample" yaml:"hash_sample"`
	HTTP         HTTPConfig         `json:"http" yaml:"http"`
	InsertPart   InsertPartConfig   `json:"insert_part" yaml:"insert_part"`
	JMESPath     JMESPathConfig     `json:"jmespath" yaml:"jmespath"`
	JQ           JQConfig           `json:"jq" yaml:"jq"`
	JSON         JSONConfig         `json:"json" yaml:"json"`
	JSONSchema   JSONSchemaConfig   `json:"json_schema" yaml:"json_schema"`
	Lambda       LambdaConfig       `json:"lambda" yaml:"lambda"`
	Log          LogConfig          `json:"log" yaml:"log"`
	MergeJSON    MergeJSONConfig    `json:"merge_json" yaml:"merge_json"`
	Metadata     MetadataConfig     `json:"metadata" yaml:"metadata"`
	Metric       MetricConfig       `json:"metric" yaml:"metric"`
	Number       NumberConfig       `json:"number" yaml:"number"`
	Plugin       interface{}        `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Parallel     ParallelConfig     `json:"parallel" yaml:"parallel"`
	ParseLog     ParseLogConfig     `json:"parse_log" yaml:"parse_log"`
	ProcessBatch ForEachConfig      `json:"process_batch" yaml:"process_batch"`
	ProcessDAG   ProcessDAGConfig   `json:"process_dag" yaml:"process_dag"`
	ProcessField ProcessFieldConfig `json:"process_field" yaml:"process_field"`
	ProcessMap   ProcessMapConfig   `json:"process_map" yaml:"process_map"`
	Protobuf     ProtobufConfig     `json:"protobuf" yaml:"protobuf"`
	RateLimit    RateLimitConfig    `json:"rate_limit" yaml:"rate_limit"`
	Redis        RedisConfig        `json:"redis" yaml:"redis"`
	Resource     string             `json:"resource" yaml:"resource"`
	Sample       SampleConfig       `json:"sample" yaml:"sample"`
	SelectParts  SelectPartsConfig  `json:"select_parts" yaml:"select_parts"`
	Sleep        SleepConfig        `json:"sleep" yaml:"sleep"`
	Split        SplitConfig        `json:"split" yaml:"split"`
	SQL          SQLConfig          `json:"sql" yaml:"sql"`
	Subprocess   SubprocessConfig   `json:"subprocess" yaml:"subprocess"`
	Switch       SwitchConfig       `json:"switch" yaml:"switch"`
	SyncResponse SyncResponseConfig `json:"sync_response" yaml:"sync_response"`
	Text         TextConfig         `json:"text" yaml:"text"`
	Try          TryConfig          `json:"try" yaml:"try"`
	Throttle     ThrottleConfig     `json:"throttle" yaml:"throttle"`
	Unarchive    UnarchiveConfig    `json:"unarchive" yaml:"unarchive"`
	While        WhileConfig        `json:"while" yaml:"while"`
	Workflow     WorkflowConfig     `json:"workflow" yaml:"workflow"`
	XML          XMLConfig          `json:"xml" yaml:"xml"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:         "bounds_check",
		Archive:      NewArchiveConfig(),
		Avro:         NewAvroConfig(),
		AWK:          NewAWKConfig(),
		Batch:        NewBatchConfig(),
		Bloblang:     NewBloblangConfig(),
		BoundsCheck:  NewBoundsCheckConfig(),
		Branch:       NewBranchConfig(),
		Cache:        NewCacheConfig(),
		Catch:        NewCatchConfig(),
		Compress:     NewCompressConfig(),
		Conditional:  NewConditionalConfig(),
		Decode:       NewDecodeConfig(),
		Decompress:   NewDecompressConfig(),
		Dedupe:       NewDedupeConfig(),
		Encode:       NewEncodeConfig(),
		Filter:       NewFilterConfig(),
		FilterParts:  NewFilterPartsConfig(),
		ForEach:      NewForEachConfig(),
		Grok:         NewGrokConfig(),
		GroupBy:      NewGroupByConfig(),
		GroupByValue: NewGroupByValueConfig(),
		Hash:         NewHashConfig(),
		HashSample:   NewHashSampleConfig(),
		HTTP:         NewHTTPConfig(),
		InsertPart:   NewInsertPartConfig(),
		JMESPath:     NewJMESPathConfig(),
		JQ:           NewJQConfig(),
		JSON:         NewJSONConfig(),
		JSONSchema:   NewJSONSchemaConfig(),
		Lambda:       NewLambdaConfig(),
		Log:          NewLogConfig(),
		MergeJSON:    NewMergeJSONConfig(),
		Metadata:     NewMetadataConfig(),
		Metric:       NewMetricConfig(),
		Number:       NewNumberConfig(),
		Plugin:       nil,
		Parallel:     NewParallelConfig(),
		ParseLog:     NewParseLogConfig(),
		ProcessBatch: NewForEachConfig(),
		ProcessDAG:   NewProcessDAGConfig(),
		ProcessField: NewProcessFieldConfig(),
		ProcessMap:   NewProcessMapConfig(),
		Protobuf:     NewProtobufConfig(),
		RateLimit:    NewRateLimitConfig(),
		Redis:        NewRedisConfig(),
		Resource:     "",
		Sample:       NewSampleConfig(),
		SelectParts:  NewSelectPartsConfig(),
		Sleep:        NewSleepConfig(),
		Split:        NewSplitConfig(),
		SQL:          NewSQLConfig(),
		Subprocess:   NewSubprocessConfig(),
		Switch:       NewSwitchConfig(),
		SyncResponse: NewSyncResponseConfig(),
		Text:         NewTextConfig(),
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
	if sfunc := Constructors[conf.Type].sanitiseConfigFunc; sfunc != nil {
		if outputMap[conf.Type], err = sfunc(conf); err != nil {
			return nil, err
		}
	}
	if spec, exists := pluginSpecs[conf.Type]; exists {
		if spec.confSanitiser != nil {
			outputMap["plugin"] = spec.confSanitiser(conf.Plugin)
		}
	}
	if removeDeprecated {
		Constructors[conf.Type].FieldSpecs.RemoveDeprecated(outputMap)
	}
	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := value.Decode(&aliased); err != nil {
		if strings.HasPrefix(err.Error(), "line ") {
			return err
		}
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var raw interface{}
	if err := value.Decode(&raw); err != nil {
		if strings.HasPrefix(err.Error(), "line ") {
			return err
		}
		return fmt.Errorf("line %v: %v", value.Line, err)
	}
	if typeCandidates := config.GetInferenceCandidates(raw); len(typeCandidates) > 0 {
		var inferredType string
		for _, tc := range typeCandidates {
			if _, exists := Constructors[tc]; exists {
				if len(inferredType) > 0 {
					return fmt.Errorf("line %v: unable to infer type, multiple candidates '%v' and '%v'", value.Line, inferredType, tc)
				}
				inferredType = tc
			}
		}
		if len(inferredType) == 0 {
			return fmt.Errorf("line %v: unable to infer type, candidates were: %v", value.Line, typeCandidates)
		}
		aliased.Type = inferredType
	}

	if spec, exists := pluginSpecs[aliased.Type]; exists && spec.confConstructor != nil {
		confBytes, err := yaml.Marshal(aliased.Plugin)
		if err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}

		conf := spec.confConstructor()
		if err = yaml.Unmarshal(confBytes, conf); err != nil {
			return fmt.Errorf("line %v: %v", value.Line, err)
		}
		aliased.Plugin = conf
	} else {
		if !exists {
			if _, exists = Constructors[aliased.Type]; !exists {
				return fmt.Errorf("line %v: processor type '%v' was not recognised", value.Line, aliased.Type)
			}
		}
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
	if c, ok := Constructors[conf.Type]; ok {
		return c.constructor(conf, mgr, log, stats)
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		return c.constructor(conf.Plugin, mgr, log, stats)
	}
	return nil, types.ErrInvalidProcessorType
}

//------------------------------------------------------------------------------
