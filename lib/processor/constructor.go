// Copyright (c) 2017 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package processor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

// TypeSpec Constructor and a usage description for each processor type.
type TypeSpec struct {
	constructor func(
		conf Config,
		mgr types.Manager,
		log log.Modular,
		stats metrics.Type,
	) (Type, error)
	description        string
	sanitiseConfigFunc func(conf Config) (interface{}, error)
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
	TypeBoundsCheck  = "bounds_check"
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
	TypeProcessBatch = "process_batch"
	TypeProcessDAG   = "process_dag"
	TypeProcessField = "process_field"
	TypeProcessMap   = "process_map"
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
	BoundsCheck  BoundsCheckConfig  `json:"bounds_check" yaml:"bounds_check"`
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
	ProcessBatch ForEachConfig      `json:"process_batch" yaml:"process_batch"`
	ProcessDAG   ProcessDAGConfig   `json:"process_dag" yaml:"process_dag"`
	ProcessField ProcessFieldConfig `json:"process_field" yaml:"process_field"`
	ProcessMap   ProcessMapConfig   `json:"process_map" yaml:"process_map"`
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
		BoundsCheck:  NewBoundsCheckConfig(),
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
		ProcessBatch: NewForEachConfig(),
		ProcessDAG:   NewProcessDAGConfig(),
		ProcessField: NewProcessFieldConfig(),
		ProcessMap:   NewProcessMapConfig(),
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
	cBytes, err := json.Marshal(conf)
	if err != nil {
		return nil, err
	}

	hashMap := map[string]interface{}{}
	if err = json.Unmarshal(cBytes, &hashMap); err != nil {
		return nil, err
	}

	outputMap := config.Sanitised{}
	outputMap["type"] = conf.Type
	if sfunc := Constructors[conf.Type].sanitiseConfigFunc; sfunc != nil {
		if outputMap[conf.Type], err = sfunc(conf); err != nil {
			return nil, err
		}
	} else {
		if _, exists := hashMap[conf.Type]; exists {
			outputMap[conf.Type] = hashMap[conf.Type]
		}
		if spec, exists := pluginSpecs[conf.Type]; exists {
			var plugSanit interface{}
			if spec.confSanitiser != nil {
				plugSanit = spec.confSanitiser(conf.Plugin)
			} else {
				plugSanit = hashMap["plugin"]
			}
			if plugSanit != nil {
				outputMap["plugin"] = plugSanit
			}
		}
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (m *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	if err := value.Decode(&aliased); err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var raw interface{}
	if err := value.Decode(&raw); err != nil {
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
		aliased.Plugin = nil
	}

	*m = Config(aliased)
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
  - type: foo
    foo:
      bar: baz
` + "```" + `

The ` + "`threads`" + ` field in the pipeline section determines how many
parallel processing threads are created. You can read more about parallel
processing in the [pipeline guide][1].

By organising processors you can configure complex behaviours in your pipeline.
You can [find some examples here][0].

### Error Handling

Some processors have conditions whereby they might fail. Benthos has mechanisms
for detecting and recovering from these failures which can be read about
[here](../error_handling.md).

### Batching and Multiple Part Messages

All Benthos processors support multiple part messages, which are synonymous with
batches. Some processors such as [batch](#batch) and [split](#split) are able to
create, expand and break down batches.

Many processors are able to perform their behaviours on specific parts of a
message batch, or on all parts, and have a field ` + "`parts`" + ` for
specifying an array of part indexes they should apply to. If the list of target
parts is empty these processors will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the last
element will be selected, and so on.

Some processors such as ` + "`filter` and `dedupe`" + ` act across an entire
batch, when instead we'd like to perform them on individual messages of a batch.
In this case the ` + "[`for_each`](#for_each)" + ` processor can be used.`

var footer = `
[0]: ../examples/README.md
[1]: ../pipeline.md`

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
	for i, name := range names {
		buf.WriteString(fmt.Sprintf("%v. [`%v`](#%v)\n", i+1, name, name))
	}
	buf.WriteString("\n")

	// Append each description
	for i, name := range names {
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
		buf.WriteString(Constructors[name].description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n")
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
