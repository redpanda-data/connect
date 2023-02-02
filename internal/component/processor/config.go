package processor

import (
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/docs"
)

// Config is the all encompassing configuration struct for all processor types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl.
type Config struct {
	Label        string             `json:"label" yaml:"label"`
	Type         string             `json:"type" yaml:"type"`
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
	InsertPart   InsertPartConfig   `json:"insert_part" yaml:"insert_part"`
	JMESPath     JMESPathConfig     `json:"jmespath" yaml:"jmespath"`
	JQ           JQConfig           `json:"jq" yaml:"jq"`
	JSONSchema   JSONSchemaConfig   `json:"json_schema" yaml:"json_schema"`
	Log          LogConfig          `json:"log" yaml:"log"`
	Metric       MetricConfig       `json:"metric" yaml:"metric"`
	MongoDB      MongoDBConfig      `json:"mongodb" yaml:"mongodb"`
	Noop         struct{}           `json:"noop" yaml:"noop"`
	Plugin       any                `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Parallel     ParallelConfig     `json:"parallel" yaml:"parallel"`
	ParseLog     ParseLogConfig     `json:"parse_log" yaml:"parse_log"`
	Protobuf     ProtobufConfig     `json:"protobuf" yaml:"protobuf"`
	RateLimit    RateLimitConfig    `json:"rate_limit" yaml:"rate_limit"`
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
// APIs. Examples can be found in: ./internal/impl.
func NewConfig() Config {
	return Config{
		Label:        "",
		Type:         "bounds_check",
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

// UnmarshalYAML ensures that when parsing configs that are in a slice the
// default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(docs.DeprecatedProvider, docs.TypeProcessor, value); err != nil {
		return docs.NewLintError(value.Line, docs.LintComponentMissing, err.Error())
	}

	if spec.Plugin {
		pluginNode, err := docs.GetPluginConfigYAML(aliased.Type, value)
		if err != nil {
			return docs.NewLintError(value.Line, docs.LintFailedRead, err.Error())
		}
		aliased.Plugin = &pluginNode
	} else {
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}
