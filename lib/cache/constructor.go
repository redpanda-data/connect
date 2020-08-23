package cache

import (
	"bytes"
	"encoding/json"
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

// TypeSpec is a constructor and a usage description for each cache type.
type TypeSpec struct {
	constructor func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error)

	Summary     string
	Description string
	Footnotes   string
	FieldSpecs  docs.FieldSpecs
	Beta        bool
	Deprecated  bool
}

// Constructors is a map of all cache types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each cache type.
const (
	TypeDynamoDB   = "dynamodb"
	TypeFile       = "file"
	TypeMemcached  = "memcached"
	TypeMemory     = "memory"
	TypeMultilevel = "multilevel"
	TypeRedis      = "redis"
	TypeRistretto  = "ristretto"
	TypeS3         = "s3"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all cache types.
type Config struct {
	Type       string           `json:"type" yaml:"type"`
	DynamoDB   DynamoDBConfig   `json:"dynamodb" yaml:"dynamodb"`
	File       FileConfig       `json:"file" yaml:"file"`
	Memcached  MemcachedConfig  `json:"memcached" yaml:"memcached"`
	Memory     MemoryConfig     `json:"memory" yaml:"memory"`
	Multilevel MultilevelConfig `json:"multilevel" yaml:"multilevel"`
	Plugin     interface{}      `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Redis      RedisConfig      `json:"redis" yaml:"redis"`
	Ristretto  RistrettoConfig  `json:"ristretto" yaml:"ristretto"`
	S3         S3Config         `json:"s3" yaml:"s3"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Type:       "memory",
		DynamoDB:   NewDynamoDBConfig(),
		File:       NewFileConfig(),
		Memcached:  NewMemcachedConfig(),
		Memory:     NewMemoryConfig(),
		Multilevel: NewMultilevelConfig(),
		Plugin:     nil,
		Redis:      NewRedisConfig(),
		Ristretto:  NewRistrettoConfig(),
		S3:         NewS3Config(),
	}
}

//------------------------------------------------------------------------------

// SanitiseConfig creates a sanitised version of a config.
func SanitiseConfig(conf Config) (interface{}, error) {
	return conf.Sanitised(false)
}

// Sanitised returns a sanitised version of the config, meaning sections that
// aren't relevant to behaviour are removed. Also optionally removes deprecated
// fields.
func (conf Config) Sanitised(removeDeprecated bool) (interface{}, error) {
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

	t := conf.Type
	def := Constructors[t]

	if removeDeprecated {
		if m, ok := outputMap[t].(map[string]interface{}); ok {
			for _, spec := range def.FieldSpecs {
				if spec.Deprecated {
					delete(m, spec.Name)
				}
			}
		}
	}

	return outputMap, nil
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
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
		if !exists {
			if _, exists = Constructors[aliased.Type]; !exists {
				return fmt.Errorf("line %v: type '%v' was not recognised", value.Line, aliased.Type)
			}
		}
		aliased.Plugin = nil
	}

	*conf = Config(aliased)
	return nil
}

//------------------------------------------------------------------------------

var header = "This document was generated with `benthos --list-caches`" + `

A cache is a key/value store which can be used by certain processors for
applications such as deduplication. Caches are listed with unique labels which
are referred to by processors that may share them.

Caches are configured as resources:

` + "```yaml" + `
resources:
  caches:
    foobar:
      memcached:
        addresses:
          - localhost:11211
        ttl: 60
` + "```" + `

And any components that use caches have a field used to refer to a cache
resource:

` + "```yaml" + `
pipeline:
  processors:
    - dedupe:
        cache: foobar
        hash: xxhash
` + "```" + ``

// Descriptions returns a formatted string of descriptions for each type.
func Descriptions() string {
	// Order our cache types alphabetically
	names := []string{}
	for name := range Constructors {
		names = append(names, name)
	}
	sort.Strings(names)

	buf := bytes.Buffer{}
	buf.WriteString("Caches\n")
	buf.WriteString(strings.Repeat("=", 6))
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
		buf.WriteString(Constructors[name].Description)
		buf.WriteString("\n")
		if i != (len(names) - 1) {
			buf.WriteString("\n")
			buf.WriteString("---\n")
		}
	}
	return buf.String()
}

// New creates a cache type based on an cache configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (types.Cache, error) {
	if c, ok := Constructors[conf.Type]; ok {
		cache, err := c.constructor(conf, mgr, log.NewModule("."+conf.Type), stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache '%v': %v", conf.Type, err)
		}
		return cache, nil
	}
	if c, ok := pluginSpecs[conf.Type]; ok {
		rl, err := c.constructor(conf.Plugin, mgr, log.NewModule("."+conf.Type), stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache '%v': %v", conf.Type, err)
		}
		return rl, nil
	}
	return nil, types.ErrInvalidCacheType
}

//------------------------------------------------------------------------------
