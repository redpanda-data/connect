package cache

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"

	"gopkg.in/yaml.v3"
)

type cacheConstructor func(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (types.Cache, error)

// TypeSpec is a constructor and a usage description for each cache type.
type TypeSpec struct {
	constructor cacheConstructor

	Summary           string
	Description       string
	Footnotes         string
	config            docs.FieldSpec
	FieldSpecs        docs.FieldSpecs
	Status            docs.Status
	SupportsPerKeyTTL bool
	Version           string
}

// ConstructorFunc is a func signature able to construct a cache.
type ConstructorFunc func(Config, types.Manager, log.Modular, metrics.Type) (types.Cache, error)

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
			Type:        docs.TypeCache,
			Name:        k,
			Summary:     v.Summary,
			Description: v.Description,
			Footnotes:   v.Footnotes,
			Config:      conf,
			Status:      v.Status,
			Version:     v.Version,
		}
		spec.Description = cache.Description(v.SupportsPerKeyTTL, spec.Description)
		fn(ConstructorFunc(v.constructor), spec)
	}
}

// Constructors is a map of all cache types with their specs.
var Constructors = map[string]TypeSpec{}

//------------------------------------------------------------------------------

// String constants representing each cache type.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
const (
	TypeAWSDynamoDB = "aws_dynamodb"
	TypeAWSS3       = "aws_s3"
	TypeDynamoDB    = "dynamodb"
	TypeFile        = "file"
	TypeMemcached   = "memcached"
	TypeMemory      = "memory"
	TypeMongoDB     = "mongodb"
	TypeMultilevel  = "multilevel"
	TypeRedis       = "redis"
	TypeRistretto   = "ristretto"
	TypeS3          = "s3"
)

//------------------------------------------------------------------------------

// Config is the all encompassing configuration struct for all cache types.
// Deprecated: Do not add new components here. Instead, use the public plugin
// APIs. Examples can be found in: ./internal/impl
type Config struct {
	Label       string           `json:"label" yaml:"label"`
	Type        string           `json:"type" yaml:"type"`
	AWSDynamoDB DynamoDBConfig   `json:"aws_dynamodb" yaml:"aws_dynamodb"`
	AWSS3       S3Config         `json:"aws_s3" yaml:"aws_s3"`
	DynamoDB    DynamoDBConfig   `json:"dynamodb" yaml:"dynamodb"`
	File        FileConfig       `json:"file" yaml:"file"`
	Memcached   MemcachedConfig  `json:"memcached" yaml:"memcached"`
	Memory      MemoryConfig     `json:"memory" yaml:"memory"`
	MongoDB     MongoDBConfig    `json:"mongodb" yaml:"mongodb"`
	Multilevel  MultilevelConfig `json:"multilevel" yaml:"multilevel"`
	Plugin      interface{}      `json:"plugin,omitempty" yaml:"plugin,omitempty"`
	Redis       RedisConfig      `json:"redis" yaml:"redis"`
	Ristretto   RistrettoConfig  `json:"ristretto" yaml:"ristretto"`
	S3          S3Config         `json:"s3" yaml:"s3"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Label:       "",
		Type:        "memory",
		AWSDynamoDB: NewDynamoDBConfig(),
		AWSS3:       NewS3Config(),
		DynamoDB:    NewDynamoDBConfig(),
		File:        NewFileConfig(),
		Memcached:   NewMemcachedConfig(),
		Memory:      NewMemoryConfig(),
		MongoDB:     NewMongoDBConfig(),
		Multilevel:  NewMultilevelConfig(),
		Plugin:      nil,
		Redis:       NewRedisConfig(),
		Ristretto:   NewRistrettoConfig(),
		S3:          NewS3Config(),
	}
}

//------------------------------------------------------------------------------

// UnmarshalYAML ensures that when parsing configs that are in a map or slice
// the default values are still applied.
func (conf *Config) UnmarshalYAML(value *yaml.Node) error {
	type confAlias Config
	aliased := confAlias(NewConfig())

	err := value.Decode(&aliased)
	if err != nil {
		return fmt.Errorf("line %v: %v", value.Line, err)
	}

	var spec docs.ComponentSpec
	if aliased.Type, spec, err = docs.GetInferenceCandidateFromYAML(nil, docs.TypeCache, aliased.Type, value); err != nil {
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

// New creates a cache type based on an cache configuration.
func New(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (types.Cache, error) {
	if mgrV2, ok := mgr.(interface {
		NewCache(conf Config) (types.Cache, error)
	}); ok {
		return mgrV2.NewCache(conf)
	}
	if c, ok := Constructors[conf.Type]; ok {
		cache, err := c.constructor(conf, mgr, log, stats)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache '%v': %v", conf.Type, err)
		}
		return cache, nil
	}
	return nil, types.ErrInvalidCacheType
}
