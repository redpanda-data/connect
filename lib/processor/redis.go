package processor

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/docs"
	bredis "github.com/Jeffail/benthos/v3/internal/impl/redis/old"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/tracing"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/go-redis/redis/v7"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeRedis] = TypeSpec{
		constructor: NewRedis,
		Categories: []Category{
			CategoryIntegration,
		},
		Summary: `
Performs actions against Redis that aren't possible using a
` + "[`cache`](/docs/components/processors/cache)" + ` processor. Actions are
performed for each message of a batch, where the contents are replaced with the
result.`,
		Description: `
## Operators

### ` + "`keys`" + `

Returns an array of strings containing all the keys that match the pattern specified by the ` + "`key` field" + `.

### ` + "`scard`" + `

Returns the cardinality of a set, or ` + "`0`" + ` if the key does not exist.

### ` + "`sadd`" + `

Adds a new member to a set. Returns ` + "`1`" + ` if the member was added.

### ` + "`incrby`" + `

Increments the number stored at ` + "`key`" + ` by the message content. If the
key does not exist, it is set to ` + "`0`" + ` before performing the operation.
Returns the value of ` + "`key`" + ` after the increment.`,
		FieldSpecs: bredis.ConfigDocs().Add(
			docs.FieldCommon("operator", "The [operator](#operators) to apply.").HasOptions("scard", "sadd", "incrby", "keys"),
			docs.FieldCommon("key", "A key to use for the target operator.").IsInterpolated(),
			docs.FieldAdvanced("retries", "The maximum number of retries before abandoning a request."),
			docs.FieldAdvanced("retry_period", "The time to wait before consecutive retry attempts."),
			PartsFieldSpec,
		),
		Examples: []docs.AnnotatedExample{
			{
				Title: "Querying Cardinality",
				Summary: `
If given payloads containing a metadata field ` + "`set_key`" + ` it's possible
to query and store the cardinality of the set for each message using a
` + "[`branch` processor](/docs/components/processors/branch)" + ` in order to
augment rather than replace the message contents:`,
				Config: `
pipeline:
  processors:
    - branch:
        processors:
          - redis:
              url: TODO
              operator: scard
              key: ${! meta("set_key") }
        result_map: 'root.cardinality = this'
`,
			},
			{
				Title: "Running Total",
				Summary: `
If we have JSON data containing number of friends visited during covid 19:

` + "```json" + `
{"name":"ash","month":"feb","year":2019,"friends_visited":10}
{"name":"ash","month":"apr","year":2019,"friends_visited":-2}
{"name":"bob","month":"feb","year":2019,"friends_visited":3}
{"name":"bob","month":"apr","year":2019,"friends_visited":1}
` + "```" + `

We can add a field that contains the running total number of friends visited:

` + "```json" + `
{"name":"ash","month":"feb","year":2019,"friends_visited":10,"total":10}
{"name":"ash","month":"apr","year":2019,"friends_visited":-2,"total":8}
{"name":"bob","month":"feb","year":2019,"friends_visited":3,"total":3}
{"name":"bob","month":"apr","year":2019,"friends_visited":1,"total":4}
` + "```" + `

Using the ` + "`incrby`" + ` operator:
                `,
				Config: `
pipeline:
  processors:
    - branch:
        request_map: |
            root = this.friends_visited
            meta name = this.name
        processors:
          - redis:
              url: TODO
              operator: incrby
              key: ${! meta("name") }
        result_map: 'root.total = this'
`,
			},
		},
	}
}

//------------------------------------------------------------------------------

// RedisConfig contains configuration fields for the Redis processor.
type RedisConfig struct {
	bredis.Config `json:",inline" yaml:",inline"`
	Parts         []int  `json:"parts" yaml:"parts"`
	Operator      string `json:"operator" yaml:"operator"`
	Key           string `json:"key" yaml:"key"`
	Retries       int    `json:"retries" yaml:"retries"`
	RetryPeriod   string `json:"retry_period" yaml:"retry_period"`
}

// NewRedisConfig returns a RedisConfig with default values.
func NewRedisConfig() RedisConfig {
	return RedisConfig{
		Config:      bredis.NewConfig(),
		Parts:       []int{},
		Operator:    "scard",
		Key:         "",
		Retries:     3,
		RetryPeriod: "500ms",
	}
}

//------------------------------------------------------------------------------

// Redis is a processor that performs redis operations
type Redis struct {
	parts []int
	conf  Config
	log   log.Modular
	stats metrics.Type

	key *field.Expression

	operator    redisOperator
	client      redis.UniversalClient
	retryPeriod time.Duration

	mCount      metrics.StatCounter
	mErr        metrics.StatCounter
	mSent       metrics.StatCounter
	mBatchSent  metrics.StatCounter
	mRedisRetry metrics.StatCounter
}

// NewRedis returns a Redis processor.
func NewRedis(
	conf Config, mgr types.Manager, log log.Modular, stats metrics.Type,
) (processor.V1, error) {
	var retryPeriod time.Duration
	if tout := conf.Redis.RetryPeriod; len(tout) > 0 {
		var err error
		if retryPeriod, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse retry period string: %v", err)
		}
	}

	client, err := conf.Redis.Config.Client()
	if err != nil {
		return nil, err
	}

	key, err := interop.NewBloblangField(mgr, conf.Redis.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to parse key expression: %v", err)
	}

	r := &Redis{
		parts: conf.Redis.Parts,
		conf:  conf,
		log:   log,
		stats: stats,

		key: key,

		retryPeriod: retryPeriod,
		client:      client,

		mCount:      stats.GetCounter("count"),
		mErr:        stats.GetCounter("error"),
		mSent:       stats.GetCounter("sent"),
		mBatchSent:  stats.GetCounter("batch.sent"),
		mRedisRetry: stats.GetCounter("redis.retry"),
	}

	if r.operator, err = getRedisOperator(conf.Redis.Operator); err != nil {
		return nil, err
	}
	return r, nil
}

//------------------------------------------------------------------------------

type redisOperator func(r *Redis, key string, part *message.Part) error

func newRedisKeysOperator() redisOperator {
	return func(r *Redis, key string, part *message.Part) error {
		res, err := r.client.Keys(key).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("Keys command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.Keys(key).Result()
		}
		if err != nil {
			return err
		}

		iRes := make([]interface{}, 0, len(res))
		for _, v := range res {
			iRes = append(iRes, v)
		}
		return part.SetJSON(iRes)
	}
}

func newRedisSCardOperator() redisOperator {
	return func(r *Redis, key string, part *message.Part) error {
		res, err := r.client.SCard(key).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("SCard command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.SCard(key).Result()
		}
		if err != nil {
			return err
		}

		part.Set(strconv.AppendInt(nil, res, 10))
		return nil
	}
}

func newRedisSAddOperator() redisOperator {
	return func(r *Redis, key string, part *message.Part) error {
		res, err := r.client.SAdd(key, part.Get()).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("SAdd command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.SAdd(key, part.Get()).Result()
		}
		if err != nil {
			return err
		}

		part.Set(strconv.AppendInt(nil, res, 10))
		return nil
	}
}

func newRedisIncrByOperator() redisOperator {
	return func(r *Redis, key string, part *message.Part) error {
		valueInt, err := strconv.Atoi(string(part.Get()))
		if err != nil {
			return err
		}
		res, err := r.client.IncrBy(key, int64(valueInt)).Result()

		for i := 0; i <= r.conf.Redis.Retries && err != nil; i++ {
			r.log.Errorf("incrby command failed: %v\n", err)
			<-time.After(r.retryPeriod)
			r.mRedisRetry.Incr(1)
			res, err = r.client.IncrBy(key, int64(valueInt)).Result()
		}
		if err != nil {
			return err
		}

		part.Set(strconv.AppendInt(nil, res, 10))
		return nil
	}
}

func getRedisOperator(opStr string) (redisOperator, error) {
	switch opStr {
	case "keys":
		return newRedisKeysOperator(), nil
	case "sadd":
		return newRedisSAddOperator(), nil
	case "scard":
		return newRedisSCardOperator(), nil
	case "incrby":
		return newRedisIncrByOperator(), nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

// ProcessMessage applies the processor to a message, either creating >0
// resulting messages or a response to be sent back to the message source.
func (r *Redis) ProcessMessage(msg *message.Batch) ([]*message.Batch, error) {
	r.mCount.Incr(1)
	newMsg := msg.Copy()

	proc := func(index int, span *tracing.Span, part *message.Part) error {
		key := r.key.String(index, newMsg)
		if err := r.operator(r, key, part); err != nil {
			r.mErr.Incr(1)
			r.log.Debugf("Operator failed for key '%s': %v\n", key, err)
			return err
		}
		return nil
	}

	IteratePartsWithSpanV2(TypeRedis, r.parts, newMsg, proc)

	r.mBatchSent.Incr(1)
	r.mSent.Incr(int64(newMsg.Len()))
	return []*message.Batch{newMsg}, nil
}

// CloseAsync shuts down the processor and stops processing requests.
func (r *Redis) CloseAsync() {
}

// WaitForClose blocks until the processor has closed down.
func (r *Redis) WaitForClose(timeout time.Duration) error {
	r.client.Close()
	return nil
}

//------------------------------------------------------------------------------
