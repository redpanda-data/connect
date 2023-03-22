package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func redisScriptProcConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Beta().
		Version("4.11.0").
		Summary(`Performs actions against Redis using [LUA scripts](https://redis.io/docs/manual/programmability/eval-intro/).`).
		Description(`Actions are performed for each message and the message contents are replaced with the result.

In order to merge the result into the original message compose this processor within a ` + "[`branch` processor](/docs/components/processors/branch)" + `.`).
		Categories("Integration")

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	return spec.
		Field(service.NewStringField("script").
			Description("A script to use for the target operator. It has precedence over the 'command' field.").
			Example("return redis.call('set', KEYS[1], ARGV[1])")).
		Field(service.NewBloblangField("args_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of values matching in size to the number of arguments required for the specified Redis script.").
			Example("root = [ this.key ]").
			Example(`root = [ meta("kafka_key"), "hardcoded_value" ]`)).
		Field(service.NewBloblangField("keys_mapping").
			Description("A [Bloblang mapping](/docs/guides/bloblang/about) which should evaluate to an array of keys matching in size to the number of arguments required for the specified Redis script.").
			Example("root = [ this.key ]").
			Example(`root = [ meta("kafka_key"), this.count ]`)).
		Field(service.NewIntField("retries").
			Description("The maximum number of retries before abandoning a request.").
			Default(3).
			Advanced()).
		Field(service.NewDurationField("retry_period").
			Description("The time to wait before consecutive retry attempts.").
			Default("500ms").
			Advanced()).
		Example("Running a script",
			`The following example will use a script execution to get next element from a sorted set and set its score with timestamp unix nano value.`,
			`
pipeline:
  processors:
    - redis_script:
        url: TODO
        script: |
          local value = redis.call("ZRANGE", KEYS[1], '0', '0')

          if next(elements) == nil then
            return ''
          end

          redis.call("ZADD", "XX", KEYS[1], ARGV[1], value)

          return value
        keys_mapping: 'root = [ meta("key") ]'
        args_mapping: 'root = [ timestamp_unix_nano() ]'
`)
}

func init() {
	err := service.RegisterBatchProcessor(
		"redis_script", redisScriptProcConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchProcessor, error) {
			return newRedisScriptProcFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type redisScriptProc struct {
	log *service.Logger

	script      *redis.Script
	argsMapping *bloblang.Executor
	keysMapping *bloblang.Executor

	client      redis.UniversalClient
	retries     int
	retryPeriod time.Duration
}

func newRedisScriptProcFromConfig(conf *service.ParsedConfig, res *service.Resources) (*redisScriptProc, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	retries, err := conf.FieldInt("retries")
	if err != nil {
		return nil, err
	}

	retryPeriod, err := conf.FieldDuration("retry_period")
	if err != nil {
		return nil, err
	}

	var argsMapping *bloblang.Executor
	var keysMapping *bloblang.Executor

	var script string
	if script, err = conf.FieldString("script"); err != nil {
		return nil, err
	}

	redisScript := redis.NewScript(script)

	if argsMapping, err = conf.FieldBloblang("args_mapping"); err != nil {
		return nil, err
	}

	if keysMapping, err = conf.FieldBloblang("keys_mapping"); err != nil {
		return nil, err
	}

	r := &redisScriptProc{
		log: res.Logger(),

		script:      redisScript,
		argsMapping: argsMapping,
		keysMapping: keysMapping,

		retries:     retries,
		retryPeriod: retryPeriod,
		client:      client,
	}

	return r, nil
}

func (r *redisScriptProc) exec(ctx context.Context, index int, inBatch service.MessageBatch, msg *service.Message) error {
	args, err := getArgsMapping(inBatch, index, r.argsMapping)
	if err != nil {
		return fmt.Errorf("args_mapping failed: %w", err)
	}

	keys, err := getKeysStrMapping(inBatch, index, r.keysMapping)
	if err != nil {
		return fmt.Errorf("keys_mapping failed: %w", err)
	}

	res, err := r.script.Run(ctx, r.client, keys, args...).Result()
	for i := 0; i <= r.retries && err != nil; i++ {
		r.log.Errorf("script failed: %v", err)
		select {
		case <-time.After(r.retryPeriod):
		case <-ctx.Done():
			return ctx.Err()
		}
		res, err = r.script.Run(ctx, r.client, keys, args...).Result()
	}
	if err != nil {
		return err
	}

	msg.SetStructuredMut(res)
	return nil
}

func (r *redisScriptProc) ProcessBatch(ctx context.Context, inBatch service.MessageBatch) ([]service.MessageBatch, error) {
	newMsg := inBatch.Copy()
	for index, part := range newMsg {
		if err := r.exec(ctx, index, inBatch, part); err != nil {
			r.log.Debugf("Args mapping failed: %v", err)
			part.SetError(err)
		}
	}
	return []service.MessageBatch{newMsg}, nil
}

func (r *redisScriptProc) Close(ctx context.Context) error {
	return r.client.Close()
}

func getArgsMapping(inBatch service.MessageBatch, index int, mapping *bloblang.Executor) ([]any, error) {
	resMsg, err := inBatch.BloblangQuery(index, mapping)
	if err != nil {
		return nil, fmt.Errorf("mapping failed: %v", err)
	}

	iargs, err := resMsg.AsStructured()
	if err != nil {
		return nil, err
	}

	args, ok := iargs.([]any)
	if !ok {
		return nil, fmt.Errorf("mapping returned non-array result: %T", iargs)
	}

	for i, v := range args {
		args[i] = query.ISanitize(v)
	}
	return args, nil
}

func getKeysStrMapping(inBatch service.MessageBatch, index int, mapping *bloblang.Executor) ([]string, error) {
	resMsg, err := inBatch.BloblangQuery(index, mapping)
	if err != nil {
		return nil, fmt.Errorf("mapping failed: %v", err)
	}

	iargs, err := resMsg.AsStructured()
	if err != nil {
		return nil, err
	}

	args, ok := iargs.([]any)
	if !ok {
		return nil, fmt.Errorf("mapping returned non-array result: %T", iargs)
	}

	strArgs := make([]string, len(args))
	for i, v := range args {
		strArgs[i] = query.IToString(v)
	}
	return strArgs, nil
}
