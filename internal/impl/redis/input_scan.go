package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/public/service"
)

func init() {
	err := service.RegisterInput(
		"redis_scan", redisScanInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			i, err := newRedisScanInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, i)
		})
	if err != nil {
		panic(err)
	}
}

const matchFieldName = "match"

func redisScanInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Summary(`Scans the set of keys in the current selected database and gets their values, using the Scan and Get commands.`).
		Description(`Optionally, iterates only elements matching a blob-style pattern. For example:
- ` + "`*foo*`" + ` iterates only keys which contain ` + "`foo`" + ` in it.
- ` + "`foo*`" + ` iterates only keys starting with ` + "`foo`" + `.

This input generates a message for each key value pair in the following format:

` + "```json" + `
{"key":"foo","value":"bar"}
` + "```" + `
`).
		Categories("Services").
		Version("4.27.0")

	for _, f := range clientFields() {
		spec = spec.Field(f)
	}

	return spec.
		Field(service.NewAutoRetryNacksToggleField()).
		Field(service.NewStringField(matchFieldName).
			Description("Iterates only elements matching the optional glob-style pattern. By default, it matches all elements.").
			Example("*").
			Example("1*").
			Example("foo*").
			Example("foo").
			Example("*4*").
			Default(""))
}

func newRedisScanInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}
	match, err := conf.FieldString(matchFieldName)
	if err != nil {
		return nil, fmt.Errorf("error retrieving %s: %v", matchFieldName, err)
	}
	r := &redisScanReader{
		client: client,
		match:  match,
		log:    mgr.Logger(),
	}
	return r, nil
}

type redisScanReader struct {
	match  string
	client redis.UniversalClient
	iter   *redis.ScanIterator
	log    *service.Logger
}

func (r *redisScanReader) Connect(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	r.iter = r.client.Scan(context.Background(), 0, r.match, 0).Iterator()
	return r.iter.Err()
}

func (r *redisScanReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if r.iter.Next(ctx) {
		key := r.iter.Val()

		res := r.client.Get(ctx, key)
		if err := res.Err(); err != nil {
			return nil, nil, err
		}

		msg := service.NewMessage(nil)
		msg.SetStructuredMut(map[string]any{
			"key":   key,
			"value": res.Val(),
		})
		return msg, func(ctx context.Context, err error) error {
			return err
		}, nil
	}
	return nil, nil, service.ErrEndOfInput
}

func (r *redisScanReader) Close(ctx context.Context) (err error) {
	return r.client.Close()
}
