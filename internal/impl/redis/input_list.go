package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

type redisPopCommand string

const (
	bLPop redisPopCommand = "blpop"
	bRPop redisPopCommand = "brpop"
)

func redisListInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Pops messages from the beginning of a Redis list using the BLPop command.`).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewStringField("key").
				Description("The key of a list to read from."),
			service.NewAutoRetryNacksToggleField(),
			service.NewInputMaxInFlightField().Version("4.9.0"),
			service.NewDurationField("timeout").
				Description("The length of time to poll for new messages before reattempting.").
				Default("5s").
				Advanced(),
			service.NewStringEnumField("command", string(bLPop), string(bRPop)).
				Description("The command used to pop elements from the Redis list").
				Default(string(bLPop)).
				Advanced().
				Version("4.22.0"),
		)
}

func init() {
	err := service.RegisterInput(
		"redis_list", redisListInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			mInF, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, err
			}

			i, err := newRedisListInputFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}

			if i, err = service.AutoRetryNacksToggled(conf, i); err != nil {
				return nil, err
			}

			return service.InputWithMaxInFlight(mInF, i), nil
		})
	if err != nil {
		panic(err)
	}
}

func newRedisListInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	client, err := getClient(conf)
	if err != nil {
		return nil, err
	}

	r := &redisListReader{
		client: client,
		log:    mgr.Logger(),
	}

	if r.key, err = conf.FieldString("key"); err != nil {
		return nil, err
	}

	if r.timeout, err = conf.FieldDuration("timeout"); err != nil {
		return nil, err
	}

	popCommand, err := conf.FieldString("command")
	if err != nil {
		return nil, err
	}

	switch redisPopCommand(popCommand) {
	case bLPop:
		r.pop = client.BLPop

	case bRPop:
		r.pop = client.BRPop

	default:
		return nil, fmt.Errorf("invalid redis command: %s", popCommand)
	}

	return r, nil
}

type redisListReader struct {
	client  redis.UniversalClient
	timeout time.Duration
	key     string
	pop     func(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd

	log *service.Logger
}

func (r *redisListReader) Connect(ctx context.Context) error {
	_, err := r.client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	return nil
}

func (r *redisListReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	res, err := r.pop(ctx, r.timeout, r.key).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, nil, err
	}

	if len(res) < 2 {
		return nil, nil, component.ErrTimeout
	}

	return service.NewMessage([]byte(res[1])),
		func(context.Context, error) error { return nil },
		nil
}

func (r *redisListReader) Close(ctx context.Context) (err error) {
	return r.client.Close()
}
