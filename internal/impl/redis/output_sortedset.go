package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	zsFieldKey    = "key"
	zsFieldScore  = "score"
	zsFieldMember = "member"
	zsFieldExist  = "overwrite"
	zsFieldComp   = "compare"
	zsFieldIncr   = "increment"
)

func redisSortedsetOutputConfig() *service.ConfigSpec {

	return service.NewConfigSpec().
		Stable().
		Summary(`Adds to a Redis sorted set using the ZADD command.`).
		Description(output.Description(true, false, `
For more information on the advanced settings, visit the [redis docs](https://redis.io/commands/zadd/) for ZADD.`)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(zsFieldKey).
				Description("Specifies the key for the sorted set.").
				Examples("${! this.id }", "leaderboard"),
			service.NewInterpolatedStringField(zsFieldScore).
				Description("Specifies the float value associated with the member.").
				Examples("${! this.highscore }", "3.1415"),
			service.NewInterpolatedStringField(zsFieldMember).
				Description("Specifies the identifier for the member.").
				Examples("${! this.player_name }"),
			service.NewStringEnumField(zsFieldExist, "NX", "XX").
				Description("`XX`: Only update elements that already exist. `NX`: Only add new elements. `NX` is mutually exclusive with the `LT` and `GT` setting.").
				Optional().
				Advanced(),
			service.NewStringEnumField(zsFieldComp, "GT", "LT").
				Description("`LT`: Only update the score if the new score is less than the current score. `GT`: Only update the score if the new score is greater than the current score. These settings do not prevent new elements from being added. These settings are mutually exclusive with the `NX` setting.").
				Optional().
				Advanced(),
			service.NewBoolField(zsFieldIncr).
				Description("If this is set to true, the new score is added onto the current score instead of updating it.").
				Optional().
				Advanced(),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput(
		"redis_sortedset", redisSortedsetOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newRedisSortedsetWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type redisSortedsetWriter struct {
	log *service.Logger

	key    *service.InterpolatedString
	score  *service.InterpolatedString
	member *service.InterpolatedString
	exist  *string
	comp   *string
	incr   *bool

	clientCtor func() (redis.UniversalClient, error)
	client     redis.UniversalClient
	connMut    sync.RWMutex
}

func newRedisSortedsetWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *redisSortedsetWriter, err error) {
	r = &redisSortedsetWriter{
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
		log: mgr.Logger(),
	}
	if _, err = getClient(conf); err != nil {
		return
	}
	if r.key, err = conf.FieldInterpolatedString(zsFieldKey); err != nil {
		return
	}
	if r.score, err = conf.FieldInterpolatedString(zsFieldScore); err != nil {
		return
	}
	if r.member, err = conf.FieldInterpolatedString(zsFieldMember); err != nil {
		return
	}
	if conf.Contains(zsFieldExist) {
		exist, err := conf.FieldString(zsFieldExist)
		if err != nil {
			return nil, err
		}
		r.exist = &exist
	}
	if conf.Contains(zsFieldComp) {
		comp, err := conf.FieldString(zsFieldComp)
		if err != nil {
			return nil, err
		}
		r.comp = &comp
	}
	if conf.Contains(zsFieldIncr) {
		incr, err := conf.FieldBool(zsFieldIncr)
		if err != nil {
			return nil, err
		}
		r.incr = &incr
	}
	return
}

func (r *redisSortedsetWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.clientCtor()
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}
	r.log.Info("Setting messages as sorted set entries to Redis")
	r.client = client
	return nil
}

func (r *redisSortedsetWriter) Write(ctx context.Context, msg *service.Message) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	key, err := r.key.TryString(msg)
	if err != nil {
		return fmt.Errorf("key interpolation error: %w", err)
	}

	scoreStr, err := r.score.TryString(msg)
	if err != nil {
		return fmt.Errorf("score interpolation error: %w", err)
	}
	score, err := strconv.ParseFloat(scoreStr, 64)
	if err != nil {
		return fmt.Errorf("score integer conversion error: %w", err)
	}

	member, err := r.member.TryString(msg)
	if err != nil {
		return fmt.Errorf("member interpolation error: %w", err)
	}

	zArgs := redis.ZAddArgs{Members: []redis.Z{{Score: score, Member: member}}}

	if r.exist != nil {
		if *r.exist == "NX" {
			if r.comp != nil {
				return fmt.Errorf("NX is mutually exclusive with LT and GT")
			}
			zArgs.NX = true
		} else {
			zArgs.XX = true
		}
	}

	if r.comp != nil {
		if *r.comp == "GT" {
			zArgs.GT = true
		} else {
			zArgs.LT = true
		}
	}

	var redisErr error
	if r.incr != nil && *r.incr {
		redisErr = client.ZAddArgsIncr(ctx, key, zArgs).Err()
	} else {
		redisErr = client.ZAddArgs(ctx, key, zArgs).Err()
	}
	if redisErr != nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", redisErr)
		return service.ErrNotConnected
	}
	return nil
}

func (r *redisSortedsetWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisSortedsetWriter) Close(context.Context) error {
	return r.disconnect()
}
