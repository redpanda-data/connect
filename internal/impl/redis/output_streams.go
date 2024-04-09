package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	soFieldStream       = "stream"
	soFieldBodyKey      = "body_key"
	soFieldMaxLenApprox = "max_length"
	soFieldMetadata     = "metadata"
	soFieldBatching     = "batching"
)

func redisStreamsOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Pushes messages to a Redis (v5.0+) Stream (which is created if it doesn't already exist) using the XADD command.`).
		Description(output.Description(true, true, `
It's possible to specify a maximum length of the target stream by setting it to a value greater than 0, in which case this cap is applied only when Redis is able to remove a whole macro node, for efficiency.

Redis stream entries are key/value pairs, as such it is necessary to specify the key to be set to the body of the message. All metadata fields of the message will also be set as key/value pairs, if there is a key collision between a metadata item and the body then the body takes precedence.`)).
		Categories("Services").
		Fields(clientFields()...).
		Fields(
			service.NewInterpolatedStringField(soFieldStream).
				Description("The stream to add messages to."),
			service.NewStringField(soFieldBodyKey).
				Description("A key to set the raw body of the message to.").
				Default("body"),
			service.NewIntField(soFieldMaxLenApprox).
				Description("When greater than zero enforces a rough cap on the length of the target stream.").
				Default(0),
			service.NewOutputMaxInFlightField(),
			service.NewMetadataExcludeFilterField(soFieldMetadata).
				Description("Specify criteria for which metadata values are included in the message body."),
			service.NewBatchPolicyField(soFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput(
		"redis_streams", redisStreamsOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPol service.BatchPolicy, mif int, err error) {
			if batchPol, err = conf.FieldBatchPolicy(soFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			out, err = newRedisStreamsWriter(conf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type redisStreamsWriter struct {
	log *service.Logger

	stream     *service.InterpolatedString
	streamStr  string
	bodyKey    string
	maxLen     int
	metaFilter *service.MetadataExcludeFilter

	clientCtor func() (redis.UniversalClient, error)
	client     redis.UniversalClient
	connMut    sync.RWMutex
}

func newRedisStreamsWriter(conf *service.ParsedConfig, mgr *service.Resources) (r *redisStreamsWriter, err error) {
	r = &redisStreamsWriter{
		log: mgr.Logger(),
		clientCtor: func() (redis.UniversalClient, error) {
			return getClient(conf)
		},
	}

	if r.stream, err = conf.FieldInterpolatedString(soFieldStream); err != nil {
		return
	}
	if r.streamStr, err = conf.FieldString(soFieldStream); err != nil {
		return
	}
	if r.bodyKey, err = conf.FieldString(soFieldBodyKey); err != nil {
		return
	}
	if r.maxLen, err = conf.FieldInt(soFieldMaxLenApprox); err != nil {
		return
	}
	if r.metaFilter, err = conf.FieldMetadataExcludeFilter(soFieldMetadata); err != nil {
		return
	}

	if _, err := getClient(conf); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *redisStreamsWriter) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	client, err := r.clientCtor()
	if err != nil {
		return err
	}
	if _, err = client.Ping(ctx).Result(); err != nil {
		return err
	}
	r.client = client
	return nil
}

func (r *redisStreamsWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	r.connMut.RLock()
	client := r.client
	r.connMut.RUnlock()

	if client == nil {
		return service.ErrNotConnected
	}

	partToMap := func(p *service.Message) (values map[string]any, err error) {
		values = map[string]any{}
		_ = r.metaFilter.WalkMut(p, func(k string, v any) error {
			values[k] = v
			return nil
		})
		values[r.bodyKey], err = p.AsBytes()
		return
	}

	if len(batch) == 1 {
		stream, err := batch.TryInterpolatedString(0, r.stream)
		if err != nil {
			return fmt.Errorf("stream interpolation error: %w", err)
		}

		values, err := partToMap(batch[0])
		if err != nil {
			return err
		}

		if err := client.XAdd(ctx, &redis.XAddArgs{
			ID:     "*",
			Stream: stream,
			MaxLen: int64(r.maxLen),
			Approx: true,
			Values: values,
		}).Err(); err != nil {
			_ = r.disconnect()
			r.log.Errorf("Error from redis: %v\n", err)
			return service.ErrNotConnected
		}
		return nil
	}

	pipe := client.Pipeline()
	for i := 0; i < len(batch); i++ {
		stream, err := batch.TryInterpolatedString(i, r.stream)
		if err != nil {
			return fmt.Errorf("stream interpolation error: %w", err)
		}

		values, err := partToMap(batch[i])
		if err != nil {
			return err
		}

		_ = pipe.XAdd(ctx, &redis.XAddArgs{
			ID:     "*",
			Stream: stream,
			MaxLen: int64(r.maxLen),
			Approx: true,
			Values: values,
		})
	}

	cmders, err := pipe.Exec(ctx)
	if err != nil {
		_ = r.disconnect()
		r.log.Errorf("Error from redis: %v\n", err)
		return service.ErrNotConnected
	}

	var batchErr *service.BatchError
	for i, res := range cmders {
		if res.Err() != nil {
			if batchErr == nil {
				batchErr = service.NewBatchError(batch, res.Err())
			}
			batchErr.Failed(i, res.Err())
		}
	}
	if batchErr != nil {
		return batchErr
	}
	return nil
}

func (r *redisStreamsWriter) disconnect() error {
	r.connMut.Lock()
	defer r.connMut.Unlock()
	if r.client != nil {
		err := r.client.Close()
		r.client = nil
		return err
	}
	return nil
}

func (r *redisStreamsWriter) Close(context.Context) error {
	return r.disconnect()
}
