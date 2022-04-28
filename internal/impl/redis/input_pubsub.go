package redis

import (
	"context"
	"sync"
	"time"

	"github.com/go-redis/redis/v7"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	oinput "github.com/benthosdev/benthos/v4/internal/old/input"
	"github.com/benthosdev/benthos/v4/internal/old/input/reader"
)

func init() {
	err := bundle.AllInputs.Add(bundle.InputConstructorFromSimple(func(c oinput.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newRedisPubSubInput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name: "redis_pubsub",
		Summary: `
Consume from a Redis publish/subscribe channel using either the SUBSCRIBE or
PSUBSCRIBE commands.`,
		Description: `
In order to subscribe to channels using the ` + "`PSUBSCRIBE`" + ` command set
the field ` + "`use_patterns` to `true`" + `, then you can include glob-style
patterns in your channel names. For example:

- ` + "`h?llo`" + ` subscribes to hello, hallo and hxllo
- ` + "`h*llo`" + ` subscribes to hllo and heeeello
- ` + "`h[ae]llo`" + ` subscribes to hello and hallo, but not hillo

Use ` + "`\\`" + ` to escape special characters if you want to match them
verbatim.`,
		Config: docs.FieldComponent().WithChildren(old.ConfigDocs()...).WithChildren(
			docs.FieldString("channels", "A list of channels to consume from.").Array(),
			docs.FieldBool("use_patterns", "Whether to use the PSUBSCRIBE command."),
		).ChildDefaultAndTypesFromStruct(oinput.NewRedisPubSubConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisPubSubInput(conf oinput.Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (input.Streamed, error) {
	r, err := newRedisPubSubReader(conf.RedisPubSub, log)
	if err != nil {
		return nil, err
	}
	return oinput.NewAsyncReader("redis_pubsub", true, reader.NewAsyncPreserver(r), log, stats)
}

type redisPubSubReader struct {
	client redis.UniversalClient
	pubsub *redis.PubSub
	cMut   sync.Mutex

	conf oinput.RedisPubSubConfig

	log log.Modular
}

func newRedisPubSubReader(conf oinput.RedisPubSubConfig, log log.Modular) (*redisPubSubReader, error) {
	r := &redisPubSubReader{
		conf: conf,
		log:  log,
	}

	_, err := r.conf.Config.Client()
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *redisPubSubReader) ConnectWithContext(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := r.conf.Config.Client()
	if err != nil {
		return err
	}
	if _, err := client.Ping().Result(); err != nil {
		return err
	}

	r.log.Infof("Receiving Redis pub/sub messages from channels: %v\n", r.conf.Channels)

	r.client = client
	if r.conf.UsePatterns {
		r.pubsub = r.client.PSubscribe(r.conf.Channels...)
	} else {
		r.pubsub = r.client.Subscribe(r.conf.Channels...)
	}
	return nil
}

func (r *redisPubSubReader) ReadWithContext(ctx context.Context) (*message.Batch, reader.AsyncAckFn, error) {
	var pubsub *redis.PubSub

	r.cMut.Lock()
	pubsub = r.pubsub
	r.cMut.Unlock()

	if pubsub == nil {
		return nil, nil, component.ErrNotConnected
	}

	select {
	case rMsg, open := <-pubsub.Channel():
		if !open {
			_ = r.disconnect()
			return nil, nil, component.ErrTypeClosed
		}
		return message.QuickBatch([][]byte{[]byte(rMsg.Payload)}), func(ctx context.Context, err error) error {
			return nil
		}, nil
	case <-ctx.Done():
	}

	return nil, nil, component.ErrTimeout
}

func (r *redisPubSubReader) disconnect() error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	var err error
	if r.pubsub != nil {
		err = r.pubsub.Close()
		r.pubsub = nil
	}
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

func (r *redisPubSubReader) CloseAsync() {
	_ = r.disconnect()
}

func (r *redisPubSubReader) WaitForClose(timeout time.Duration) error {
	return nil
}
