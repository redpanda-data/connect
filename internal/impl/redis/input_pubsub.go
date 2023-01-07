package redis

import (
	"context"
	"sync"

	"github.com/go-redis/redis/v8"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/redis/old"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newRedisPubSubInput), docs.ComponentSpec{
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
		).ChildDefaultAndTypesFromStruct(input.NewRedisPubSubConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newRedisPubSubInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	r, err := newRedisPubSubReader(conf.RedisPubSub, mgr)
	if err != nil {
		return nil, err
	}
	return input.NewAsyncReader("redis_pubsub", input.NewAsyncPreserver(r), mgr)
}

type redisPubSubReader struct {
	client redis.UniversalClient
	pubsub *redis.PubSub
	cMut   sync.Mutex

	conf input.RedisPubSubConfig

	mgr bundle.NewManagement
	log log.Modular
}

func newRedisPubSubReader(conf input.RedisPubSubConfig, mgr bundle.NewManagement) (*redisPubSubReader, error) {
	r := &redisPubSubReader{
		conf: conf,
		mgr:  mgr,
		log:  mgr.Logger(),
	}

	_, err := clientFromConfig(mgr.FS(), r.conf.Config)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *redisPubSubReader) Connect(ctx context.Context) error {
	r.cMut.Lock()
	defer r.cMut.Unlock()

	if r.client != nil {
		return nil
	}

	client, err := clientFromConfig(r.mgr.FS(), r.conf.Config)
	if err != nil {
		return err
	}
	if _, err := client.Ping(ctx).Result(); err != nil {
		return err
	}

	r.log.Infof("Receiving Redis pub/sub messages from channels: %v\n", r.conf.Channels)

	r.client = client
	if r.conf.UsePatterns {
		r.pubsub = r.client.PSubscribe(ctx, r.conf.Channels...)
	} else {
		r.pubsub = r.client.Subscribe(ctx, r.conf.Channels...)
	}
	return nil
}

func (r *redisPubSubReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
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

func (r *redisPubSubReader) Close(ctx context.Context) (err error) {
	err = r.disconnect()
	return
}
