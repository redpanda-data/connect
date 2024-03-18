package nats

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

func natsKVCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.27.0").
		Summary("Cache key/values in a NATS key-value bucket.").
		Description(ConnectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewStringField("bucket").
			Description("The name of the KV bucket to store items within.").
			Example("my_kv_bucket")).
		Fields(connectionTailFields()...)
}

func init() {
	err := service.RegisterCache(
		"nats_kv", natsKVCacheConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Cache, error) {
			return newKVCache(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

type kvCache struct {
	connDetails connectionDetails
	bucket      string

	log *service.Logger

	shutSig *shutdown.Signaller

	connMut  sync.RWMutex
	natsConn *nats.Conn
	kv       nats.KeyValue
}

func newKVCache(conf *service.ParsedConfig, mgr *service.Resources) (*kvCache, error) {
	p := &kvCache{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if p.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if p.bucket, err = conf.FieldString("bucket"); err != nil {
		return nil, err
	}

	err = p.connect(context.Background())
	return p, err
}

func (p *kvCache) disconnect() {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		p.natsConn.Close()
		p.natsConn = nil
	}
	p.kv = nil
}

func (p *kvCache) connect(ctx context.Context) error {
	p.connMut.Lock()
	defer p.connMut.Unlock()

	if p.natsConn != nil {
		return nil
	}

	var err error

	defer func() {
		if err != nil {
			p.disconnect()
		}
	}()

	if p.natsConn, err = p.connDetails.get(ctx); err != nil {
		return err
	}

	var js nats.JetStreamContext
	if js, err = p.natsConn.JetStream(); err != nil {
		return err
	}

	if p.kv, err = js.KeyValue(p.bucket); err != nil {
		return err
	}

	p.log.Infof("Caching on NATS KV bucket: %s", p.bucket)
	return nil
}

func (p *kvCache) Get(ctx context.Context, key string) ([]byte, error) {
	p.connMut.RLock()
	defer p.connMut.RUnlock()

	entry, err := p.kv.Get(key)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			err = service.ErrKeyNotFound
		}
		return nil, err
	}
	return entry.Value(), nil
}

func (p *kvCache) Set(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	p.connMut.RLock()
	defer p.connMut.RUnlock()

	_, err := p.kv.Put(key, value)
	return err
}

func (p *kvCache) Add(ctx context.Context, key string, value []byte, _ *time.Duration) error {
	p.connMut.RLock()
	defer p.connMut.RUnlock()
	_, err := p.kv.Create(key, value)
	if errors.Is(err, nats.ErrKeyExists) {
		return service.ErrKeyAlreadyExists
	}
	return err
}

func (p *kvCache) Delete(ctx context.Context, key string) error {
	p.connMut.RLock()
	defer p.connMut.RUnlock()
	return p.kv.Delete(key)
}

func (p *kvCache) Close(ctx context.Context) error {
	go func() {
		p.disconnect()
		p.shutSig.ShutdownComplete()
	}()
	select {
	case <-p.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
