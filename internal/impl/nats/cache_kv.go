package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

func natsKVCacheConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.24.0").
		Summary("Cache key/values in a NATS key-value bucket.").
		Description(ConnectionNameDescription() + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewStringField("bucket").
			Description("The name of the KV bucket to watch for updates.").
			Example("my_kv_bucket")).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
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
	label    string
	urls     string
	bucket   string
	authConf auth.Config
	tlsConf  *tls.Config

	log *service.Logger
	fs  *service.FS

	shutSig *shutdown.Signaller

	connMut  sync.RWMutex
	natsConn *nats.Conn
	kv       nats.KeyValue
}

func newKVCache(conf *service.ParsedConfig, mgr *service.Resources) (*kvCache, error) {
	p := &kvCache{
		label:   mgr.Label(),
		log:     mgr.Logger(),
		fs:      mgr.FS(),
		shutSig: shutdown.NewSignaller(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	p.urls = strings.Join(urlList, ",")

	if p.bucket, err = conf.FieldString("bucket"); err != nil {
		return nil, err
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		p.tlsConf = tlsConf
	}

	if p.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
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
			if p.natsConn != nil {
				p.natsConn.Close()
			}
		}
	}()

	var opts []nats.Option
	if p.tlsConf != nil {
		opts = append(opts, nats.Secure(p.tlsConf))
	}
	opts = append(opts, nats.Name(p.label))
	opts = append(opts, authConfToOptions(p.authConf, p.fs)...)
	if p.natsConn, err = nats.Connect(p.urls, opts...); err != nil {
		return err
	}

	js, err := p.natsConn.JetStream()
	if err != nil {
		return err
	}

	p.kv, err = js.KeyValue(p.bucket)
	if err != nil {
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
