package nats

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"

	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/nats-io/nats.go"
)

func natsKVInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.11.0"). // TODO
		Summary("Watches for updates in a NATS Key Value bucket").
		Description(`TODO` + auth.Description()).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"nats://127.0.0.1:4222"}).
			Example([]string{"nats://username:password@127.0.0.1:4222"})).
		Field(service.NewStringField("bucket").
			Description("TODO: bucket to watch").
			Example("my_kv_bucket")).
		Field(service.NewStringField("key").
			Description("TODO: A subject to filter key value watch results, can use wildcards").
			Default(">").
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewTLSToggledField("tls")).
		Field(service.NewInternalField(auth.FieldSpec()))
}

func init() {
	service.RegisterInput(
		"nats_kv", natsKVInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newKVReader(conf, mgr)
			return service.AutoRetryNacks(reader), err
		},
	)
}

type kvReader struct {
	urls     string
	bucket   string
	key      string
	authConf auth.Config
	tlsConf  *tls.Config

	log *service.Logger
	fs  *service.FS

	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	watcher  nats.KeyWatcher
	jCtx     nats.JetStreamContext
}

func newKVReader(conf *service.ParsedConfig, mgr *service.Resources) (*kvReader, error) {
	r := &kvReader{
		log:     mgr.Logger(),
		fs:      mgr.FS(),
		shutSig: shutdown.NewSignaller(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	r.urls = strings.Join(urlList, ",")

	if r.bucket, err = conf.FieldString("bucket"); err != nil {
		return nil, err
	}

	if conf.Contains("key") {
		if r.key, err = conf.FieldString("key"); err != nil {
			return nil, err
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		r.tlsConf = tlsConf
	}

	if r.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *kvReader) Connect(ctx context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.natsConn != nil {
		return nil
	}

	var err error

	defer func() {
		if err != nil {
			if r.watcher != nil {
				r.watcher.Stop()
			}
			if r.natsConn != nil {
				r.natsConn.Close()
			}
		}
	}()

	var opts []nats.Option
	if r.tlsConf != nil {
		opts = append(opts, nats.Secure(r.tlsConf))
	}
	opts = append(opts, authConfToOptions(r.authConf, r.fs)...)
	if r.natsConn, err = nats.Connect(r.urls, opts...); err != nil {
		return err
	}

	js, err := r.natsConn.JetStream()
	if err != nil {
		return err
	}

	kv, err := js.KeyValue(r.bucket)
	if err != nil {
		return err
	}

	r.watcher, err = kv.Watch(r.key)
	if err != nil {
		return err
	}

	r.log.Infof(`Watching NATS KV bucket "%v" for key(s) "%v"`, r.bucket, r.key)

	return nil
}

func (r *kvReader) disconnect() {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.watcher != nil {
		r.watcher.Stop()
		r.watcher = nil
	}
	if r.natsConn != nil {
		r.natsConn.Close()
		r.natsConn = nil
	}
}

func (r *kvReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	r.connMut.Lock()
	watcher := r.watcher
	r.connMut.Unlock()

	if watcher == nil {
		return nil, nil, service.ErrNotConnected
	}

	updates := watcher.Updates()

	for {
		var entry nats.KeyValueEntry
		var open bool
		select {
		case entry, open = <-updates:
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		if !open {
			//n.disconnect() TODO: disconnect?
			return nil, nil, service.ErrNotConnected
		}

		if entry == nil {
			continue
		}

		msg := service.NewMessage(entry.Value())
		msg.MetaSetMut("nats_kv_key", entry.Key())
		msg.MetaSetMut("nats_kv_bucket", entry.Bucket())
		msg.MetaSetMut("nats_kv_revision", entry.Revision())
		msg.MetaSetMut("nats_kv_delta", entry.Delta())
		msg.MetaSetMut("nats_kv_operation", entry.Operation().String())
		msg.MetaSetMut("nats_kv_created", entry.Created())

		return msg, func(ctx context.Context, res error) error {
			return nil // TODO: Don't know what to do here
		}, nil
	}
}

func (r *kvReader) Close(ctx context.Context) error {
	go func() {
		r.disconnect()
		r.shutSig.ShutdownComplete()
	}()
	select {
	case <-r.shutSig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}
