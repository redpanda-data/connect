package nats

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	kviFieldKey            = "key"
	kviFieldIgnoreDeletes  = "ignore_deletes"
	kviFieldIncludeHistory = "include_history"
	kviFieldMetaOnly       = "meta_only"
)

func natsKVInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Services").
		Version("4.12.0").
		Summary("Watches for updates in a NATS key-value bucket.").
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_kv_key
- nats_kv_bucket
- nats_kv_revision
- nats_kv_delta
- nats_kv_operation
- nats_kv_created
` + "```" + `

` + connectionNameDescription() + authDescription()).
		Fields(kvDocs([]*service.ConfigField{
			service.NewStringField(kviFieldKey).
				Description("Key to watch for updates, can include wildcards.").
				Default(">").
				Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>"),
			service.NewAutoRetryNacksToggleField(),
			service.NewBoolField(kviFieldIgnoreDeletes).
				Description("Do not send delete markers as messages.").
				Default(false).
				Advanced(),
			service.NewBoolField(kviFieldIncludeHistory).
				Description("Include all the history per key, not just the last one.").
				Default(false).
				Advanced(),
			service.NewBoolField(kviFieldMetaOnly).
				Description("Retrieve only the metadata of the entry").
				Default(false).
				Advanced(),
		}...)...)
}

func init() {
	err := service.RegisterInput(
		"nats_kv", natsKVInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newKVReader(conf, mgr)
			if err != nil {
				return nil, err
			}
			return service.AutoRetryNacksToggled(conf, reader)
		},
	)
	if err != nil {
		panic(err)
	}
}

type kvReader struct {
	connDetails    connectionDetails
	bucket         string
	key            string
	ignoreDeletes  bool
	includeHistory bool
	metaOnly       bool

	log *service.Logger

	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	watcher  jetstream.KeyWatcher
}

func newKVReader(conf *service.ParsedConfig, mgr *service.Resources) (*kvReader, error) {
	r := &kvReader{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if r.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if r.bucket, err = conf.FieldString(kvFieldBucket); err != nil {
		return nil, err
	}

	if r.key, err = conf.FieldString(kviFieldKey); err != nil {
		return nil, err
	}

	if r.ignoreDeletes, err = conf.FieldBool(kviFieldIgnoreDeletes); err != nil {
		return nil, err
	}

	if r.includeHistory, err = conf.FieldBool(kviFieldIncludeHistory); err != nil {
		return nil, err
	}

	if r.metaOnly, err = conf.FieldBool(kviFieldMetaOnly); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *kvReader) Connect(ctx context.Context) (err error) {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.natsConn != nil {
		return nil
	}

	defer func() {
		if err != nil {
			if r.watcher != nil {
				_ = r.watcher.Stop()
			}
			if r.natsConn != nil {
				r.natsConn.Close()
			}
		}
	}()

	if r.natsConn, err = r.connDetails.get(ctx); err != nil {
		return err
	}

	js, err := jetstream.New(r.natsConn)
	if err != nil {
		return err
	}

	kv, err := js.KeyValue(ctx, r.bucket)
	if err != nil {
		return err
	}

	var watchOpts []jetstream.WatchOpt
	if r.ignoreDeletes {
		watchOpts = append(watchOpts, jetstream.IgnoreDeletes())
	}
	if r.includeHistory {
		watchOpts = append(watchOpts, jetstream.IncludeHistory())
	}
	if r.metaOnly {
		watchOpts = append(watchOpts, jetstream.MetaOnly())
	}

	r.watcher, err = kv.Watch(ctx, r.key, watchOpts...)
	if err != nil {
		return err
	}
	return nil
}

func (r *kvReader) disconnect() {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.watcher != nil {
		_ = r.watcher.Stop()
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

	for {
		var entry jetstream.KeyValueEntry
		var open bool
		select {
		case entry, open = <-watcher.Updates():
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		if !open {
			r.disconnect()
			return nil, nil, service.ErrNotConnected
		}

		if entry == nil {
			continue
		}

		r.log.With(
			metaKVBucket, entry.Bucket(),
			metaKVKey, entry.Key(),
			metaKVRevision, entry.Revision(),
			metaKVOperation, entry.Operation().String(),
		).Debugf("Received kv bucket update")

		return newMessageFromKVEntry(entry), func(ctx context.Context, res error) error {
			return nil
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
