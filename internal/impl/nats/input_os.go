package nats

import (
	"context"
	"fmt"
	"sync"

	"github.com/Jeffail/shutdown"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	osiBucketField         = "bucket"
	osiIgnoreDeletesField  = "ignore_deletes"
	osiIncludeHistoryField = "include_history"
	osiMetadataOnlyField   = "meta_only"
	osiTest                = "test"
)

func natsOSInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("1.8.0").
		Summary("Watches for updates in a nats object store.").
		Description(`
### Metadata

This input adds the following metadata fields to each message:

` + "``` text" + `
- nats_object_store_name
- nats_object_store_description
- nats_object_store_headers
- nats_object_store_metadata
- nats_object_store_bucket
- nats_object_store_nuid
- nats_object_store_size
- nats_object_store_modtime
- nats_object_store_chunks
- nats_object_store_digest
- nats_object_store_deleted

` + "```" + `

` + connectionNameDescription() + authDescription()).
		Fields(Docs("object store", []*service.ConfigField{
			service.NewBoolField("create_bucket").
				Description("Whether to automatically create the bucket if it doesn't exist.").
				Advanced().
				Default(false),
			service.NewAutoRetryNacksToggleField(),
		}...)...)
}

func init() {
	err := service.RegisterInput(
		"nats_object_store", natsOSInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			reader, err := newOSInput(conf, mgr)
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

//------------------------------------------------------------------------------

type osInput struct {
	connDetails  connectionDetails
	bucket       string
	createBucket bool

	log     *service.Logger
	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	watcher  jetstream.ObjectWatcher
	os       jetstream.ObjectStore
}

func newOSInput(conf *service.ParsedConfig, mgr *service.Resources) (*osInput, error) {
	osi := osInput{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if osi.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if osi.bucket, err = conf.FieldString(osiBucketField); err != nil {
		return nil, err
	}

	if osi.createBucket, err = conf.FieldBool("create_bucket"); err != nil {
		return nil, err
	}

	return &osi, nil
}

//------------------------------------------------------------------------------

func (osi *osInput) Connect(ctx context.Context) (err error) {
	osi.connMut.Lock()
	defer osi.connMut.Unlock()

	if osi.natsConn != nil {
		return nil
	}

	defer func() {
		if err != nil {
			if osi.watcher != nil {
				_ = osi.watcher.Stop()
			}
			if osi.natsConn != nil {
				osi.natsConn.Close()
			}
		}
	}()

	if osi.natsConn, err = osi.connDetails.get(ctx); err != nil {
		return err
	}

	js, err := jetstream.New(osi.natsConn)
	if err != nil {
		return err
	}

	// Check if bucket exists first, create only if config allows
	osi.os, err = js.ObjectStore(ctx, osi.bucket)
	if err != nil {
		if osi.createBucket {
			osi.os, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
				Bucket: osi.bucket,
			})
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", osi.bucket, err)
			}
			osi.log.Infof("Created bucket %s", osi.bucket)
		} else {
			return fmt.Errorf("bucket %s does not exist and create_bucket is false", osi.bucket)
		}
	}

	osi.watcher, err = osi.os.Watch(ctx, nil)
	if err != nil {
		return err
	}
	return nil
}

func (osi *osInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	osi.connMut.Lock()
	watcher := osi.watcher
	osi.connMut.Unlock()

	if watcher == nil {
		return nil, nil, service.ErrNotConnected
	}

	for {
		var objectInfo *jetstream.ObjectInfo
		var open bool

		select {
		case objectInfo, open = <-watcher.Updates():
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}

		if !open {
			osi.disconnect()
			return nil, nil, service.ErrNotConnected
		}

		if objectInfo == nil {
			continue
		}

		msg, err := osi.newMessageFromObjectInfo(ctx, objectInfo)
		if err != nil {
			return nil, nil, err
		}

		return msg, func(ctx context.Context, res error) error {
			return nil
		}, nil
	}
}

func (osi *osInput) Close(ctx context.Context) error {
	go func() {
		osi.disconnect()
		osi.shutSig.TriggerHasStopped()
	}()
	select {
	case <-osi.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

//------------------------------------------------------------------------------

func (osi *osInput) disconnect() {
	osi.connMut.Lock()
	defer osi.connMut.Unlock()

	if osi.watcher != nil {
		_ = osi.watcher.Stop()
		osi.watcher = nil
	}
	if osi.natsConn != nil {
		osi.natsConn.Close()
		osi.natsConn = nil
	}
}

func (osi *osInput) newMessageFromObjectInfo(ctx context.Context, object *jetstream.ObjectInfo) (*service.Message, error) {
	objectBytes, err := osi.os.GetBytes(ctx, object.Name)
	if err != nil {
		return nil, err
	}

	msg := service.NewMessage(objectBytes)

	msg.MetaSetMut("nats_object_store_name", object.Name)
	msg.MetaSetMut("nats_object_store_description", object.Description)
	msg.MetaSetMut("nats_object_store_headers", object.Headers)
	msg.MetaSetMut("nats_object_store_metadata", object.Metadata)
	msg.MetaSetMut("nats_object_store_bucket", object.Bucket)
	msg.MetaSetMut("nats_object_store_nuid", object.NUID)
	msg.MetaSetMut("nats_object_store_size", object.Size)
	msg.MetaSetMut("nats_object_store_modtime", object.ModTime)
	msg.MetaSetMut("nats_object_store_chunks", object.Chunks)
	msg.MetaSetMut("nats_object_store_digest", object.Digest)
	msg.MetaSetMut("nats_object_store_deleted", object.Deleted)

	return msg, nil
}
