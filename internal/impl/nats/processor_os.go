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
	ospOperationField  = "operation"
	ospBucketField     = "bucket"
	ospObjectNameField = "object_name"
)

type ospOperationType string

const (
	ospOperationGet ospOperationType = "get"
	ospOperationPut ospOperationType = "put"
)

var ospOperations = map[string]string{
	string(ospOperationGet): "Returns the latest value for the `object_name`.",
	string(ospOperationPut): "Places a new value for the `object_name` in the object store.",
}

func natsOSProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("1.8.0").
		Summary("Perform operations on a NATS object store bucket.").
		Description(`
### Operations

The NATS object store processor supports ` + "`get`" + ` and ` + "`put`" + ` operations via the ` + "`operation`" + ` field.

` + connectionNameDescription() + authDescription()).
		Fields(Docs("object store", []*service.ConfigField{
			service.NewStringAnnotatedEnumField(ospOperationField, ospOperations).
				Description("The operation to perform on the Object Store bucket."),
			service.NewInterpolatedStringField(ospObjectNameField).
				Description("The name of the object in the object store to operate on."),
			service.NewBoolField("create_bucket").
				Description("Whether to automatically create the bucket if it doesn't exist.").
				Advanced().
				Default(false),
		}...)...)
}

func init() {
	err := service.RegisterProcessor(
		"nats_object_store", natsOSProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newOSProcessor(conf, mgr)
		},
	)
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type osProcessor struct {
	connDetails  connectionDetails
	bucket       string
	operation    ospOperationType
	objectName   *service.InterpolatedString
	createBucket bool

	log     *service.Logger
	shutSig *shutdown.Signaller

	connMut  sync.Mutex
	natsConn *nats.Conn
	os       jetstream.ObjectStore
}

func newOSProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*osProcessor, error) {
	osp := &osProcessor{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if osp.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if osp.bucket, err = conf.FieldString(ospBucketField); err != nil {
		return nil, err
	}

	if osp.createBucket, err = conf.FieldBool("create_bucket"); err != nil {
		return nil, err
	}

	if operation, err := conf.FieldString(ospOperationField); err != nil {
		return nil, err
	} else {
		osp.operation = ospOperationType(operation)
	}

	if osp.objectName, err = conf.FieldInterpolatedString(ospObjectNameField); err != nil {
		return nil, err
	}

	err = osp.connect(context.Background())
	return osp, err
}

func (osp *osProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	osp.connMut.Lock()
	os := osp.os
	osp.connMut.Unlock()

	objectName, err := osp.objectName.TryString(msg)
	if err != nil {
		return nil, err
	}

	switch osp.operation {
	case ospOperationGet:
		objectBytes, err := os.GetBytes(ctx, objectName)
		if err != nil {
			return nil, err
		}
		m := msg.Copy()
		m.SetBytes(objectBytes)
		return service.MessageBatch{m}, nil

	case ospOperationPut:
		msgBytes, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}

		_, err = osp.os.PutBytes(ctx, objectName, msgBytes)
		if err != nil {
			return nil, err
		}

		return service.MessageBatch{msg}, nil

	default:
		return nil, fmt.Errorf("invalid nats_object_store operation: %s", osp.operation)
	}
}

func (osp *osProcessor) Close(ctx context.Context) error {
	go func() {
		osp.disconnect()
		osp.shutSig.TriggerHasStopped()
	}()
	select {
	case <-osp.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (osp *osProcessor) connect(ctx context.Context) (err error) {
	osp.connMut.Lock()
	defer osp.connMut.Unlock()

	if osp.natsConn != nil {
		return nil
	}

	defer func() {
		if err != nil {
			if osp.natsConn != nil {
				osp.natsConn.Close()
			}
		}
	}()

	if osp.natsConn, err = osp.connDetails.get(ctx); err != nil {
		return err
	}

	js, err := jetstream.New(osp.natsConn)
	if err != nil {
		return err
	}

	// Check if bucket exists first, create only if config allows
	osp.os, err = js.ObjectStore(ctx, osp.bucket)
	if err != nil {
		if osp.createBucket {
			osp.os, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
				Bucket: osp.bucket,
			})
			if err != nil {
				return fmt.Errorf("failed to create bucket %s: %w", osp.bucket, err)
			}
			osp.log.Infof("Created bucket %s", osp.bucket)
		} else {
			return fmt.Errorf("bucket %s does not exist and create_bucket is false", osp.bucket)
		}
	}
	return nil
}

func (osp *osProcessor) disconnect() {
	osp.connMut.Lock()
	defer osp.connMut.Unlock()

	if osp.natsConn != nil {
		osp.natsConn.Close()
		osp.natsConn = nil
	}
	osp.os = nil
}
