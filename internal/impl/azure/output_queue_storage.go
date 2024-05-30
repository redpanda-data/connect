package azure

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Queue Storage Output Fields
	qsoFieldQueueName = "queue_name"
	qsoFieldTTL       = "ttl"
	qsoFieldBatching  = "batching"
)

type qsoConfig struct {
	client    *azqueue.ServiceClient
	QueueName *service.InterpolatedString
	TTL       *service.InterpolatedString
}

func qsoConfigFromParsed(pConf *service.ParsedConfig) (conf qsoConfig, err error) {
	if conf.client, err = queueServiceClientFromParsed(pConf); err != nil {
		return
	}
	if conf.QueueName, err = pConf.FieldInterpolatedString(qsoFieldQueueName); err != nil {
		return
	}
	if conf.TTL, err = pConf.FieldInterpolatedString(qsoFieldTTL); err != nil {
		return
	}
	return
}

func qsoSpec() *service.ConfigSpec {
	return azureComponentSpec(false).
		Beta().
		Version("3.36.0").
		Summary(`Sends messages to an Azure Storage Queue.`).
		Description(`
Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

In order to set the `+"`queue_name`"+` you can use function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here], which are calculated per message of a batch.`+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewInterpolatedStringField(qsoFieldQueueName).
				Description("The name of the target Queue Storage queue."),
			service.NewInterpolatedStringField(qsoFieldTTL).
				Description("The TTL of each individual message as a duration string. Defaults to 0, meaning no retention period is set").
				Example("60s").Example("5m").Example("36h").
				Advanced().
				Default(""),
			service.NewOutputMaxInFlightField().
				Description("The maximum number of parallel message batches to have in flight at any given time."),
			service.NewBatchPolicyField(qsoFieldBatching),
		)
}

func init() {
	err := service.RegisterBatchOutput("azure_queue_storage", qsoSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batcher service.BatchPolicy, mif int, err error) {
			var pConf qsoConfig
			if pConf, err = qsoConfigFromParsed(conf); err != nil {
				return
			}
			if batcher, err = conf.FieldBatchPolicy(qsoFieldBatching); err != nil {
				return
			}
			if mif, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if out, err = newAzureQueueStorageWriter(pConf, mgr.Logger()); err != nil {
				return
			}
			return
		})
	if err != nil {
		panic(err)
	}
}

type azureQueueStorageWriter struct {
	conf qsoConfig
	log  *service.Logger
}

func newAzureQueueStorageWriter(conf qsoConfig, log *service.Logger) (*azureQueueStorageWriter, error) {
	s := &azureQueueStorageWriter{
		conf: conf,
		log:  log,
	}
	return s, nil
}

func (a *azureQueueStorageWriter) Connect(ctx context.Context) error {
	return nil
}

func (a *azureQueueStorageWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	return batch.WalkWithBatchedErrors(func(i int, msg *service.Message) error {
		queueNameStr, err := batch.TryInterpolatedString(i, a.conf.QueueName)
		if err != nil {
			return fmt.Errorf("queue name interpolation error: %w", err)
		}
		queue := a.conf.client.NewQueueClient(queueNameStr)

		ttls, err := batch.TryInterpolatedString(i, a.conf.TTL)
		if err != nil {
			return fmt.Errorf("ttl interpolation error: %w", err)
		}

		var ttl *time.Duration
		if ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				a.log.Debugf("TTL must be a duration: %v\n", err)
				return err
			}
			ttl = &td
		}
		timeToLive := func() *int32 {
			if ttl != nil {
				ttlAsSeconds := int32(ttl.Seconds())
				return &ttlAsSeconds
			}
			return nil
		}()

		mBytes, err := msg.AsBytes()
		if err != nil {
			return err
		}
		message := string(mBytes)
		opts := &azqueue.EnqueueMessageOptions{TimeToLive: timeToLive}
		if _, err = queue.EnqueueMessage(ctx, message, opts); err != nil {
			if cerr, ok := err.(*azcore.ResponseError); ok {
				if cerr.StatusCode == http.StatusNotFound {
					_, err = queue.Create(ctx, nil)
					if err != nil {
						return fmt.Errorf("error creating queue: %v", err)
					}
					_, err := queue.EnqueueMessage(ctx, message, opts)
					if err != nil {
						return fmt.Errorf("error retrying to enqueue message: %v", err)
					}
				} else {
					return fmt.Errorf("storage error message: %v", err)
				}
			} else {
				return fmt.Errorf("error enqueuing message: %v", err)
			}
		}
		return nil
	})
}

func (a *azureQueueStorageWriter) Close(context.Context) error {
	return nil
}
