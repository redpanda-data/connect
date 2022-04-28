package azure

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-storage-queue-go/azqueue"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/azure/shared"
	"github.com/benthosdev/benthos/v4/internal/interop"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	ooutput "github.com/benthosdev/benthos/v4/internal/old/output"
	"github.com/benthosdev/benthos/v4/internal/old/output/writer"
)

func init() {
	err := bundle.AllOutputs.Add(bundle.OutputConstructorFromSimple(func(c ooutput.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newAzureQueueStorageOutput(c, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "azure_queue_storage",
		Status:  docs.StatusBeta,
		Version: "3.36.0",
		Summary: `
Sends messages to an Azure Storage Queue.`,
		Description: output.Description(true, true, `
Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

In order to set the `+"`queue_name`"+` you can use function interpolations described [here](/docs/configuration/interpolation#bloblang-queries), which are calculated per message of a batch.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("storage_account", "The storage account to upload messages to. This field is ignored if `storage_connection_string` is set."),
			docs.FieldString("storage_access_key", "The storage account access key. This field is ignored if `storage_connection_string` is set."),
			docs.FieldString("storage_connection_string", "A storage account connection string. This field is required if `storage_account` and `storage_access_key` are not set."),
			docs.FieldString("queue_name", "The name of the target Queue Storage queue.").IsInterpolated(),
			docs.FieldString(
				"ttl", "The TTL of each individual message as a duration string. Defaults to 0, meaning no retention period is set",
				"60s", "5m", "36h",
			).IsInterpolated().Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").AtVersion("3.45.0"),
			policy.FieldSpec(),
		).ChildDefaultAndTypesFromStruct(ooutput.NewAzureQueueStorageConfig()),
		Categories: []string{
			"Services",
			"Azure",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newAzureQueueStorageOutput(conf ooutput.Config, mgr interop.Manager, log log.Modular, stats metrics.Type) (output.Streamed, error) {
	s, err := newAzureQueueStorageWriter(conf.AzureQueueStorage, mgr, log)
	if err != nil {
		return nil, err
	}
	w, err := ooutput.NewAsyncWriter("azure_queue_storage", conf.AzureQueueStorage.MaxInFlight, s, log, stats)
	if err != nil {
		return nil, err
	}
	return ooutput.NewBatcherFromConfig(conf.AzureQueueStorage.Batching, ooutput.OnlySinglePayloads(w), mgr, log, stats)
}

type azureQueueStorageWriter struct {
	conf ooutput.AzureQueueStorageConfig

	queueName  *field.Expression
	ttl        *field.Expression
	serviceURL *azqueue.ServiceURL

	log log.Modular
}

func newAzureQueueStorageWriter(conf ooutput.AzureQueueStorageConfig, mgr interop.Manager, log log.Modular) (*azureQueueStorageWriter, error) {
	serviceURL, err := shared.GetQueueServiceURL(conf.StorageAccount, conf.StorageAccessKey, conf.StorageConnectionString)
	if err != nil {
		return nil, err
	}
	s := &azureQueueStorageWriter{
		conf:       conf,
		log:        log,
		serviceURL: serviceURL,
	}

	if s.ttl, err = mgr.BloblEnvironment().NewField(conf.TTL); err != nil {
		return nil, fmt.Errorf("failed to parse ttl expression: %v", err)
	}

	if s.queueName, err = mgr.BloblEnvironment().NewField(conf.QueueName); err != nil {
		return nil, fmt.Errorf("failed to parse queue name expression: %v", err)
	}

	return s, nil
}

func (a *azureQueueStorageWriter) ConnectWithContext(ctx context.Context) error {
	return nil
}

func (a *azureQueueStorageWriter) WriteWithContext(ctx context.Context, msg *message.Batch) error {
	return writer.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		queueURL := a.serviceURL.NewQueueURL(a.queueName.String(i, msg))
		msgURL := queueURL.NewMessagesURL()
		var ttl *time.Duration
		if ttls := a.ttl.String(i, msg); ttls != "" {
			td, err := time.ParseDuration(ttls)
			if err != nil {
				a.log.Debugf("TTL must be a duration: %v\n", err)
				return err
			}
			ttl = &td
		}
		timeToLive := func() time.Duration {
			if ttl != nil {
				return *ttl
			}
			return 0
		}()
		message := string(p.Get())
		_, err := msgURL.Enqueue(ctx, message, 0, timeToLive)
		if err != nil {
			if cerr, ok := err.(azqueue.StorageError); ok {
				if cerr.ServiceCode() == azqueue.ServiceCodeQueueNotFound {
					ctx := context.Background()
					_, err = queueURL.Create(ctx, azqueue.Metadata{})
					if err != nil {
						return fmt.Errorf("error creating queue: %v", err)
					}
					_, err := msgURL.Enqueue(ctx, message, 0, 0)
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

func (a *azureQueueStorageWriter) CloseAsync() {
}

func (a *azureQueueStorageWriter) WaitForClose(time.Duration) error {
	return nil
}
