// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package azure

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	azq "github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	// Queue Storage Input Fields
	qsiFieldQueueName                = "queue_name"
	qsiFieldDequeueVisibilityTimeout = "dequeue_visibility_timeout"
	qsiFieldTrackProperties          = "track_properties"
)

type qsiConfig struct {
	client                   *azq.ServiceClient
	QueueName                *service.InterpolatedString
	DequeueVisibilityTimeout time.Duration
	MaxInFlight              int
	TrackProperties          bool
}

func qsiConfigFromParsed(pConf *service.ParsedConfig) (conf qsiConfig, err error) {
	if conf.client, err = queueServiceClientFromParsed(pConf); err != nil {
		return
	}
	if conf.QueueName, err = pConf.FieldInterpolatedString(qsiFieldQueueName); err != nil {
		return
	}
	if conf.DequeueVisibilityTimeout, err = pConf.FieldDuration(qsiFieldDequeueVisibilityTimeout); err != nil {
		return
	}
	if conf.MaxInFlight, err = pConf.FieldMaxInFlight(); err != nil {
		return
	}
	if conf.TrackProperties, err = pConf.FieldBool(qsiFieldTrackProperties); err != nil {
		return
	}
	return
}

func qsiSpec() *service.ConfigSpec {
	return azureComponentSpec().
		Stable().
		Version("3.42.0").
		Summary(`Dequeue objects from an Azure Storage Queue.`).
		Description(`
This input adds the following metadata fields to each message:

`+"```"+`
- queue_storage_insertion_time
- queue_storage_queue_name
- queue_storage_message_lag (if 'track_properties' set to true)
- All user defined queue metadata
`+"```"+`

Only one authentication method is required, `+"`storage_connection_string`"+` or `+"`storage_account` and `storage_access_key`"+`. If both are set then the `+"`storage_connection_string`"+` is given priority.

Messages are deleted from the queue after they are acknowledged. A rejected message is not deleted. Instead its visibility timeout is reset to zero so that the queue redelivers it right away.`).
		Fields(
			service.NewInterpolatedStringField(qsiFieldQueueName).
				Description(`The name of the source storage queue.`).
				Example("foo_queue"),
			service.NewDurationField(qsiFieldDequeueVisibilityTimeout).
				Description("The timeout duration until a dequeued message gets visible again, 30s by default").
				Version("3.45.0").
				Advanced().
				Default("30s"),
			service.NewInputMaxInFlightField().
				Description("The maximum number of unprocessed messages to fetch at a given time.").
				Default(10).
				Advanced(),
			service.NewBoolField(qsiFieldTrackProperties).
				Description("If set to `true` the queue is polled on each read request for information such as the queue message lag. These properties are added to consumed messages as metadata, but will also have a negative performance impact.").
				ShortDescription("Poll the queue on each read for properties such as message lag, at a performance cost.").
				Default(false).
				Advanced(),
			service.NewStringField(bscFieldStorageSASToken).Deprecated().Default(""), // This field was never implemented
		)
}

func init() {
	service.MustRegisterBatchInput("azure_queue_storage", qsiSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			pConf, err := qsiConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newAzureQueueStorage(pConf, mgr)
		})
}

type azureQueueStorage struct {
	conf qsiConfig
	log  *service.Logger
}

func newAzureQueueStorage(conf qsiConfig, mgr *service.Resources) (*azureQueueStorage, error) {
	a := &azureQueueStorage{
		conf: conf,
		log:  mgr.Logger(),
	}
	return a, nil
}

func (*azureQueueStorage) Connect(context.Context) error {
	return nil
}

func (a *azureQueueStorage) ReadBatch(ctx context.Context) (batch service.MessageBatch, ackFn service.AckFunc, err error) {
	var queueName string
	if queueName, err = a.conf.QueueName.TryString(service.NewMessage(nil)); err != nil {
		err = fmt.Errorf("queue name interpolation error: %w", err)
		return
	}
	queueClient := a.conf.client.NewQueueClient(queueName)
	var approxMsgCount int32
	if a.conf.TrackProperties {
		if props, err := queueClient.GetProperties(ctx, nil); err == nil {
			if amc := props.ApproximateMessagesCount; amc != nil {
				approxMsgCount = *amc
			}
		}
	}
	visibilityTimeout := int32(a.conf.DequeueVisibilityTimeout.Seconds())
	numMessages := int32(a.conf.MaxInFlight)
	dequeue, err := queueClient.DequeueMessages(ctx, &azq.DequeueMessagesOptions{
		NumberOfMessages:  &numMessages,
		VisibilityTimeout: &visibilityTimeout,
	})
	if err != nil {
		if cerr, ok := err.(*azcore.ResponseError); ok {
			if cerr.StatusCode == http.StatusNotFound {
				_, err = queueClient.Create(ctx, nil)
				return nil, nil, err
			}
			return nil, nil, fmt.Errorf("storage error message: %v", cerr)
		}
		return nil, nil, fmt.Errorf("error dequeing message: %v", err)
	}
	n := int32(len(dequeue.Messages))
	props, _ := queueClient.GetProperties(ctx, nil)
	dqm := make([]*azq.DequeuedMessage, n)
	for i, queueMsg := range dequeue.Messages {
		part := service.NewMessage([]byte(*queueMsg.MessageText))
		if queueMsg.InsertionTime != nil {
			part.MetaSetMut("queue_storage_insertion_time", queueMsg.InsertionTime.Format(time.RFC3339))
		}
		part.MetaSetMut("queue_storage_queue_name", queueName)
		if a.conf.TrackProperties {
			msgLag := 0
			if approxMsgCount >= n {
				msgLag = int(approxMsgCount - n)
			}
			part.MetaSetMut("queue_storage_message_lag", msgLag)
		}
		for k, v := range props.Metadata {
			if v != nil {
				part.MetaSetMut(k, *v)
			}
		}
		batch = append(batch, part)
		dqm[i] = queueMsg
	}
	return batch, func(ctx context.Context, res error) error {
		if res != nil {
			a.releaseMessages(ctx, queueClient, dqm)
			return nil
		}
		return deleteMessages(ctx, queueClient, dqm)
	}, nil
}

// deleteMessages removes acknowledged messages from the queue. Every message
// is attempted even when an earlier one fails, so a single failed delete does
// not leave the rest of the batch to be redelivered.
func deleteMessages(ctx context.Context, queueClient *azq.QueueClient, dqm []*azq.DequeuedMessage) error {
	var errs []error
	for _, queueMsg := range dqm {
		if _, err := queueClient.DeleteMessage(ctx, *queueMsg.MessageID, *queueMsg.PopReceipt, nil); err != nil {
			errs = append(errs, fmt.Errorf("error deleting message %s: %w", *queueMsg.MessageID, err))
		}
	}
	return errors.Join(errs...)
}

// releaseMessages makes rejected messages visible again right away so they
// are redelivered without waiting for the dequeue visibility timeout. A
// failure here is not fatal: the message stays in the queue and becomes
// visible again after the timeout expires.
func (a *azureQueueStorage) releaseMessages(ctx context.Context, queueClient *azq.QueueClient, dqm []*azq.DequeuedMessage) {
	visibilityTimeout := int32(0)
	for _, queueMsg := range dqm {
		if _, err := queueClient.UpdateMessage(ctx, *queueMsg.MessageID, *queueMsg.PopReceipt, *queueMsg.MessageText, &azq.UpdateMessageOptions{
			VisibilityTimeout: &visibilityTimeout,
		}); err != nil {
			a.log.Warnf("Failed to reset visibility of rejected message %s, it is redelivered after the visibility timeout expires: %v", *queueMsg.MessageID, err)
		}
	}
}

func (*azureQueueStorage) Close(context.Context) error {
	return nil
}
