package aws

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/aws/aws-sdk-go/service/firehose/firehoseiface"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		kin, err := newKinesisFirehoseWriter(c.AWSKinesisFirehose, nm.Logger())
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("aws_kinesis_firehose", c.AWSKinesisFirehose.MaxInFlight, kin, nm)
		if err != nil {
			return w, err
		}
		return batcher.NewFromConfig(c.AWSKinesisFirehose.Batching, w, nm)
	}), docs.ComponentSpec{
		Name:    "aws_kinesis_firehose",
		Version: "3.36.0",
		Summary: `
Sends messages to a Kinesis Firehose delivery stream.`,
		Description: output.Description(true, true, `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("stream", "The stream to publish messages to."),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
		).WithChildren(sess.FieldSpecs()...).WithChildren(retries.FieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewKinesisFirehoseConfig()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

type kinesisFirehoseWriter struct {
	conf output.KinesisFirehoseConfig

	session  *session.Session
	firehose firehoseiface.FirehoseAPI

	backoffCtor func() backoff.BackOff
	streamName  *string

	log log.Modular
}

func newKinesisFirehoseWriter(conf output.KinesisFirehoseConfig, log log.Modular) (*kinesisFirehoseWriter, error) {
	k := kinesisFirehoseWriter{
		conf:       conf,
		log:        log,
		streamName: aws.String(conf.Stream),
	}

	var err error
	if k.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	return &k, nil
}

// toRecords converts an individual benthos message into a slice of Kinesis Firehose
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *kinesisFirehoseWriter) toRecords(msg message.Batch) ([]*firehose.Record, error) {
	entries := make([]*firehose.Record, msg.Len())

	err := msg.Iter(func(i int, p *message.Part) error {
		entry := firehose.Record{
			Data: p.AsBytes(),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis Firehose payload limit of 1 MiB\n", i)
			return component.ErrMessageTooLarge
		}

		entries[i] = &entry
		return nil
	})

	return entries, err
}

//------------------------------------------------------------------------------

// Connect creates a new Kinesis Firehose client and ensures that the target
// Kinesis Firehose delivery stream.
func (a *kinesisFirehoseWriter) Connect(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := GetSessionFromConf(a.conf.SessionConfig.Config)
	if err != nil {
		return err
	}

	a.session = sess
	a.firehose = firehose.New(sess)

	if _, err := a.firehose.DescribeDeliveryStream(&firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: a.streamName,
	}); err != nil {
		return err
	}

	a.log.Infof("Sending messages to Kinesis Firehose delivery stream: %v\n", a.conf.Stream)
	return nil
}

// Write attempts to write message contents to a target Kinesis Firehose delivery
// stream in batches of 500. If throttling is detected, failed messages are retried
// according to the configurable backoff settings.
func (a *kinesisFirehoseWriter) Write(msg message.Batch) error {
	return a.WriteBatch(context.Background(), msg)
}

// WriteBatch attempts to write message contents to a target Kinesis
// Firehose delivery stream in batches of 500. If throttling is detected, failed
// messages are retried according to the configurable backoff settings.
func (a *kinesisFirehoseWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	if a.session == nil {
		return component.ErrNotConnected
	}

	backOff := a.backoffCtor()

	records, err := a.toRecords(msg)
	if err != nil {
		return err
	}

	input := &firehose.PutRecordBatchInput{
		Records:            records,
		DeliveryStreamName: a.streamName,
	}

	// trim input record length to max kinesis firehose batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []*firehose.Record
	for len(input.Records) > 0 {
		wait := backOff.NextBackOff()

		// batch write to kinesis firehose
		output, err := a.firehose.PutRecordBatch(input)
		if err != nil {
			a.log.Warnf("kinesis firehose error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			continue
		}

		// requeue any individual records that failed due to throttling
		failed = nil
		if output.FailedPutCount != nil {
			for i, entry := range output.RequestResponses {
				if entry.ErrorCode != nil {
					failed = append(failed, input.Records[i])
					if *entry.ErrorCode != firehose.ErrCodeServiceUnavailableException {
						err = fmt.Errorf("record failed with code [%s] %s: %+v", *entry.ErrorCode, *entry.ErrorMessage, input.Records[i])
						a.log.Errorf("kinesis firehose record error: %v\n", err)
						return err
					}
				}
			}
		}
		input.Records = failed

		// if throttling errors detected, pause briefly
		l := len(failed)
		if l > 0 {
			a.log.Warnf("scheduling retry of throttled records (%d)\n", l)
			if wait == backoff.Stop {
				return component.ErrTimeout
			}
			time.Sleep(wait)
		}

		// add remaining records to batch
		if n := len(records); n > 0 && l < kinesisMaxRecordsCount {
			if remaining := kinesisMaxRecordsCount - l; remaining < n {
				input.Records, records = append(input.Records, records[:remaining]...), records[remaining:]
			} else {
				input.Records, records = append(input.Records, records...), nil
			}
		}
	}
	return err
}

func (a *kinesisFirehoseWriter) Close(context.Context) error {
	return nil
}
