package aws

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
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

const (
	kinesisMaxRecordsCount = 500
	mebibyte               = 1048576
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		kin, err := newKinesisWriter(c.AWSKinesis, nm)
		if err != nil {
			return nil, err
		}
		w, err := output.NewAsyncWriter("aws_kinesis", c.AWSKinesis.MaxInFlight, kin, nm)
		if err != nil {
			return w, err
		}
		return batcher.NewFromConfig(c.AWSKinesis.Batching, w, nm)
	}), docs.ComponentSpec{
		Name:    "aws_kinesis",
		Version: "3.36.0",
		Summary: `
Sends messages to a Kinesis stream.`,
		Description: output.Description(true, true, `
Both the `+"`partition_key`"+`(required) and `+"`hash_key`"+` (optional)
fields can be dynamically set using function interpolations described
[here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages the
interpolations are performed per message part.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("stream", "The stream to publish messages to."),
			docs.FieldString("partition_key", "A required key for partitioning messages.").IsInterpolated(),
			docs.FieldString("hash_key", "A optional hash key for partitioning messages.").IsInterpolated().Advanced(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			policy.FieldSpec(),
		).WithChildren(sess.FieldSpecs()...).WithChildren(retries.FieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewKinesisConfig()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

type kinesisWriter struct {
	conf output.KinesisConfig

	session *session.Session
	kinesis kinesisiface.KinesisAPI

	backoffCtor  func() backoff.BackOff
	hashKey      *field.Expression
	partitionKey *field.Expression
	streamName   *string

	log log.Modular
}

func newKinesisWriter(conf output.KinesisConfig, mgr bundle.NewManagement) (*kinesisWriter, error) {
	if conf.PartitionKey == "" {
		return nil, errors.New("partition key must not be empty")
	}

	k := kinesisWriter{
		conf:       conf,
		log:        mgr.Logger(),
		streamName: aws.String(conf.Stream),
	}
	var err error
	if k.hashKey, err = mgr.BloblEnvironment().NewField(conf.HashKey); err != nil {
		return nil, fmt.Errorf("failed to parse hash key expression: %v", err)
	}
	if k.partitionKey, err = mgr.BloblEnvironment().NewField(conf.PartitionKey); err != nil {
		return nil, fmt.Errorf("failed to parse partition key expression: %v", err)
	}
	if k.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	return &k, nil
}

// toRecords converts an individual benthos message into a slice of Kinesis
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *kinesisWriter) toRecords(msg message.Batch) ([]*kinesis.PutRecordsRequestEntry, error) {
	entries := make([]*kinesis.PutRecordsRequestEntry, msg.Len())

	err := msg.Iter(func(i int, p *message.Part) error {
		partKey, err := a.partitionKey.String(i, msg)
		if err != nil {
			return fmt.Errorf("partition key interpolation error: %w", err)
		}
		entry := kinesis.PutRecordsRequestEntry{
			Data:         p.AsBytes(),
			PartitionKey: aws.String(partKey),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis payload limit of 1 MiB\n", i)
			return component.ErrMessageTooLarge
		}

		hashKey, err := a.hashKey.String(i, msg)
		if err != nil {
			return fmt.Errorf("hash key interpolation error: %w", err)
		}
		if hashKey != "" {
			entry.ExplicitHashKey = aws.String(hashKey)
		}

		entries[i] = &entry
		return nil
	})

	return entries, err
}

func (a *kinesisWriter) Connect(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := GetSessionFromConf(a.conf.SessionConfig.Config)
	if err != nil {
		return err
	}

	a.session = sess
	a.kinesis = kinesis.New(sess)

	if err := a.kinesis.WaitUntilStreamExists(&kinesis.DescribeStreamInput{
		StreamName: a.streamName,
	}); err != nil {
		return err
	}

	a.log.Infof("Sending messages to Kinesis stream: %v\n", a.conf.Stream)
	return nil
}

func (a *kinesisWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	if a.session == nil {
		return component.ErrNotConnected
	}

	backOff := a.backoffCtor()

	records, err := a.toRecords(msg)
	if err != nil {
		return err
	}

	input := &kinesis.PutRecordsInput{
		Records:    records,
		StreamName: a.streamName,
	}

	// trim input record length to max kinesis batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []*kinesis.PutRecordsRequestEntry
	backOff.Reset()
	for len(input.Records) > 0 {
		wait := backOff.NextBackOff()

		// batch write to kinesis
		output, err := a.kinesis.PutRecords(input)
		if err != nil {
			a.log.Warnf("kinesis error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			continue
		}

		// requeue any individual records that failed due to throttling
		failed = nil
		if output.FailedRecordCount != nil {
			for i, entry := range output.Records {
				if entry.ErrorCode != nil {
					failed = append(failed, input.Records[i])
					switch *entry.ErrorCode {
					case kinesis.ErrCodeProvisionedThroughputExceededException:
						a.log.Errorf("Kinesis record write request rate too high, either the frequency or the size of the data exceeds your available throughput.")
					case kinesis.ErrCodeKMSThrottlingException:
						a.log.Errorf("Kinesis record write request throttling exception, the send traffic exceeds your request quota.")
					default:
						err = fmt.Errorf("record failed with code [%s] %s: %+v", *entry.ErrorCode, *entry.ErrorMessage, input.Records[i])
						a.log.Errorf("kinesis record write error: %v\n", err)
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

func (a *kinesisWriter) Close(context.Context) error {
	return nil
}
