package aws

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/firehose"
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Kinesis Firehose Output Fields
	kfoFieldStream   = "stream"
	kfoFieldBatching = "batching"
)

type kfoConfig struct {
	Stream string

	aconf       aws.Config
	backoffCtor func() backoff.BackOff
}

func kfoConfigFromParsed(pConf *service.ParsedConfig) (conf kfoConfig, err error) {
	if conf.Stream, err = pConf.FieldString(kfoFieldStream); err != nil {
		return
	}
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}
	if conf.backoffCtor, err = pure.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
		return
	}
	return
}

func kfoOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary(`Sends messages to a Kinesis Firehose delivery stream.`).
		Description(`
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).

## Performance

This output benefits from sending multiple messages in flight in parallel for improved performance. You can tune the max number of in flight messages (or message batches) with the field `+"`max_in_flight`"+`.

This output benefits from sending messages as a batch for improved performance. Batches can be formed at both the input and output level. You can find out more [in this doc](/docs/configuration/batching).
`).
		Fields(
			service.NewStringField(kfoFieldStream).
				Description("The stream to publish messages to."),
			service.NewOutputMaxInFlightField(),
			service.NewBatchPolicyField(kfoFieldBatching),
		).
		Fields(config.SessionFields()...).
		Fields(pure.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
}

func init() {
	err := service.RegisterBatchOutput("aws_kinesis_firehose", kfoOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(kfoFieldBatching); err != nil {
				return
			}
			var wConf kfoConfig
			if wConf, err = kfoConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newKinesisFirehoseWriter(wConf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

type firehoseAPI interface {
	DescribeDeliveryStream(ctx context.Context, params *firehose.DescribeDeliveryStreamInput, optFns ...func(*firehose.Options)) (*firehose.DescribeDeliveryStreamOutput, error)
	PutRecordBatch(ctx context.Context, params *firehose.PutRecordBatchInput, optFns ...func(*firehose.Options)) (*firehose.PutRecordBatchOutput, error)
}

type kinesisFirehoseWriter struct {
	firehose firehoseAPI

	conf kfoConfig
	log  *service.Logger
}

func newKinesisFirehoseWriter(conf kfoConfig, log *service.Logger) (*kinesisFirehoseWriter, error) {
	return &kinesisFirehoseWriter{
		conf: conf,
		log:  log,
	}, nil
}

// toRecords converts an individual benthos message into a slice of Kinesis Firehose
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *kinesisFirehoseWriter) toRecords(batch service.MessageBatch) ([]types.Record, error) {
	entries := make([]types.Record, len(batch))

	for i, p := range batch {
		var entry types.Record
		var err error
		if entry.Data, err = p.AsBytes(); err != nil {
			return nil, err
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("batch message %d exceeds the maximum Kinesis Firehose payload limit of 1 MiB", i)
			return nil, component.ErrMessageTooLarge
		}

		entries[i] = entry
	}

	return entries, nil
}

//------------------------------------------------------------------------------

// Connect creates a new Kinesis Firehose client and ensures that the target
// Kinesis Firehose delivery stream.
func (a *kinesisFirehoseWriter) Connect(ctx context.Context) error {
	if a.firehose != nil {
		return nil
	}

	a.firehose = firehose.NewFromConfig(a.conf.aconf)
	if _, err := a.firehose.DescribeDeliveryStream(ctx, &firehose.DescribeDeliveryStreamInput{
		DeliveryStreamName: aws.String(a.conf.Stream),
	}); err != nil {
		return err
	}
	return nil
}

// WriteBatch attempts to write message contents to a target Kinesis
// Firehose delivery stream in batches of 500. If throttling is detected, failed
// messages are retried according to the configurable backoff settings.
func (a *kinesisFirehoseWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if a.firehose == nil {
		return service.ErrNotConnected
	}

	backOff := a.conf.backoffCtor()

	records, err := a.toRecords(batch)
	if err != nil {
		return err
	}

	input := &firehose.PutRecordBatchInput{
		Records:            records,
		DeliveryStreamName: aws.String(a.conf.Stream),
	}

	// trim input record length to max kinesis firehose batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []types.Record
	for len(input.Records) > 0 {
		wait := backOff.NextBackOff()

		// batch write to kinesis firehose
		output, err := a.firehose.PutRecordBatch(ctx, input)
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
					if *entry.ErrorCode != "ServiceUnavailableException" {
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
