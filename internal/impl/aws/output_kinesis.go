package aws

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Kinesis Output Fields
	koFieldStream       = "stream"
	koFieldHashKey      = "hash_key"
	koFieldPartitionKey = "partition_key"
	koFieldBatching     = "batching"
)

type koConfig struct {
	Stream       string
	HashKey      *service.InterpolatedString
	PartitionKey *service.InterpolatedString

	aconf       aws.Config
	backoffCtor func() backoff.BackOff
}

func koConfigFromParsed(pConf *service.ParsedConfig) (conf koConfig, err error) {
	if conf.Stream, err = pConf.FieldString(koFieldStream); err != nil {
		return
	}
	if conf.PartitionKey, err = pConf.FieldInterpolatedString(koFieldPartitionKey); err != nil {
		return
	}
	if pConf.Contains(koFieldHashKey) {
		if conf.HashKey, err = pConf.FieldInterpolatedString(koFieldHashKey); err != nil {
			return
		}
	}
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}
	if conf.backoffCtor, err = pure.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
		return
	}
	return
}

func koOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary(`Sends messages to a Kinesis stream.`).
		Description(output.Description(true, true, `
Both the `+"`partition_key`"+`(required) and `+"`hash_key`"+` (optional) fields can be dynamically set using function interpolations described [here](/docs/configuration/interpolation#bloblang-queries). When sending batched messages the interpolations are performed per message part.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).`)).
		Fields(
			service.NewStringField(koFieldStream).
				Description("The stream to publish messages to. Streams can either be specified by their name or full ARN.").
				Examples("foo", "arn:aws:kinesis:*:111122223333:stream/my-stream"),
			service.NewInterpolatedStringField(koFieldPartitionKey).
				Description("A required key for partitioning messages."),
			service.NewInterpolatedStringField(koFieldHashKey).
				Description("A optional hash key for partitioning messages.").
				Optional().
				Advanced(),
			service.NewOutputMaxInFlightField().
				Description("The maximum number of parallel message batches to have in flight at any given time."),
			service.NewBatchPolicyField(koFieldBatching),
		).
		Fields(config.SessionFields()...).
		Fields(pure.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
}

func init() {
	err := service.RegisterBatchOutput("aws_kinesis", koOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(koFieldBatching); err != nil {
				return
			}
			var wConf koConfig
			if wConf, err = koConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newKinesisWriter(wConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

const (
	kinesisMaxRecordsCount = 500
	mebibyte               = 1048576
)

type kinesisAPI interface {
	PutRecords(ctx context.Context, params *kinesis.PutRecordsInput, optFns ...func(*kinesis.Options)) (*kinesis.PutRecordsOutput, error)
}

type kinesisWriter struct {
	conf      koConfig
	streamARN string
	kinesis   kinesisAPI
	log       *service.Logger
}

func newKinesisWriter(conf koConfig, mgr *service.Resources) (*kinesisWriter, error) {
	return &kinesisWriter{
		conf: conf,
		log:  mgr.Logger(),
	}, nil
}

// toRecords converts an individual benthos message into a slice of Kinesis
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *kinesisWriter) toRecords(batch service.MessageBatch) ([]types.PutRecordsRequestEntry, error) {
	entries := make([]types.PutRecordsRequestEntry, len(batch))

	err := batch.WalkWithBatchedErrors(func(i int, m *service.Message) error {
		partKey, err := batch.TryInterpolatedString(i, a.conf.PartitionKey)
		if err != nil {
			return fmt.Errorf("partition key interpolation error: %w", err)
		}

		mBytes, err := m.AsBytes()
		if err != nil {
			return err
		}
		entry := types.PutRecordsRequestEntry{
			Data:         mBytes,
			PartitionKey: aws.String(partKey),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis payload limit of 1 MiB\n", i)
			return component.ErrMessageTooLarge
		}

		var hashKey string
		if a.conf.HashKey != nil {
			if hashKey, err = batch.TryInterpolatedString(i, a.conf.HashKey); err != nil {
				return fmt.Errorf("hash key interpolation error: %w", err)
			}
		}
		if hashKey != "" {
			entry.ExplicitHashKey = aws.String(hashKey)
		}

		entries[i] = entry
		return nil
	})

	return entries, err
}

func (a *kinesisWriter) Connect(ctx context.Context) error {
	if a.kinesis != nil {
		return nil
	}

	k := kinesis.NewFromConfig(a.conf.aconf)

	in := &kinesis.DescribeStreamInput{}
	if strings.HasPrefix(a.conf.Stream, "arn:") {
		in.StreamARN = &a.conf.Stream
	} else {
		in.StreamName = &a.conf.Stream
	}

	out, err := k.DescribeStream(ctx, in)
	if err != nil {
		return err
	}

	a.streamARN = *out.StreamDescription.StreamARN
	a.kinesis = k
	return nil
}

func (a *kinesisWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if a.kinesis == nil {
		return component.ErrNotConnected
	}

	backOff := a.conf.backoffCtor()

	records, err := a.toRecords(batch)
	if err != nil {
		return err
	}

	input := &kinesis.PutRecordsInput{
		Records:   records,
		StreamARN: &a.streamARN,
	}

	// trim input record length to max kinesis batch size
	if len(records) > kinesisMaxRecordsCount {
		input.Records, records = records[:kinesisMaxRecordsCount], records[kinesisMaxRecordsCount:]
	} else {
		records = nil
	}

	var failed []types.PutRecordsRequestEntry
	backOff.Reset()
	for len(input.Records) > 0 {
		wait := backOff.NextBackOff()

		// batch write to kinesis
		output, err := a.kinesis.PutRecords(ctx, input)
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
					case "ProvisionedThroughputExceededException":
						a.log.Errorf("Kinesis record write request rate too high, either the frequency or the size of the data exceeds your available throughput.")
					case "KMSThrottlingException":
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
