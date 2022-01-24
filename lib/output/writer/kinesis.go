package writer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

const (
	kinesisMaxRecordsCount = 500
	mebibyte               = 1048576
)

type sessionConfig struct {
	sess.Config `json:",inline" yaml:",inline"`
}

// KinesisConfig contains configuration fields for the Kinesis output type.
type KinesisConfig struct {
	sessionConfig  `json:",inline" yaml:",inline"`
	Stream         string `json:"stream" yaml:"stream"`
	HashKey        string `json:"hash_key" yaml:"hash_key"`
	PartitionKey   string `json:"partition_key" yaml:"partition_key"`
	MaxInFlight    int    `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config `json:",inline" yaml:",inline"`
	Batching       batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewKinesisConfig creates a new Config with default values.
func NewKinesisConfig() KinesisConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"
	return KinesisConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		Stream:       "",
		HashKey:      "",
		PartitionKey: "",
		MaxInFlight:  1,
		Config:       rConf,
		Batching:     batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// Kinesis is a benthos writer.Type implementation that writes messages to an
// Amazon Kinesis stream.
type Kinesis struct {
	conf KinesisConfig

	session *session.Session
	kinesis kinesisiface.KinesisAPI

	backoffCtor  func() backoff.BackOff
	hashKey      *field.Expression
	partitionKey *field.Expression
	streamName   *string

	log   log.Modular
	stats metrics.Type

	mThrottled       metrics.StatCounter
	mThrottledF      metrics.StatCounter
	mPartsThrottled  metrics.StatCounter
	mPartsThrottledF metrics.StatCounter
}

// NewKinesisV2 creates a new Amazon Kinesis writer.Type.
func NewKinesisV2(
	conf KinesisConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*Kinesis, error) {
	if conf.PartitionKey == "" {
		return nil, errors.New("partition key must not be empty")
	}

	k := Kinesis{
		conf:            conf,
		log:             log,
		stats:           stats,
		mPartsThrottled: stats.GetCounter("parts.send.throttled"),
		mThrottled:      stats.GetCounter("send.throttled"),
		streamName:      aws.String(conf.Stream),
	}
	var err error
	if k.hashKey, err = interop.NewBloblangField(mgr, conf.HashKey); err != nil {
		return nil, fmt.Errorf("failed to parse hash key expression: %v", err)
	}
	if k.partitionKey, err = interop.NewBloblangField(mgr, conf.PartitionKey); err != nil {
		return nil, fmt.Errorf("failed to parse partition key expression: %v", err)
	}
	if k.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	return &k, nil
}

//------------------------------------------------------------------------------

// toRecords converts an individual benthos message into a slice of Kinesis
// batch put entries by promoting each message part into a single part message
// and passing each new message through the partition and hash key interpolation
// process, allowing the user to define the partition and hash key per message
// part.
func (a *Kinesis) toRecords(msg types.Message) ([]*kinesis.PutRecordsRequestEntry, error) {
	entries := make([]*kinesis.PutRecordsRequestEntry, msg.Len())

	err := msg.Iter(func(i int, p types.Part) error {
		entry := kinesis.PutRecordsRequestEntry{
			Data:         p.Get(),
			PartitionKey: aws.String(a.partitionKey.String(i, msg)),
		}

		if len(entry.Data) > mebibyte {
			a.log.Errorf("part %d exceeds the maximum Kinesis payload limit of 1 MiB\n", i)
			return types.ErrMessageTooLarge
		}

		if hashKey := a.hashKey.String(i, msg); hashKey != "" {
			entry.ExplicitHashKey = aws.String(hashKey)
		}

		entries[i] = &entry
		return nil
	})

	return entries, err
}

//------------------------------------------------------------------------------

// Connect creates a new Kinesis client and ensures that the target Kinesis
// stream exists.
func (a *Kinesis) Connect() error {
	return a.ConnectWithContext(context.Background())
}

// ConnectWithContext creates a new Kinesis client and ensures that the target
// Kinesis stream exists.
func (a *Kinesis) ConnectWithContext(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
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

// Write attempts to write message contents to a target Kinesis stream in
// batches of 500. If throttling is detected, failed messages are retried
// according to the configurable backoff settings.
func (a *Kinesis) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target Kinesis
// stream in batches of 500. If throttling is detected, failed messages are
// retried according to the configurable backoff settings.
func (a *Kinesis) WriteWithContext(ctx context.Context, msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
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
			a.mThrottled.Incr(1)
			a.mPartsThrottled.Incr(int64(l))
			a.log.Warnf("scheduling retry of throttled records (%d)\n", l)
			if wait == backoff.Stop {
				return types.ErrTimeout
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

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *Kinesis) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *Kinesis) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
