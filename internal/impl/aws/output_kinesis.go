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

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/retries"
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
	if conf.backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
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
		Description(`
Both the `+"`partition_key`"+`(required) and `+"`hash_key`"+` (optional) fields can be dynamically set using function interpolations described xref:configuration:interpolation.adoc#bloblang-queries[here]. When sending batched messages the interpolations are performed per message part.

== Credentials

By default Redpanda Connect will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more in xref:guides:cloud/aws.adoc[].`+service.OutputPerformanceDocs(true, true)).
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
		Fields(retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
}

func init() {
	service.MustRegisterBatchOutput("aws_kinesis", koOutputSpec(),
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
			err = fmt.Errorf("batch message %d exceeds the maximum Kinesis payload limit of 1 MiB", i)
			a.log.With("error", err).Error("Failed to prepare record")
			return err
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
		return service.ErrNotConnected
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
				return fmt.Errorf("%v records failed to be delivered within backoff policy", l)
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
	return nil
}

func (*kinesisWriter) Close(context.Context) error {
	return nil
}
