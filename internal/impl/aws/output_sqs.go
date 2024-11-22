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
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/bloblang"
	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/redpanda-data/connect/v4/internal/impl/aws/config"
	"github.com/redpanda-data/connect/v4/internal/retries"
)

const (
	// SQS Output Fields
	sqsoFieldURL             = "url"
	sqsoFieldMessageGroupID  = "message_group_id"
	sqsoFieldMessageDedupeID = "message_deduplication_id"
	sqsoFieldDelaySeconds    = "delay_seconds"
	sqsoFieldMetadata        = "metadata"
	sqsoFieldBatching        = "batching"
	sqsoFieldMaxRecordsCount = "max_records_per_request"
)

type sqsoConfig struct {
	URL                    *service.InterpolatedString
	MessageGroupID         *service.InterpolatedString
	MessageDeduplicationID *service.InterpolatedString
	DelaySeconds           *service.InterpolatedString

	MaxRecordsCount int

	Metadata    *service.MetadataExcludeFilter
	aconf       aws.Config
	backoffCtor func() backoff.BackOff
}

func sqsoConfigFromParsed(pConf *service.ParsedConfig) (conf sqsoConfig, err error) {
	if conf.URL, err = pConf.FieldInterpolatedString(sqsoFieldURL); err != nil {
		return
	}
	if pConf.Contains(sqsoFieldMessageGroupID) {
		if conf.MessageGroupID, err = pConf.FieldInterpolatedString(sqsoFieldMessageGroupID); err != nil {
			return
		}
	}
	if pConf.Contains(sqsoFieldMessageDedupeID) {
		if conf.MessageDeduplicationID, err = pConf.FieldInterpolatedString(sqsoFieldMessageDedupeID); err != nil {
			return
		}
	}
	if pConf.Contains(sqsoFieldDelaySeconds) {
		if conf.DelaySeconds, err = pConf.FieldInterpolatedString(sqsoFieldDelaySeconds); err != nil {
			return
		}
	}
	if conf.Metadata, err = pConf.FieldMetadataExcludeFilter(sqsoFieldMetadata); err != nil {
		return
	}
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}
	if conf.backoffCtor, err = retries.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
		return
	}
	if conf.MaxRecordsCount, err = pConf.FieldInt(sqsoFieldMaxRecordsCount); err != nil {
		return
	}
	if conf.MaxRecordsCount <= 0 || conf.MaxRecordsCount > 10 {
		err = errors.New("field " + sqsoFieldMaxRecordsCount + " must be >0 and <= 10")
		return
	}
	return
}

func sqsoOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary(`Sends messages to an SQS queue.`).
		Description(`
Metadata values are sent along with the payload as attributes with the data type String. If the number of metadata values in a message exceeds the message attribute limit (10) then the top ten keys ordered alphabetically will be selected.

The fields `+"`message_group_id`, `message_deduplication_id` and `delay_seconds`"+` can be set dynamically using xref:configuration:interpolation.adoc#bloblang-queries[function interpolations], which are resolved individually for each message of a batch.

== Credentials

By default Redpanda Connect will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more in xref:guides:cloud/aws.adoc[].`+service.OutputPerformanceDocs(true, true)).
		Fields(
			service.NewInterpolatedStringField(sqsoFieldURL).Description("The URL of the target SQS queue."),
			service.NewInterpolatedStringField(sqsoFieldMessageGroupID).
				Description("An optional group ID to set for messages.").
				Optional(),
			service.NewInterpolatedStringField(sqsoFieldMessageDedupeID).
				Description("An optional deduplication ID to set for messages.").
				Optional(),
			service.NewInterpolatedStringField(sqsoFieldDelaySeconds).
				Description("An optional delay time in seconds for message. Value between 0 and 900").
				Optional(),
			service.NewOutputMaxInFlightField().
				Description("The maximum number of parallel message batches to have in flight at any given time."),
			service.NewMetadataExcludeFilterField(snsoFieldMetadata).
				Description("Specify criteria for which metadata values are sent as headers."),
			service.NewBatchPolicyField(koFieldBatching),
			service.NewIntField(sqsoFieldMaxRecordsCount).
				Description("Customize the maximum number of records delivered in a single SQS request. This value must be greater than 0 but no greater than 10.").
				Default(10).
				LintRule(`if this <= 0 || this > 10 { "this field must be >0 and <=10" } `).
				Advanced(),
		).
		Fields(config.SessionFields()...).
		Fields(retries.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
}

func init() {
	err := service.RegisterBatchOutput("aws_sqs", sqsoOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy(sqsoFieldBatching); err != nil {
				return
			}
			var wConf sqsoConfig
			if wConf, err = sqsoConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newSQSWriter(wConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type sqsWriter struct {
	conf sqsoConfig
	sqs  sqsAPI

	closer    sync.Once
	closeChan chan struct{}

	log *service.Logger
}

func newSQSWriter(conf sqsoConfig, mgr *service.Resources) (*sqsWriter, error) {
	s := &sqsWriter{
		conf:      conf,
		log:       mgr.Logger(),
		closeChan: make(chan struct{}),
	}
	return s, nil
}

func (a *sqsWriter) Connect(ctx context.Context) error {
	if a.sqs != nil {
		return nil
	}

	a.sqs = sqs.NewFromConfig(a.conf.aconf)
	return nil
}

type sqsAttributes struct {
	attrMap      map[string]types.MessageAttributeValue
	groupID      *string
	dedupeID     *string
	delaySeconds int32
	content      *string
}

var sqsAttributeKeyInvalidCharRegexp = regexp.MustCompile(`(^\.)|(\.\.)|(^aws\.)|(^amazon\.)|(\.$)|([^a-z0-9_\-.]+)`)

func isValidSQSAttribute(k, v string) bool {
	return len(sqsAttributeKeyInvalidCharRegexp.FindStringIndex(strings.ToLower(k))) == 0
}

func (a *sqsWriter) getSQSAttributes(batch service.MessageBatch, i int) (sqsAttributes, error) {
	msg := batch[i]
	keys := []string{}
	_ = a.conf.Metadata.WalkMut(msg, func(k string, v any) error {
		if isValidSQSAttribute(k, bloblang.ValueToString(v)) {
			keys = append(keys, k)
		} else {
			a.log.Debugf("Rejecting metadata key '%v' due to invalid characters\n", k)
		}
		return nil
	})
	var values map[string]types.MessageAttributeValue
	if len(keys) > 0 {
		sort.Strings(keys)
		values = map[string]types.MessageAttributeValue{}

		for i, k := range keys {
			v, _ := msg.MetaGet(k)
			dataType := "String"
			values[k] = types.MessageAttributeValue{
				DataType:    &dataType,
				StringValue: &v,
			}
			if i == 9 {
				break
			}
		}
	}

	var groupID, dedupeID *string
	var delaySeconds int32
	if a.conf.MessageGroupID != nil {
		groupIDStr, err := batch.TryInterpolatedString(i, a.conf.MessageGroupID)
		if err != nil {
			return sqsAttributes{}, fmt.Errorf("group id interpolation: %w", err)
		}
		groupID = aws.String(groupIDStr)
	}
	if a.conf.MessageDeduplicationID != nil {
		dedupeIDStr, err := batch.TryInterpolatedString(i, a.conf.MessageDeduplicationID)
		if err != nil {
			return sqsAttributes{}, fmt.Errorf("dedupe id interpolation: %w", err)
		}
		dedupeID = aws.String(dedupeIDStr)
	}
	if a.conf.DelaySeconds != nil {
		delaySecondsStr, err := batch.TryInterpolatedString(i, a.conf.DelaySeconds)
		if err != nil {
			return sqsAttributes{}, fmt.Errorf("delay seconds interpolation: %w", err)
		}
		delaySecondsInt64, err := strconv.ParseInt(delaySecondsStr, 10, 64)
		if err != nil {
			return sqsAttributes{}, fmt.Errorf("delay seconds invalid input: %w", err)
		}
		if delaySecondsInt64 < 0 || delaySecondsInt64 > 900 {
			return sqsAttributes{}, errors.New("delay seconds must be between 0 and 900")
		}
		delaySeconds = int32(delaySecondsInt64)
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return sqsAttributes{}, err
	}

	return sqsAttributes{
		attrMap:      values,
		groupID:      groupID,
		dedupeID:     dedupeID,
		delaySeconds: delaySeconds,
		content:      aws.String(string(msgBytes)),
	}, nil
}

func (a *sqsWriter) WriteBatch(ctx context.Context, batch service.MessageBatch) error {
	if a.sqs == nil {
		return service.ErrNotConnected
	}

	backOff := a.conf.backoffCtor()

	entries := map[string][]types.SendMessageBatchRequestEntry{}
	attrMap := map[string]sqsAttributes{}

	urlExecutor := batch.InterpolationExecutor(a.conf.URL)

	for i := 0; i < len(batch); i++ {
		id := strconv.Itoa(i)
		attrs, err := a.getSQSAttributes(batch, i)
		if err != nil {
			return err
		}

		attrMap[id] = attrs

		url, err := urlExecutor.TryString(i)
		if err != nil {
			return fmt.Errorf("error interpolating %s: %w", sqsoFieldURL, err)
		}
		entries[url] = append(entries[url], types.SendMessageBatchRequestEntry{
			Id:                     &id,
			MessageBody:            attrs.content,
			MessageAttributes:      attrs.attrMap,
			MessageGroupId:         attrs.groupID,
			MessageDeduplicationId: attrs.dedupeID,
			DelaySeconds:           attrs.delaySeconds,
		})
	}

	for url, entries := range entries {
		backOff.Reset()
		if err := a.writeChunk(ctx, url, entries, attrMap, backOff); err != nil {
			return err
		}
	}

	return nil
}

func (a *sqsWriter) writeChunk(
	ctx context.Context,
	url string,
	entries []types.SendMessageBatchRequestEntry,
	attrMap map[string]sqsAttributes,
	backOff backoff.BackOff,
) error {
	input := &sqs.SendMessageBatchInput{
		QueueUrl: &url,
		Entries:  entries,
	}

	// trim input length to max sqs batch size
	if len(entries) > a.conf.MaxRecordsCount {
		input.Entries, entries = entries[:a.conf.MaxRecordsCount], entries[a.conf.MaxRecordsCount:]
	} else {
		entries = nil
	}

	var err error
	for len(input.Entries) > 0 {
		wait := backOff.NextBackOff()

		var batchResult *sqs.SendMessageBatchOutput
		if batchResult, err = a.sqs.SendMessageBatch(ctx, input); err != nil {
			a.log.Warnf("SQS error: %v\n", err)
			// bail if a message is too large or all retry attempts expired
			if wait == backoff.Stop {
				return err
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			case <-a.closeChan:
				return err
			}
			continue
		}

		if unproc := batchResult.Failed; len(unproc) > 0 {
			input.Entries = []types.SendMessageBatchRequestEntry{}
			for _, v := range unproc {
				if v.SenderFault {
					err = fmt.Errorf("record failed with code: %v, message: %v", *v.Code, *v.Message)
					a.log.Errorf("SQS record error: %v\n", err)
					return err
				}
				aMap := attrMap[*v.Id]
				input.Entries = append(input.Entries, types.SendMessageBatchRequestEntry{
					Id:                     v.Id,
					MessageBody:            aMap.content,
					MessageAttributes:      aMap.attrMap,
					MessageGroupId:         aMap.groupID,
					MessageDeduplicationId: aMap.dedupeID,
				})
			}
			err = fmt.Errorf("failed to send %v messages", len(unproc))
		} else {
			input.Entries = nil
		}

		if err != nil {
			if wait == backoff.Stop {
				break
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			case <-a.closeChan:
				return err
			}
		}

		// add remaining records to batch
		l := len(input.Entries)
		if n := len(entries); n > 0 && l < a.conf.MaxRecordsCount {
			if remaining := a.conf.MaxRecordsCount - l; remaining < n {
				input.Entries, entries = append(input.Entries, entries[:remaining]...), entries[remaining:]
			} else {
				input.Entries, entries = append(input.Entries, entries...), nil
			}
		}
	}

	return err
}

func (a *sqsWriter) Close(context.Context) error {
	a.closer.Do(func() {
		close(a.closeChan)
	})
	return nil
}
