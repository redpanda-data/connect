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

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// SQS Output Fields
	sqsoFieldURL             = "url"
	sqsoFieldMessageGroupID  = "message_group_id"
	sqsoFieldMessageDedupeID = "message_deduplication_id"
	sqsoFieldDelaySeconds    = "delay_seconds"
	sqsoFieldMetadata        = "metadata"
	sqsoFieldBatching        = "batching"

	sqsMaxRecordsCount = 10
)

type sqsoConfig struct {
	URL                    string
	MessageGroupID         *service.InterpolatedString
	MessageDeduplicationID *service.InterpolatedString
	DelaySeconds           *service.InterpolatedString

	Metadata    *service.MetadataExcludeFilter
	aconf       aws.Config
	backoffCtor func() backoff.BackOff
}

func sqsoConfigFromParsed(pConf *service.ParsedConfig) (conf sqsoConfig, err error) {
	if conf.URL, err = pConf.FieldString(sqsoFieldURL); err != nil {
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
	if conf.backoffCtor, err = pure.CommonRetryBackOffCtorFromParsed(pConf); err != nil {
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
		Description(output.Description(true, true, `
Metadata values are sent along with the payload as attributes with the data type String. If the number of metadata values in a message exceeds the message attribute limit (10) then the top ten keys ordered alphabetically will be selected.

The fields `+"`message_group_id`, `message_deduplication_id` and `delay_seconds`"+` can be set dynamically using [function interpolations](/docs/configuration/interpolation#bloblang-queries), which are resolved individually for each message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).`)).
		Fields(
			service.NewStringField(sqsoFieldURL).Description("The URL of the target SQS queue."),
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
		).
		Fields(config.SessionFields()...).
		Fields(pure.CommonRetryBackOffFields(0, "1s", "5s", "30s")...)
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
		if isValidSQSAttribute(k, value.IToString(v)) {
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

	entries := []types.SendMessageBatchRequestEntry{}
	attrMap := map[string]sqsAttributes{}

	for i := 0; i < len(batch); i++ {
		id := strconv.Itoa(i)
		attrs, err := a.getSQSAttributes(batch, i)
		if err != nil {
			return err
		}

		attrMap[id] = attrs

		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:                     &id,
			MessageBody:            attrs.content,
			MessageAttributes:      attrs.attrMap,
			MessageGroupId:         attrs.groupID,
			MessageDeduplicationId: attrs.dedupeID,
			DelaySeconds:           attrs.delaySeconds,
		})
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(a.conf.URL),
		Entries:  entries,
	}

	// trim input length to max sqs batch size
	if len(entries) > sqsMaxRecordsCount {
		input.Entries, entries = entries[:sqsMaxRecordsCount], entries[sqsMaxRecordsCount:]
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
				return component.ErrTimeout
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
				return component.ErrTimeout
			case <-a.closeChan:
				return err
			}
		}

		// add remaining records to batch
		l := len(input.Entries)
		if n := len(entries); n > 0 && l < sqsMaxRecordsCount {
			if remaining := sqsMaxRecordsCount - l; remaining < n {
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
