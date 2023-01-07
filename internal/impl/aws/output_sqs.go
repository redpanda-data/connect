package aws

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/batch/policy"
	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/batcher"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
	"github.com/benthosdev/benthos/v4/internal/old/util/retries"
)

const (
	sqsMaxRecordsCount = 10
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newSQSWriterFromConfig(c.AWSSQS, nm)
	}), docs.ComponentSpec{
		Name:    "aws_sqs",
		Version: "3.36.0",
		Summary: `
Sends messages to an SQS queue.`,
		Description: output.Description(true, true, `
Metadata values are sent along with the payload as attributes with the data type
String. If the number of metadata values in a message exceeds the message
attribute limit (10) then the top ten keys ordered alphabetically will be
selected.

The fields `+"`message_group_id` and `message_deduplication_id`"+` can be
set dynamically using
[function interpolations](/docs/configuration/interpolation#bloblang-queries), which are
resolved individually for each message of a batch.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("url", "The URL of the target SQS queue."),
			docs.FieldString("message_group_id", "An optional group ID to set for messages.").IsInterpolated(),
			docs.FieldString("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated(),
			docs.FieldInt("max_in_flight", "The maximum number of parallel message batches to have in flight at any given time."),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are sent as headers.").WithChildren(metadata.ExcludeFilterFields()...),
			policy.FieldSpec(),
		).WithChildren(sess.FieldSpecs()...).WithChildren(retries.FieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewAmazonSQSConfig()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newSQSWriterFromConfig(conf output.AmazonSQSConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	s, err := newSQSWriter(conf, mgr)
	if err != nil {
		return nil, err
	}
	w, err := output.NewAsyncWriter("aws_sqs", conf.MaxInFlight, s, mgr)
	if err != nil {
		return w, err
	}
	return batcher.NewFromConfig(conf.Batching, w, mgr)
}

type sqsWriter struct {
	conf output.AmazonSQSConfig
	sqs  sqsiface.SQSAPI

	backoffCtor func() backoff.BackOff

	groupID    *field.Expression
	dedupeID   *field.Expression
	metaFilter *metadata.ExcludeFilter

	closer    sync.Once
	closeChan chan struct{}

	log log.Modular
}

func newSQSWriter(conf output.AmazonSQSConfig, mgr bundle.NewManagement) (*sqsWriter, error) {
	s := &sqsWriter{
		conf:      conf,
		log:       mgr.Logger(),
		closeChan: make(chan struct{}),
	}

	var err error
	if id := conf.MessageGroupID; len(id) > 0 {
		if s.groupID, err = mgr.BloblEnvironment().NewField(id); err != nil {
			return nil, fmt.Errorf("failed to parse group ID expression: %v", err)
		}
	}
	if id := conf.MessageDeduplicationID; len(id) > 0 {
		if s.dedupeID, err = mgr.BloblEnvironment().NewField(id); err != nil {
			return nil, fmt.Errorf("failed to parse dedupe ID expression: %v", err)
		}
	}
	if s.metaFilter, err = conf.Metadata.Filter(); err != nil {
		return nil, fmt.Errorf("failed to construct metadata filter: %w", err)
	}

	if s.backoffCtor, err = conf.Config.GetCtor(); err != nil {
		return nil, err
	}
	return s, nil
}

func (a *sqsWriter) Connect(ctx context.Context) error {
	if a.sqs != nil {
		return nil
	}

	sess, err := GetSessionFromConf(a.conf.SessionConfig.Config)
	if err != nil {
		return err
	}

	a.sqs = sqs.New(sess)
	a.log.Infof("Sending messages to Amazon SQS URL: %v\n", a.conf.URL)
	return nil
}

type sqsAttributes struct {
	attrMap  map[string]*sqs.MessageAttributeValue
	groupID  *string
	dedupeID *string
	content  *string
}

var sqsAttributeKeyInvalidCharRegexp = regexp.MustCompile(`(^\.)|(\.\.)|(^aws\.)|(^amazon\.)|(\.$)|([^a-z0-9_\-.]+)`)

func isValidSQSAttribute(k, v string) bool {
	return len(sqsAttributeKeyInvalidCharRegexp.FindStringIndex(strings.ToLower(k))) == 0
}

func (a *sqsWriter) getSQSAttributes(msg message.Batch, i int) (sqsAttributes, error) {
	p := msg.Get(i)
	keys := []string{}
	_ = a.metaFilter.Iter(p, func(k string, v any) error {
		if isValidSQSAttribute(k, query.IToString(v)) {
			keys = append(keys, k)
		} else {
			a.log.Debugf("Rejecting metadata key '%v' due to invalid characters\n", k)
		}
		return nil
	})
	var values map[string]*sqs.MessageAttributeValue
	if len(keys) > 0 {
		sort.Strings(keys)
		values = map[string]*sqs.MessageAttributeValue{}

		for i, k := range keys {
			values[k] = &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(p.MetaGetStr(k)),
			}
			if i == 9 {
				break
			}
		}
	}

	var groupID, dedupeID *string
	if a.groupID != nil {
		groupIDStr, err := a.groupID.String(i, msg)
		if err != nil {
			return sqsAttributes{}, fmt.Errorf("group id interpolation: %w", err)
		}
		groupID = aws.String(groupIDStr)
	}
	if a.dedupeID != nil {
		dedupeIDStr, err := a.dedupeID.String(i, msg)
		if err != nil {
			return sqsAttributes{}, fmt.Errorf("dedupe id interpolation: %w", err)
		}
		dedupeID = aws.String(dedupeIDStr)
	}

	return sqsAttributes{
		attrMap:  values,
		groupID:  groupID,
		dedupeID: dedupeID,
		content:  aws.String(string(p.AsBytes())),
	}, nil
}

func (a *sqsWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	if a.sqs == nil {
		return component.ErrNotConnected
	}

	backOff := a.backoffCtor()

	entries := []*sqs.SendMessageBatchRequestEntry{}
	attrMap := map[string]sqsAttributes{}
	if err := msg.Iter(func(i int, p *message.Part) error {
		id := strconv.Itoa(i)
		attrs, err := a.getSQSAttributes(msg, i)
		if err != nil {
			return err
		}

		attrMap[id] = attrs

		entries = append(entries, &sqs.SendMessageBatchRequestEntry{
			Id:                     aws.String(id),
			MessageBody:            attrs.content,
			MessageAttributes:      attrs.attrMap,
			MessageGroupId:         attrs.groupID,
			MessageDeduplicationId: attrs.dedupeID,
		})
		return nil
	}); err != nil {
		return err
	}

	input := &sqs.SendMessageBatchInput{
		QueueUrl: aws.String(a.conf.URL),
		Entries:  entries,
	}

	// trim input input length to max sqs batch size
	if len(entries) > sqsMaxRecordsCount {
		input.Entries, entries = entries[:sqsMaxRecordsCount], entries[sqsMaxRecordsCount:]
	} else {
		entries = nil
	}

	var err error
	for len(input.Entries) > 0 {
		wait := backOff.NextBackOff()

		var batchResult *sqs.SendMessageBatchOutput
		if batchResult, err = a.sqs.SendMessageBatch(input); err != nil {
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
			input.Entries = []*sqs.SendMessageBatchRequestEntry{}
			for _, v := range unproc {
				if *v.SenderFault {
					err = fmt.Errorf("record failed with code: %v, message: %v", *v.Code, *v.Message)
					a.log.Errorf("SQS record error: %v\n", err)
					return err
				}
				aMap := attrMap[*v.Id]
				input.Entries = append(input.Entries, &sqs.SendMessageBatchRequestEntry{
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
