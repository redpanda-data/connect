package writer

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/Jeffail/benthos/v3/lib/util/retries"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

const (
	sqsMaxRecordsCount = 10
)

//------------------------------------------------------------------------------

// AmazonSQSConfig contains configuration fields for the output AmazonSQS type.
type AmazonSQSConfig struct {
	sessionConfig          `json:",inline" yaml:",inline"`
	URL                    string                       `json:"url" yaml:"url"`
	MessageGroupID         string                       `json:"message_group_id" yaml:"message_group_id"`
	MessageDeduplicationID string                       `json:"message_deduplication_id" yaml:"message_deduplication_id"`
	Metadata               metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	MaxInFlight            int                          `json:"max_in_flight" yaml:"max_in_flight"`
	retries.Config         `json:",inline" yaml:",inline"`
	Batching               batch.PolicyConfig `json:"batching" yaml:"batching"`
}

// NewAmazonSQSConfig creates a new Config with default values.
func NewAmazonSQSConfig() AmazonSQSConfig {
	rConf := retries.NewConfig()
	rConf.Backoff.InitialInterval = "1s"
	rConf.Backoff.MaxInterval = "5s"
	rConf.Backoff.MaxElapsedTime = "30s"

	return AmazonSQSConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		URL:                    "",
		MessageGroupID:         "",
		MessageDeduplicationID: "",
		Metadata:               metadata.NewExcludeFilterConfig(),
		MaxInFlight:            1,
		Config:                 rConf,
		Batching:               batch.NewPolicyConfig(),
	}
}

//------------------------------------------------------------------------------

// AmazonSQS is a benthos writer.Type implementation that writes messages to an
// Amazon SQS queue.
type AmazonSQS struct {
	conf AmazonSQSConfig
	sqs  sqsiface.SQSAPI

	backoffCtor func() backoff.BackOff

	groupID    *field.Expression
	dedupeID   *field.Expression
	metaFilter *metadata.ExcludeFilter

	closer    sync.Once
	closeChan chan struct{}

	log   log.Modular
	stats metrics.Type
}

// NewAmazonSQSV2 creates a new Amazon SQS writer.Type.
func NewAmazonSQSV2(
	conf AmazonSQSConfig,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (*AmazonSQS, error) {
	s := &AmazonSQS{
		conf:      conf,
		log:       log,
		stats:     stats,
		closeChan: make(chan struct{}),
	}

	var err error
	if id := conf.MessageGroupID; len(id) > 0 {
		if s.groupID, err = interop.NewBloblangField(mgr, id); err != nil {
			return nil, fmt.Errorf("failed to parse group ID expression: %v", err)
		}
	}
	if id := conf.MessageDeduplicationID; len(id) > 0 {
		if s.dedupeID, err = interop.NewBloblangField(mgr, id); err != nil {
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

// ConnectWithContext attempts to establish a connection to the target SQS
// queue.
func (a *AmazonSQS) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect attempts to establish a connection to the target SQS queue.
func (a *AmazonSQS) Connect() error {
	if a.sqs != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
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

func (a *AmazonSQS) getSQSAttributes(msg types.Message, i int) sqsAttributes {
	p := msg.Get(i)
	keys := []string{}
	a.metaFilter.Iter(p.Metadata(), func(k, v string) error {
		if isValidSQSAttribute(k, v) {
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
				StringValue: aws.String(p.Metadata().Get(k)),
			}
			if i == 9 {
				break
			}
		}
	}

	var groupID, dedupeID *string
	if a.groupID != nil {
		groupID = aws.String(a.groupID.String(i, msg))
	}
	if a.dedupeID != nil {
		dedupeID = aws.String(a.dedupeID.String(i, msg))
	}

	return sqsAttributes{
		attrMap:  values,
		groupID:  groupID,
		dedupeID: dedupeID,
		content:  aws.String(string(p.Get())),
	}
}

// Write attempts to write message contents to a target SQS.
func (a *AmazonSQS) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target SQS.
func (a *AmazonSQS) WriteWithContext(ctx context.Context, msg types.Message) error {
	if a.sqs == nil {
		return types.ErrNotConnected
	}

	backOff := a.backoffCtor()

	entries := []*sqs.SendMessageBatchRequestEntry{}
	attrMap := map[string]sqsAttributes{}
	msg.Iter(func(i int, p types.Part) error {
		id := strconv.Itoa(i)
		attrs := a.getSQSAttributes(msg, i)
		attrMap[id] = attrs

		entries = append(entries, &sqs.SendMessageBatchRequestEntry{
			Id:                     aws.String(id),
			MessageBody:            attrs.content,
			MessageAttributes:      attrs.attrMap,
			MessageGroupId:         attrs.groupID,
			MessageDeduplicationId: attrs.dedupeID,
		})
		return nil
	})

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
				return types.ErrTimeout
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
				return types.ErrTimeout
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

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *AmazonSQS) CloseAsync() {
	a.closer.Do(func() {
		close(a.closeChan)
	})
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *AmazonSQS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
