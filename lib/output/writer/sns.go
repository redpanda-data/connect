package writer

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/internal/metadata"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

//------------------------------------------------------------------------------

// SNSConfig contains configuration fields for the output SNS type.
type SNSConfig struct {
	TopicArn               string                       `json:"topic_arn" yaml:"topic_arn"`
	MessageGroupID         string                       `json:"message_group_id" yaml:"message_group_id"`
	MessageDeduplicationID string                       `json:"message_deduplication_id" yaml:"message_deduplication_id"`
	Metadata               metadata.ExcludeFilterConfig `json:"metadata" yaml:"metadata"`
	sessionConfig          `json:",inline" yaml:",inline"`
	Timeout                string `json:"timeout" yaml:"timeout"`
	MaxInFlight            int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewSNSConfig creates a new Config with default values.
func NewSNSConfig() SNSConfig {
	return SNSConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		TopicArn:               "",
		MessageGroupID:         "",
		MessageDeduplicationID: "",
		Metadata:               metadata.NewExcludeFilterConfig(),
		Timeout:                "5s",
		MaxInFlight:            1,
	}
}

//------------------------------------------------------------------------------

// SNS is a benthos writer.Type implementation that writes messages to an
// Amazon SNS queue.
type SNS struct {
	conf SNSConfig

	groupID    *field.Expression
	dedupeID   *field.Expression
	metaFilter *metadata.ExcludeFilter

	session *session.Session
	sns     *sns.SNS

	tout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewSNS creates a new Amazon SNS writer.Type.
func NewSNS(conf SNSConfig, log log.Modular, stats metrics.Type) (*SNS, error) {
	return NewSNSV2(conf, mock.NewManager(), log, stats)
}

// NewSNSV2 creates a new AWS SNS writer.
func NewSNSV2(conf SNSConfig, mgr types.Manager, log log.Modular, stats metrics.Type) (*SNS, error) {
	s := &SNS{
		conf:  conf,
		log:   log,
		stats: stats,
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
	if tout := conf.Timeout; len(tout) > 0 {
		if s.tout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target SNS queue.
func (a *SNS) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect attempts to establish a connection to the target SNS queue.
func (a *SNS) Connect() error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
	}

	a.session = sess
	a.sns = sns.New(sess)

	a.log.Infof("Sending messages to Amazon SNS ARN: %v\n", a.conf.TopicArn)
	return nil
}

type snsAttributes struct {
	attrMap  map[string]*sns.MessageAttributeValue
	groupID  *string
	dedupeID *string
}

var snsAttributeKeyInvalidCharRegexp = regexp.MustCompile(`(^\.)|(\.\.)|(^aws\.)|(^amazon\.)|(\.$)|([^a-z0-9_\-.]+)`)

func isValidSNSAttribute(k, v string) bool {
	return len(snsAttributeKeyInvalidCharRegexp.FindStringIndex(strings.ToLower(k))) == 0
}

func (a *SNS) getSNSAttributes(msg *message.Batch, i int) snsAttributes {
	p := msg.Get(i)
	keys := []string{}
	a.metaFilter.Iter(p, func(k, v string) error {
		if isValidSNSAttribute(k, v) {
			keys = append(keys, k)
		} else {
			a.log.Debugf("Rejecting metadata key '%v' due to invalid characters\n", k)
		}
		return nil
	})
	var values map[string]*sns.MessageAttributeValue
	if len(keys) > 0 {
		sort.Strings(keys)
		values = map[string]*sns.MessageAttributeValue{}

		for _, k := range keys {
			values[k] = &sns.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(p.MetaGet(k)),
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

	return snsAttributes{
		attrMap:  values,
		groupID:  groupID,
		dedupeID: dedupeID,
	}
}

// Write attempts to write message contents to a target SNS.
func (a *SNS) Write(msg *message.Batch) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target SNS.
func (a *SNS) WriteWithContext(wctx context.Context, msg *message.Batch) error {
	if a.session == nil {
		return component.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(wctx, a.tout)
	defer cancel()

	return IterateBatchedSend(msg, func(i int, p *message.Part) error {
		attrs := a.getSNSAttributes(msg, i)
		message := &sns.PublishInput{
			TopicArn:               aws.String(a.conf.TopicArn),
			Message:                aws.String(string(p.Get())),
			MessageAttributes:      attrs.attrMap,
			MessageGroupId:         attrs.groupID,
			MessageDeduplicationId: attrs.dedupeID,
		}
		_, err := a.sns.PublishWithContext(ctx, message)
		return err
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *SNS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *SNS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
