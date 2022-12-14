package aws

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"

	"github.com/benthosdev/benthos/v4/internal/bloblang/field"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	sess "github.com/benthosdev/benthos/v4/internal/impl/aws/session"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/metadata"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(func(c output.Config, nm bundle.NewManagement) (output.Streamed, error) {
		return newSNSWriterFromConf(c.AWSSNS, nm)
	}), docs.ComponentSpec{
		Name:    "aws_sns",
		Version: "3.36.0",
		Summary: `
Sends messages to an AWS SNS topic.`,
		Description: output.Description(true, false, `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](/docs/guides/cloud/aws).`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("topic_arn", "The topic to publish to."),
			docs.FieldString("message_group_id", "An optional group ID to set for messages.").IsInterpolated().AtVersion("3.60.0"),
			docs.FieldString("message_deduplication_id", "An optional deduplication ID to set for messages.").IsInterpolated().AtVersion("3.60.0"),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			docs.FieldObject("metadata", "Specify criteria for which metadata values are sent as headers.").WithChildren(metadata.ExcludeFilterFields()...).AtVersion("3.60.0"),
			docs.FieldString("timeout", "The maximum period to wait on an upload before abandoning it and reattempting.").Advanced(),
		).WithChildren(sess.FieldSpecs()...).ChildDefaultAndTypesFromStruct(output.NewSNSConfig()),
		Categories: []string{
			"Services",
			"AWS",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newSNSWriterFromConf(conf output.SNSConfig, mgr bundle.NewManagement) (output.Streamed, error) {
	s, err := newSNSWriter(conf, mgr)
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("aws_sns", conf.MaxInFlight, s, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(a), nil
}

type snsWriter struct {
	conf output.SNSConfig

	groupID    *field.Expression
	dedupeID   *field.Expression
	metaFilter *metadata.ExcludeFilter

	session *session.Session
	sns     *sns.SNS

	tout time.Duration

	log log.Modular
}

func newSNSWriter(conf output.SNSConfig, mgr bundle.NewManagement) (*snsWriter, error) {
	s := &snsWriter{
		conf: conf,
		log:  mgr.Logger(),
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
	if tout := conf.Timeout; len(tout) > 0 {
		if s.tout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	return s, nil
}

func (a *snsWriter) Connect(ctx context.Context) error {
	if a.session != nil {
		return nil
	}

	sess, err := GetSessionFromConf(a.conf.SessionConfig.Config)
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

func (a *snsWriter) getSNSAttributes(msg message.Batch, i int) (snsAttributes, error) {
	p := msg.Get(i)
	keys := []string{}
	_ = a.metaFilter.Iter(p, func(k string, v any) error {
		if isValidSNSAttribute(k, query.IToString(v)) {
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
				StringValue: aws.String(p.MetaGetStr(k)),
			}
		}
	}

	var groupID, dedupeID *string
	if a.groupID != nil {
		groupIDStr, err := a.groupID.String(i, msg)
		if err != nil {
			return snsAttributes{}, fmt.Errorf("group id interpolation: %w", err)
		}
		groupID = aws.String(groupIDStr)
	}
	if a.dedupeID != nil {
		dedupeIDStr, err := a.dedupeID.String(i, msg)
		if err != nil {
			return snsAttributes{}, fmt.Errorf("dedupe id interpolation: %w", err)
		}
		dedupeID = aws.String(dedupeIDStr)
	}

	return snsAttributes{
		attrMap:  values,
		groupID:  groupID,
		dedupeID: dedupeID,
	}, nil
}

func (a *snsWriter) WriteBatch(wctx context.Context, msg message.Batch) error {
	if a.session == nil {
		return component.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(wctx, a.tout)
	defer cancel()

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		attrs, err := a.getSNSAttributes(msg, i)
		if err != nil {
			return err
		}
		message := &sns.PublishInput{
			TopicArn:               aws.String(a.conf.TopicArn),
			Message:                aws.String(string(p.AsBytes())),
			MessageAttributes:      attrs.attrMap,
			MessageGroupId:         attrs.groupID,
			MessageDeduplicationId: attrs.dedupeID,
		}
		_, err = a.sns.PublishWithContext(ctx, message)
		return err
	})
}

func (a *snsWriter) Close(context.Context) error {
	return nil
}
