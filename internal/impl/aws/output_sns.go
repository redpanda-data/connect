package aws

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/aws/config"
	"github.com/benthosdev/benthos/v4/internal/value"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// SNS Output Fields
	snsoFieldTopicARN        = "topic_arn"
	snsoFieldMessageGroupID  = "message_group_id"
	snsoFieldMessageDedupeID = "message_deduplication_id"
	snsoFieldMetadata        = "metadata"
	snsoFieldTimeout         = "timeout"
)

type snsoConfig struct {
	TopicArn               string
	MessageGroupID         *service.InterpolatedString
	MessageDeduplicationID *service.InterpolatedString
	Timeout                time.Duration
	Metadata               *service.MetadataExcludeFilter

	aconf aws.Config
}

func snsoConfigFromParsed(pConf *service.ParsedConfig) (conf snsoConfig, err error) {
	if conf.TopicArn, err = pConf.FieldString(snsoFieldTopicARN); err != nil {
		return
	}
	if pConf.Contains(snsoFieldMessageGroupID) {
		if conf.MessageGroupID, err = pConf.FieldInterpolatedString(snsoFieldMessageGroupID); err != nil {
			return
		}
	}
	if pConf.Contains(snsoFieldMessageDedupeID) {
		if conf.MessageDeduplicationID, err = pConf.FieldInterpolatedString(snsoFieldMessageDedupeID); err != nil {
			return
		}
	}
	if conf.Metadata, err = pConf.FieldMetadataExcludeFilter(snsoFieldMetadata); err != nil {
		return
	}
	if conf.Timeout, err = pConf.FieldDuration(snsoFieldTimeout); err != nil {
		return
	}
	if conf.aconf, err = GetSession(context.TODO(), pConf); err != nil {
		return
	}
	return
}

func snsoOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Version("3.36.0").
		Categories("Services", "AWS").
		Summary(`Sends messages to an AWS SNS topic.`).
		Description(output.Description(true, false, `
### Credentials

By default Benthos will use a shared credentials file when connecting to AWS services. It's also possible to set them explicitly at the component level, allowing you to transfer data across accounts. You can find out more [in this document](/docs/guides/cloud/aws).`)).
		Fields(
			service.NewStringField(snsoFieldTopicARN).
				Description("The topic to publish to."),
			service.NewInterpolatedStringField(snsoFieldMessageGroupID).
				Description("An optional group ID to set for messages.").
				Version("3.60.0").
				Optional(),
			service.NewInterpolatedStringField(snsoFieldMessageDedupeID).
				Description("An optional deduplication ID to set for messages.").
				Version("3.60.0").
				Optional(),
			service.NewOutputMaxInFlightField(),
			service.NewMetadataExcludeFilterField(snsoFieldMetadata).
				Description("Specify criteria for which metadata values are sent as headers.").
				Version("3.60.0"),
			service.NewDurationField(snsoFieldTimeout).
				Description("The maximum period to wait on an upload before abandoning it and reattempting.").
				Advanced().
				Default("5s"),
		).
		Fields(config.SessionFields()...)
}

func init() {
	err := service.RegisterOutput("aws_sns", snsoOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			if maxInFlight, err = conf.FieldMaxInFlight(); err != nil {
				return
			}
			var wConf snsoConfig
			if wConf, err = snsoConfigFromParsed(conf); err != nil {
				return
			}
			out, err = newSNSWriter(wConf, mgr)
			return
		})
	if err != nil {
		panic(err)
	}
}

type snsWriter struct {
	conf snsoConfig
	sns  *sns.Client
	log  *service.Logger
}

func newSNSWriter(conf snsoConfig, mgr *service.Resources) (*snsWriter, error) {
	s := &snsWriter{
		conf: conf,
		log:  mgr.Logger(),
	}
	return s, nil
}

func (a *snsWriter) Connect(ctx context.Context) error {
	if a.sns != nil {
		return nil
	}
	a.sns = sns.NewFromConfig(a.conf.aconf)
	return nil
}

type snsAttributes struct {
	attrMap  map[string]types.MessageAttributeValue
	groupID  *string
	dedupeID *string
}

var snsAttributeKeyInvalidCharRegexp = regexp.MustCompile(`(^\.)|(\.\.)|(^aws\.)|(^amazon\.)|(\.$)|([^a-z0-9_\-.]+)`)

func isValidSNSAttribute(k, v string) bool {
	return len(snsAttributeKeyInvalidCharRegexp.FindStringIndex(strings.ToLower(k))) == 0
}

func (a *snsWriter) getSNSAttributes(msg *service.Message) (snsAttributes, error) {
	keys := []string{}
	_ = a.conf.Metadata.WalkMut(msg, func(k string, v any) error {
		if isValidSNSAttribute(k, value.IToString(v)) {
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

		for _, k := range keys {
			vStr, _ := msg.MetaGet(k)
			values[k] = types.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(vStr),
			}
		}
	}

	var groupID, dedupeID *string
	if a.conf.MessageGroupID != nil {
		groupIDStr, err := a.conf.MessageGroupID.TryString(msg)
		if err != nil {
			return snsAttributes{}, fmt.Errorf("group id interpolation: %w", err)
		}
		groupID = aws.String(groupIDStr)
	}
	if a.conf.MessageDeduplicationID != nil {
		dedupeIDStr, err := a.conf.MessageDeduplicationID.TryString(msg)
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

func (a *snsWriter) Write(wctx context.Context, msg *service.Message) error {
	if a.sns == nil {
		return component.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(wctx, a.conf.Timeout)
	defer cancel()

	attrs, err := a.getSNSAttributes(msg)
	if err != nil {
		return err
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}
	message := &sns.PublishInput{
		TopicArn:               aws.String(a.conf.TopicArn),
		Message:                aws.String(string(mBytes)),
		MessageAttributes:      attrs.attrMap,
		MessageGroupId:         attrs.groupID,
		MessageDeduplicationId: attrs.dedupeID,
	}
	_, err = a.sns.Publish(ctx, message)
	return err
}

func (a *snsWriter) Close(context.Context) error {
	return nil
}
