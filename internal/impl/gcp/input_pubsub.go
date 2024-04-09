package gcp

import (
	"context"
	"errors"
	"strings"
	"sync"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Pubsub Input Fields
	pbiFieldProjectID              = "project"
	pbiFieldSubscriptionID         = "subscription"
	pbiFieldEndpoint               = "endpoint"
	pbiFieldMaxOutstandingMessages = "max_outstanding_messages"
	pbiFieldMaxOutstandingBytes    = "max_outstanding_bytes"
	pbiFieldSync                   = "sync"
	pbiFieldCreateSub              = "create_subscription"
	pbiFieldCreateSubEnabled       = "enabled"
	pbiFieldCreateSubTopicID       = "topic"
)

type pbiConfig struct {
	ProjectID              string
	SubscriptionID         string
	Endpoint               string
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	Sync                   bool
	CreateEnabled          bool
	CreateTopicID          string
}

func pbiConfigFromParsed(pConf *service.ParsedConfig) (conf pbiConfig, err error) {
	if conf.ProjectID, err = pConf.FieldString(pbiFieldProjectID); err != nil {
		return
	}
	if conf.SubscriptionID, err = pConf.FieldString(pbiFieldSubscriptionID); err != nil {
		return
	}
	if conf.Endpoint, err = pConf.FieldString(pbiFieldEndpoint); err != nil {
		return
	}
	if conf.MaxOutstandingMessages, err = pConf.FieldInt(pbiFieldMaxOutstandingMessages); err != nil {
		return
	}
	if conf.MaxOutstandingBytes, err = pConf.FieldInt(pbiFieldMaxOutstandingBytes); err != nil {
		return
	}
	if conf.Sync, err = pConf.FieldBool(pbiFieldSync); err != nil {
		return
	}
	if pConf.Contains(pbiFieldCreateSub) {
		createConf := pConf.Namespace(pbiFieldCreateSub)
		if conf.CreateEnabled, err = createConf.FieldBool(pbiFieldCreateSubEnabled); err != nil {
			return
		}
		if conf.CreateTopicID, err = createConf.FieldString(pbiFieldCreateSubTopicID); err != nil {
			return
		}
	}
	return
}

func pbiSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "GCP").
		Summary(`Consumes messages from a GCP Cloud Pub/Sub subscription.`).
		Description(`
For information on how to set up credentials check out [this guide](https://cloud.google.com/docs/authentication/production).

### Metadata

This input adds the following metadata fields to each message:

`+"``` text"+`
- gcp_pubsub_publish_time_unix - The time at which the message was published to the topic.
- gcp_pubsub_delivery_attempt - When dead lettering is enabled, this is set to the number of times PubSub has attempted to deliver a message.
- All message attributes
`+"```"+`

You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).
`).
		Fields(
			service.NewStringField(pbiFieldProjectID).
				Description("The project ID of the target subscription."),
			service.NewStringField(pbiFieldSubscriptionID).
				Description("The target subscription ID."),
			service.NewStringField(pbiFieldEndpoint).
				Description("An optional endpoint to override the default of `pubsub.googleapis.com:443`. This can be used to connect to a region specific pubsub endpoint. For a list of valid values check out [this document.](https://cloud.google.com/pubsub/docs/reference/service_apis_overview#list_of_regional_endpoints)").
				Example("us-central1-pubsub.googleapis.com:443").
				Example("us-west3-pubsub.googleapis.com:443").
				Default(""),
			service.NewBoolField(pbiFieldSync).
				Description("Enable synchronous pull mode.").
				Default(false),
			service.NewIntField(pbiFieldMaxOutstandingMessages).
				Description("The maximum number of outstanding pending messages to be consumed at a given time.").
				Default(1000), // pubsub.DefaultReceiveSettings.MaxOutstandingMessages)
			service.NewIntField(pbiFieldMaxOutstandingBytes).
				Description("The maximum number of outstanding pending messages to be consumed measured in bytes.").
				Default(1e9), // pubsub.DefaultReceiveSettings.MaxOutstandingBytes (1G)
			service.NewObjectField(pbiFieldCreateSub,
				service.NewBoolField(pbiFieldCreateSubEnabled).
					Description("Whether to configure subscription or not.").Default(false),
				service.NewStringField(pbiFieldCreateSubTopicID).
					Description("Defines the topic that the subscription should be vinculated to.").
					Default(""),
			).
				Description("Allows you to configure the input subscription and creates if it doesn't exist.").
				Advanced(),
		)
}

func init() {
	err := service.RegisterInput("gcp_pubsub", pbiSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			pConf, err := pbiConfigFromParsed(conf)
			if err != nil {
				return nil, err
			}
			return newGCPPubSubReader(pConf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func createSubscription(conf pbiConfig, client *pubsub.Client, log *service.Logger) {
	subsExists, err := client.Subscription(conf.SubscriptionID).Exists(context.Background())
	if err != nil {
		log.Errorf("Error checking if subscription exists", err)
		return
	}

	if subsExists {
		log.Infof("Subscription '%v' already exists", conf.SubscriptionID)
		return
	}

	if conf.CreateTopicID == "" {
		log.Infof("Subscription won't be created because TopicID is not defined")
		return
	}

	log.Infof("Creating subscription '%v' on topic '%v'\n", conf.SubscriptionID, conf.CreateTopicID)
	_, err = client.CreateSubscription(context.Background(), conf.SubscriptionID, pubsub.SubscriptionConfig{Topic: client.Topic(conf.CreateTopicID)})
	if err != nil {
		log.Errorf("Error creating subscription %v", err)
	}
}

type gcpPubSubReader struct {
	conf pbiConfig

	subscription *pubsub.Subscription
	msgsChan     chan *pubsub.Message
	closeFunc    context.CancelFunc
	subMut       sync.Mutex

	client *pubsub.Client

	log *service.Logger
}

func newGCPPubSubReader(conf pbiConfig, res *service.Resources) (*gcpPubSubReader, error) {
	var opt []option.ClientOption
	if strings.TrimSpace(conf.Endpoint) != "" {
		opt = []option.ClientOption{option.WithEndpoint(conf.Endpoint)}
	}

	client, err := pubsub.NewClient(context.Background(), conf.ProjectID, opt...)
	if err != nil {
		return nil, err
	}

	if conf.CreateEnabled {
		if conf.CreateTopicID == "" {
			return nil, errors.New("must specify a topic_id when create_subscription is enabled")
		}
		createSubscription(conf, client, res.Logger())
	}

	return &gcpPubSubReader{
		conf:   conf,
		log:    res.Logger(),
		client: client,
	}, nil
}

func (c *gcpPubSubReader) Connect(ignored context.Context) error {
	c.subMut.Lock()
	defer c.subMut.Unlock()
	if c.subscription != nil {
		return nil
	}

	sub := c.client.Subscription(c.conf.SubscriptionID)
	sub.ReceiveSettings.MaxOutstandingMessages = c.conf.MaxOutstandingMessages
	sub.ReceiveSettings.MaxOutstandingBytes = c.conf.MaxOutstandingBytes
	sub.ReceiveSettings.Synchronous = c.conf.Sync

	subCtx, cancel := context.WithCancel(context.Background())
	msgsChan := make(chan *pubsub.Message, 1)

	c.subscription = sub
	c.msgsChan = msgsChan
	c.closeFunc = cancel

	go func() {
		rerr := sub.Receive(subCtx, func(ctx context.Context, m *pubsub.Message) {
			select {
			case msgsChan <- m:
			case <-ctx.Done():
				if m != nil {
					m.Nack()
				}
			}
		})
		if rerr != nil && rerr != context.Canceled {
			c.log.Errorf("Subscription error: %v\n", rerr)
		}
		c.subMut.Lock()
		c.subscription = nil
		close(c.msgsChan)
		c.msgsChan = nil
		c.closeFunc = nil
		c.subMut.Unlock()
	}()
	return nil
}

func (c *gcpPubSubReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	c.subMut.Lock()
	msgsChan := c.msgsChan
	c.subMut.Unlock()
	if msgsChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	var gmsg *pubsub.Message
	var open bool
	select {
	case gmsg, open = <-msgsChan:
	case <-ctx.Done():
		return nil, nil, component.ErrTimeout
	}
	if !open {
		return nil, nil, service.ErrNotConnected
	}

	part := service.NewMessage(gmsg.Data)
	for k, v := range gmsg.Attributes {
		part.MetaSetMut(k, v)
	}
	part.MetaSetMut("gcp_pubsub_publish_time_unix", gmsg.PublishTime.Unix())

	if gmsg.DeliveryAttempt != nil {
		part.MetaSetMut("gcp_pubsub_delivery_attempt", *gmsg.DeliveryAttempt)
	}

	return part, func(ctx context.Context, res error) error {
		if res != nil {
			gmsg.Nack()
		} else {
			gmsg.Ack()
		}
		return nil
	}, nil
}

func (c *gcpPubSubReader) Close(ctx context.Context) error {
	c.subMut.Lock()
	defer c.subMut.Unlock()

	if c.closeFunc != nil {
		c.closeFunc()
		c.closeFunc = nil
	}
	return nil
}
