package pusher

import (
	"context"

	"github.com/pusher/pusher-http-go"

	"github.com/benthosdev/benthos/v4/public/service"
)

func pusherOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.3.0").
		Summary("Output for publishing messages to Pusher API (https://pusher.com)").
		Field(service.NewBatchPolicyField("batching").
			Description("maximum batch size is 10 (limit of the pusher library)")).
		Field(service.NewInterpolatedStringField("channel").
			Description("Pusher channel to publish to. Interpolation functions can also be used").
			Example("my_channel").
			Example("${!json(\"id\")}")).
		Field(service.NewStringField("event").
			Description("Event to publish to")).
		Field(service.NewStringField("appId").
			Description("Pusher app id")).
		Field(service.NewStringField("key").
			Description("Pusher key")).
		Field(service.NewStringField("secret").
			Description("Pusher secret")).
		Field(service.NewStringField("cluster").
			Description("Pusher cluster")).
		Field(service.NewBoolField("secure").
			Description("Enable SSL encryption").
			Default(true)).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of parallel message batches to have in flight at any given time.").
			Default(1))
}

func init() {
	err := service.RegisterBatchOutput("pusher", pusherOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (
			output service.BatchOutput,
			batchPolicy service.BatchPolicy,
			maxInFlight int,
			err error,
		) {
			if maxInFlight, err = conf.FieldInt("max_in_flight"); err != nil {
				return
			}
			if batchPolicy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			output, err = newPusherWriterFromConfig(conf, mgr.Logger())
			return
		})
	if err != nil {
		panic(err)
	}
}

type pusherWriter struct {
	log *service.Logger

	event   string
	appID   string
	key     string
	secret  string
	cluster string
	secure  bool
	channel *service.InterpolatedString

	client pusher.Client
}

func newPusherWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*pusherWriter, error) {
	p := pusherWriter{
		log: log,
	}

	var err error

	// check and write all variables to config

	if p.channel, err = conf.FieldInterpolatedString("channel"); err != nil {
		return nil, err
	}

	if p.event, err = conf.FieldString("event"); err != nil {
		return nil, err
	}
	if p.appID, err = conf.FieldString("appId"); err != nil {
		return nil, err
	}
	if p.key, err = conf.FieldString("key"); err != nil {
		return nil, err
	}
	if p.secret, err = conf.FieldString("secret"); err != nil {
		return nil, err
	}
	if p.cluster, err = conf.FieldString("cluster"); err != nil {
		return nil, err
	}
	if p.secure, err = conf.FieldBool("secure"); err != nil {
		return nil, err
	}

	return &p, nil
}

func (p *pusherWriter) Connect(ctx context.Context) error {
	// create pusher client
	p.client = pusher.Client{
		AppID:   p.appID,
		Key:     p.key,
		Secret:  p.secret,
		Cluster: p.cluster,
		Secure:  p.secure,
	}
	p.log.Infof("Pusher client connected")
	return nil
}

func (p *pusherWriter) WriteBatch(ctx context.Context, b service.MessageBatch) (err error) {
	events := make([]pusher.Event, 0, len(b))

	// iterate over batch and set pusher events in array
	for _, msg := range b {
		content, err := msg.AsBytes()
		if err != nil {
			return err
		}
		key := p.channel.String(msg)

		event := pusher.Event{
			Channel: key,
			Name:    p.event,
			Data:    content,
		}
		events = append(events, event)
	}
	// send event array to pusher
	err = p.client.TriggerBatch(events)
	return err
}

func (p *pusherWriter) Close(ctx context.Context) error {
	p.client.HTTPClient.CloseIdleConnections()
	p.log.Infof("Pusher connection closed")
	return nil
}
