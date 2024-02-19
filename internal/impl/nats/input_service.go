package nats

import (
	"context"
	"crypto/tls"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/component/input/span"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/internal/transaction"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

var serviceConfigSpec = service.NewConfigSpec().
	Categories("Services").
	Version("4.24.0").
	Summary("Register a Microservice with NATS and replies to requests on grouped endpoints.").
	Description(`
### Responses

It's possible to return a response for each message received using [synchronous responses](/docs/guides/sync_responses).
A response contains the message content as the NATS message payload and all message metadata as NATS headers.

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- nats_service_groupname
- nats_service_endpoint
- nats_service_queue_group
- nats_service_subject
` + "```" + `

You can access these metadata fields using
[function interpolation](/docs/configuration/interpolation#bloblang-queries).

` + ConnectionNameDescription() + auth.Description()).
	Field(service.NewStringListField("urls").
		Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
		Example([]string{"nats://127.0.0.1:4222"}).
		Example([]string{"nats://username:password@127.0.0.1:4222"})).
	Field(service.NewStringField("name").
		Description("NATS Microservice name, no whitespaces allowed"). // TODO Lint for whitespaces
		Examples("min_max")).
	Field(service.NewStringField("version").
		Description("NATS Microservice semver based version.").
		Example("1.0.0").
		Default("0.0.1")). // TODO Lint for semver
	Field(service.NewStringField("description").
		Description("NATS Microservice description.").
		Example("Benthos Microservice").
		Default("Benthos Microservice")).
	Field(service.NewStringField("queue_group").Optional().Advanced().Description("Default queueGroup of all endpoints in this service").Example("q1").Default("q")).
	Field(service.NewObjectListField(
		"groups",
		service.NewStringField("name").Description("NATS Microservice group name. It should be a valid NATS subject or an empty string, but cannot contain > wildcard (as group name serves as subject prefix)."),
		service.NewStringField("queue_group").Optional().Advanced().Description("Default queueGroup for endpoints that overrides service queueGroup."),
		service.NewObjectListField("endpoints",
			service.NewStringField("name").Description("Endpoint name, no whitespaces allowed"),
			service.NewStringField("subject").Optional().Advanced().Description("An optional NATS subject on which the endpoint will be registered. A subject is created by concatenating the subject provided by the user with group prefix (if applicable). If subject is not provided, name is used instead."),
			service.NewStringField("queue_group").Optional().Advanced().Description("Overrides service and group queueGroup."),
			service.NewStringMapField("metadata").Optional().Description("Additional information about the endpoint.")).
			Description("list of NATS Microservice endpoints"),
	).
		Description("A group serves as a common prefix to all endpoints registered in it.").
		Example([]any{map[string]any{
			"name": "math",
			"endpoints": []any{
				map[string]any{
					"name": "min",
				},
				map[string]any{
					"name": "max",
				},
			},
		}})).
	Field(service.NewTLSToggledField("tls")).
	Field(service.NewInternalField(auth.FieldSpec())).
	Field(span.ExtractTracingSpanMappingDocs().Version(tracingVersion))

func init() {
	err := service.RegisterInput("nats_service", serviceConfigSpec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			srv, err := newServiceInput(conf, mgr)
			if err != nil {
				return nil, err
			}
			return span.NewInput("nats_service", conf, srv, mgr)
		})
	if err != nil {
		panic(err)
	}
}

func newServiceInput(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
	srv := &natsService{
		label: mgr.Label(),
		log:   mgr.Logger(),
		fs:    mgr.FS(),
	}

	urlList, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}
	srv.urls = strings.Join(urlList, ",")

	if srv.name, err = conf.FieldString("name"); err != nil {
		return nil, err
	}

	if srv.version, err = conf.FieldString("version"); err != nil {
		return nil, err
	}

	if srv.description, err = conf.FieldString("description"); err != nil {
		return nil, err
	}

	if srv.queueGroup, err = conf.FieldString("queue_group"); err != nil {
		return nil, err
	}

	if conf.Contains("groups") {
		groupsConf, err := conf.FieldObjectList("groups")
		if err != nil {
			return nil, err
		}
		for _, groupParsedConf := range groupsConf {
			var groupConf *groupConfig
			if groupConf, err = newGroupConfig(groupParsedConf); err != nil {
				return nil, err
			}
			srv.groupsConfig = append(srv.groupsConfig, groupConf)
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled("tls")
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		srv.tlsConf = tlsConf
	}

	if srv.authConf, err = AuthFromParsedConfig(conf.Namespace("auth")); err != nil {
		return nil, err
	}

	return srv, nil
}

type endpointConfig struct {
	name       string
	subject    string
	queueGroup string
	metadata   map[string]string
}

func newEndpointConfig(conf *service.ParsedConfig) (endpoint *endpointConfig, err error) {
	endpoint = &endpointConfig{}
	if endpoint.name, err = conf.FieldString("name"); err != nil {
		return
	}
	if conf.Contains("subject") {
		if endpoint.subject, err = conf.FieldString("subject"); err != nil {
			return
		}
	}
	if conf.Contains("queue_group") {
		if endpoint.queueGroup, err = conf.FieldString("queue_group"); err != nil {
			return
		}
	}
	if conf.Contains("metadata") {
		if endpoint.metadata, err = conf.FieldStringMap("metadata"); err != nil {
			return
		}
	}
	return
}

type groupConfig struct {
	name       string
	queueGroup string
	endpoints  []*endpointConfig
}

func newGroupConfig(conf *service.ParsedConfig) (group *groupConfig, err error) {
	group = &groupConfig{}
	if group.name, err = conf.FieldString("name"); err != nil {
		return
	}
	if conf.Contains("queue_group") {
		if group.queueGroup, err = conf.FieldString("queue_group"); err != nil {
			return
		}
	}

	endpoints, err := conf.FieldObjectList("endpoints")
	if err != nil {
		return
	}
	for _, endpointConf := range endpoints {
		var endpoint *endpointConfig
		endpoint, err = newEndpointConfig(endpointConf)
		if err != nil {
			return
		}
		group.endpoints = append(group.endpoints, endpoint)
	}

	return
}

type natsService struct {
	urls         string
	name         string
	version      string
	description  string
	queueGroup   string
	groupsConfig []*groupConfig
	authConf     auth.Config
	tlsConf      *tls.Config

	label string
	log   *service.Logger
	fs    *service.FS

	natsConn *nats.Conn
	service  micro.Service
	input    chan *request
}

// Close implements service.Input.
// Drains the endpoint subscriptions and marks the service as stopped.
// Closes the connection to the NATS server.
// Closes internal communication and cleansup for reuse by calling Connect.
func (n *natsService) Close(ctx context.Context) error {
	sig := shutdown.NewSignaller()
	go func() {
		if n.service != nil {
			if err := n.service.Stop(); err != nil {
				n.log.Error(err.Error())
			}
		}
		if n.natsConn != nil {
			n.natsConn.Close()
		}
		n.service = nil
		n.natsConn = nil
		if n.input != nil {
			close(n.input)
			n.input = nil
		}
		sig.ShutdownComplete()
	}()
	select {
	case <-sig.HasClosedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// Connect implements service.Input.
func (n *natsService) Connect(ctx context.Context) error {
	var (
		opts []nats.Option
		err  error
	)
	if n.tlsConf != nil {
		opts = append(opts, nats.Secure(n.tlsConf))
	}
	opts = append(opts, nats.Name(n.label))
	opts = append(opts, authConfToOptions(n.authConf, n.fs)...)
	opts = append(opts, errorHandlerOption(n.log))
	if n.natsConn, err = nats.Connect(n.urls, opts...); err != nil {
		return err
	}
	srvConf := micro.Config{
		Name:        n.name,
		Version:     n.version,
		Description: n.description,
		QueueGroup:  n.queueGroup,
	}
	n.service, err = micro.AddService(n.natsConn, srvConf)
	n.input = make(chan *request)
	if err != nil {
		// Cleanup before returning error
		n.Close(ctx)
		return err
	}
	if n.groupsConfig != nil {
		for _, gConf := range n.groupsConfig {
			var gOps []micro.GroupOpt
			if len(gConf.queueGroup) > 0 {
				gOps = append(gOps, micro.WithGroupQueueGroup(gConf.queueGroup))
			}
			group := n.service.AddGroup(gConf.name, gOps...)
			for _, eConf := range gConf.endpoints {
				var eOps []micro.EndpointOpt
				if eConf.metadata != nil {
					eOps = append(eOps, micro.WithEndpointMetadata(eConf.metadata))
				}
				if len(eConf.queueGroup) > 0 {
					eOps = append(eOps, micro.WithEndpointQueueGroup(eConf.queueGroup))
				}
				if len(eConf.subject) > 0 {
					eOps = append(eOps, micro.WithEndpointSubject(eConf.subject))
				}
				var (
					groupName    = gConf.name
					endpointName = eConf.name
					queueGroup   = eConf.queueGroup
				)
				if len(queueGroup) == 0 {
					queueGroup = gConf.queueGroup
				}
				if len(queueGroup) == 0 {
					queueGroup = n.queueGroup
				}
				n.log.Infof("Register NATS Microservice group %s endpoint %s added under subject %s.%s with queue group %s", groupName, endpointName, groupName, eConf.subject, queueGroup)
				group.AddEndpoint(eConf.name, micro.HandlerFunc(func(req micro.Request) {
					n.handle(&request{req, groupName, endpointName, queueGroup})
				}), eOps...)
			}
		}
	}
	return nil
}

func convertRequest(req *request) (*service.Message, service.AckFunc, error) {
	msg := service.NewMessage(req.req.Data())

	headers := nats.Header(req.req.Headers())
	for k := range headers {
		v := headers.Get(k)
		if v != "" {
			msg.MetaSet(k, v)
		}
	}
	msg.MetaSet("nats_service_groupname", req.groupName)
	msg.MetaSet("nats_service_endpoint", req.endpointName)
	msg.MetaSet("nats_service_queue_group", req.queueGroup)
	msg.MetaSet("nats_service_subject", req.req.Subject())

	store := transaction.NewResultStore()
	ctx := msg.Context()
	ctx = context.WithValue(ctx, transaction.ResultStoreKey, store)
	msg = msg.WithContext(ctx)

	return msg, func(ctx context.Context, err error) error {
		if err != nil {
			// TODO Allow customizing code, description, and body
			// err=batch.Error
			return req.req.Error("500", err.Error(), nil)
		}
		msgBatches := store.Get()
		for _, msgs := range msgBatches {

			for _, msg := range msgs {
				// TODO Allow customizing payload and headers
				// See: https://www.benthos.dev/docs/components/inputs/http_server#sync_response
				headers := nats.Header{}
				if err := msg.MetaIterStr(func(key string, value string) error {
					headers.Add(key, value)
					return nil
				}); err != nil {
					return err
				}
				if err := req.req.Respond(msg.AsBytes(), micro.WithHeaders(micro.Headers(headers))); err != nil {
					return err
				}
			}
		}
		return nil
	}, nil
}

type request struct {
	req          micro.Request
	groupName    string
	endpointName string
	queueGroup   string
	// TODO Add service stats to provide the as Benthos message metadata
}

// handle receives NATS Microservice requests and pipes them to an internal unbuffered channel,
// which gets read from the Read method.
func (n *natsService) handle(req *request) {
	n.input <- req
}

// Read implements service.Input.
// Read blocks until either a message has been received, the connection is lost, or
// the provided context is cancelled.
// Read a single message from an internal request channel, along with a function to be called
// once the message can be either acked (successfully sent or intentionally
// filtered) or nacked (failed to be processed or dispatched to the output).
// ErrNotConnected is returned when NATS disconnects or the connection is closed.
func (n *natsService) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case req := <-n.input:
		return convertRequest(req)
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case status := <-n.natsConn.StatusChanged(nats.DISCONNECTED, nats.CLOSED):
		n.log.Warnf("NATS Connection status changed to %s", status)
		_ = n.Close(ctx)
		return nil, nil, service.ErrNotConnected
	}
}
