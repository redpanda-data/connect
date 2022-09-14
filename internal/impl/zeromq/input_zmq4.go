//go:build x_benthos_extra
// +build x_benthos_extra

package zeromq

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/pebbe/zmq4"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

func zmqInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Consumes messages from a ZeroMQ socket.").
		Description(`
By default Benthos does not build with components that require linking to external libraries. If you wish to build Benthos locally with this component then set the build tag ` + "`x_benthos_extra`" + `:

` + "```shell" + `
# With go
go install -tags "x_benthos_extra" github.com/benthosdev/benthos/v4/cmd/benthos@latest

# Using make
make TAGS=x_benthos_extra
` + "```" + `

There is a specific docker tag postfix ` + "`-cgo`" + ` for C builds containing this component.`).
		Field(service.NewStringListField("urls").
			Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
			Example([]string{"tcp://localhost:5555"})).
		Field(service.NewBoolField("bind").
			Description("Whether to bind to the specified URLs (otherwise they are connected to).").
			Default(false)).
		Field(service.NewStringEnumField("socket_type", "PULL", "SUB").
			Description("The socket type to connect as.")).
		Field(service.NewStringListField("sub_filters").
			Description("A list of subscription topic filters to use when consuming from a SUB socket. Specifying a single sub_filter of `''` will subscribe to everything.").
			Default([]any{})).
		Field(service.NewIntField("high_water_mark").
			Description("The message high water mark to use.").
			Default(0).
			Advanced()).
		Field(service.NewDurationField("poll_timeout").
			Description("The poll timeout to use.").
			Default("5s").
			Advanced())
}

func init() {
	_ = service.RegisterBatchInput("zmq4", zmqInputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := zmqInputFromConfig(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatched(r), nil
	})
}

//------------------------------------------------------------------------------

type zmqInput struct {
	log *service.Logger

	urls        []string
	socketType  string
	hwm         int
	bind        bool
	subFilters  []string
	pollTimeout time.Duration

	poller *zmq4.Poller
	socket *zmq4.Socket
}

func zmqInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*zmqInput, error) {
	z := zmqInput{
		log: mgr.Logger(),
	}

	urlStrs, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}

	for _, u := range urlStrs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
	}

	if z.bind, err = conf.FieldBool("bind"); err != nil {
		return nil, err
	}
	if z.socketType, err = conf.FieldString("socket_type"); err != nil {
		return nil, err
	}
	if _, err := getZMQInputType(z.socketType); err != nil {
		return nil, err
	}

	if z.subFilters, err = conf.FieldStringList("sub_filters"); err != nil {
		return nil, err
	}

	if z.socketType == "SUB" && len(z.subFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
	}

	if z.hwm, err = conf.FieldInt("high_water_mark"); err != nil {
		return nil, err
	}

	if z.pollTimeout, err = conf.FieldDuration("poll_timeout"); err != nil {
		return nil, err
	}
	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQInputType(t string) (zmq4.Type, error) {
	switch t {
	case "SUB":
		return zmq4.SUB, nil
	case "PULL":
		return zmq4.PULL, nil
	}
	return zmq4.PULL, errors.New("invalid ZMQ socket type")
}

func (z *zmqInput) Connect(ignored context.Context) error {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQInputType(z.socketType)
	if err != nil {
		return err
	}

	ctx, err := zmq4.NewContext()
	if err != nil {
		return err
	}

	var socket *zmq4.Socket
	if socket, err = ctx.NewSocket(t); err != nil {
		return err
	}

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	_ = socket.SetRcvhwm(z.hwm)

	for _, address := range z.urls {
		if z.bind {
			err = socket.Bind(address)
		} else {
			err = socket.Connect(address)
		}
		if err != nil {
			return err
		}
	}

	for _, filter := range z.subFilters {
		if err := socket.SetSubscribe(filter); err != nil {
			return err
		}
	}

	z.socket = socket
	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLIN)

	if z.bind {
		z.log.Infof("Receiving zmqInput messages on bound URLs: %s\n", z.urls)
	} else {
		z.log.Infof("Receiving zmqInput messages on connected URLs: %s\n", z.urls)
	}
	return nil
}

func (z *zmqInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if z.socket == nil {
		return nil, nil, service.ErrNotConnected
	}

	data, err := z.socket.RecvMessageBytes(zmq4.DONTWAIT)
	if err != nil {
		var polled []zmq4.Polled
		if polled, err = z.poller.Poll(z.pollTimeout); len(polled) == 1 {
			data, err = z.socket.RecvMessageBytes(0)
		} else if err == nil {
			return nil, nil, component.ErrTimeout
		}
	}
	if err != nil {
		return nil, nil, err
	}

	var batch service.MessageBatch
	for _, d := range data {
		batch = append(batch, service.NewMessage(d))
	}

	return batch, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

// CloseAsync shuts down the zmqInput input and stops processing requests.
func (z *zmqInput) Close(ctx context.Context) error {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
	return nil
}
