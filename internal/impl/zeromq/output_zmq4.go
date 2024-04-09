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

func zmqOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Writes messages to a ZeroMQ socket.").
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
			Example([]string{"tcp://localhost:5556"})).
		Field(service.NewBoolField("bind").
			Description("Whether to bind to the specified URLs (otherwise they are connected to).").
			Default(true)).
		Field(service.NewStringEnumField("socket_type", "PUSH", "PUB").
			Description("The socket type to connect as.")).
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
	_ = service.RegisterBatchOutput("zmq4", zmqOutputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
		w, err := zmqOutputFromConfig(conf, mgr)
		if err != nil {
			return nil, service.BatchPolicy{}, 1, err
		}
		return w, service.BatchPolicy{}, 1, nil
	})
}

//------------------------------------------------------------------------------

// zmqOutput is an output type that writes zmqOutput messages.
type zmqOutput struct {
	log *service.Logger

	urls        []string
	socketType  string
	hwm         int
	bind        bool
	pollTimeout time.Duration

	poller *zmq4.Poller
	socket *zmq4.Socket
}

func zmqOutputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*zmqOutput, error) {
	z := zmqOutput{
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
	if _, err = getZMQOutputType(z.socketType); err != nil {
		return nil, err
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

func getZMQOutputType(t string) (zmq4.Type, error) {
	switch t {
	case "PUB":
		return zmq4.PUB, nil
	case "PUSH":
		return zmq4.PUSH, nil
	}
	return zmq4.PUSH, errors.New("invalid ZMQ socket type")
}

//------------------------------------------------------------------------------

func (z *zmqOutput) Connect(_ context.Context) (err error) {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQOutputType(z.socketType)
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

	_ = socket.SetSndhwm(z.hwm)

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

	z.socket = socket
	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLOUT)
	return nil
}

func (z *zmqOutput) WriteBatch(_ context.Context, batch service.MessageBatch) error {
	if z.socket == nil {
		return service.ErrNotConnected
	}

	var parts []any
	for _, m := range batch {
		b, err := m.AsBytes()
		if err != nil {
			return err
		}
		parts = append(parts, b)
	}

	_, err := z.socket.SendMessageDontwait(parts...)
	if err != nil {
		var polled []zmq4.Polled
		if polled, err = z.poller.Poll(z.pollTimeout); len(polled) == 1 {
			_, err = z.socket.SendMessage(parts...)
		} else if err == nil {
			return component.ErrTimeout
		}
	}
	return err
}

func (z *zmqOutput) Close(ctx context.Context) error {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
	return nil
}
