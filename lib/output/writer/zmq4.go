//go:build ZMQ4
// +build ZMQ4

package writer

import (
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/pebbe/zmq4"
)

//------------------------------------------------------------------------------

// ZMQ4 is an output type that writes ZMQ4 messages.
type ZMQ4 struct {
	log   log.Modular
	stats metrics.Type

	urls []string
	conf *ZMQ4Config

	pollTimeout time.Duration
	poller      *zmq4.Poller
	socket      *zmq4.Socket
}

// NewZMQ4 creates a new ZMQ4 output type.
func NewZMQ4(conf *ZMQ4Config, log log.Modular, stats metrics.Type) (*ZMQ4, error) {
	z := ZMQ4{
		log:   log,
		stats: stats,
		conf:  conf,
	}

	_, err := getZMQType(conf.SocketType)
	if nil != err {
		return nil, err
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		var err error
		if z.pollTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
	}

	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQType(t string) (zmq4.Type, error) {
	switch t {
	case "PUB":
		return zmq4.PUB, nil
	case "PUSH":
		return zmq4.PUSH, nil
	}
	return zmq4.PULL, types.ErrInvalidZMQType
}

//------------------------------------------------------------------------------

// Connect attempts to establish a connection to a ZMQ4 socket.
func (z *ZMQ4) Connect() error {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQType(z.conf.SocketType)
	if nil != err {
		return err
	}

	ctx, err := zmq4.NewContext()
	if nil != err {
		return err
	}

	var socket *zmq4.Socket
	if socket, err = ctx.NewSocket(t); nil != err {
		return err
	}

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	socket.SetSndhwm(z.conf.HighWaterMark)

	for _, address := range z.urls {
		if z.conf.Bind {
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

	z.log.Infof("Sending ZMQ4 messages to URLs: %s\n", z.urls)
	return nil
}

// Write will attempt to write a message to the ZMQ4 socket.
func (z *ZMQ4) Write(msg *message.Batch) error {
	if z.socket == nil {
		return types.ErrNotConnected
	}
	_, err := z.socket.SendMessageDontwait(message.GetAllBytes(msg))
	if err != nil {
		var polled []zmq4.Polled
		if polled, err = z.poller.Poll(z.pollTimeout); len(polled) == 1 {
			_, err = z.socket.SendMessage(message.GetAllBytes(msg))
		} else if err == nil {
			return types.ErrTimeout
		}
	}
	return err
}

// CloseAsync shuts down the ZMQ4 output and stops processing messages.
func (z *ZMQ4) CloseAsync() {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
}

// WaitForClose blocks until the ZMQ4 output has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
