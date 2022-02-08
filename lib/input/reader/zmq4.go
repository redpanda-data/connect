//go:build ZMQ4
// +build ZMQ4

package reader

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/pebbe/zmq4"
)

//------------------------------------------------------------------------------

// ZMQ4 is an input type that consumes ZMQ messages.
type ZMQ4 struct {
	urls  []string
	conf  *ZMQ4Config
	stats metrics.Type
	log   log.Modular

	pollTimeout time.Duration
	poller      *zmq4.Poller
	socket      *zmq4.Socket
}

// NewZMQ4 creates a new ZMQ4 input type.
func NewZMQ4(conf *ZMQ4Config, log log.Modular, stats metrics.Type) (*ZMQ4, error) {
	z := ZMQ4{
		conf:  conf,
		stats: stats,
		log:   log,
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
	}

	_, err := getZMQType(conf.SocketType)
	if nil != err {
		return nil, err
	}

	if conf.SocketType == "SUB" && len(conf.SubFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		if z.pollTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}

	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQType(t string) (zmq4.Type, error) {
	switch t {
	case "SUB":
		return zmq4.SUB, nil
	case "PULL":
		return zmq4.PULL, nil
	}
	return zmq4.PULL, component.ErrInvalidZMQType
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a ZMQ4 socket.
func (z *ZMQ4) ConnectWithContext(ignored context.Context) error {
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

	socket.SetRcvhwm(z.conf.HighWaterMark)

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

	for _, filter := range z.conf.SubFilters {
		if err = socket.SetSubscribe(filter); err != nil {
			return err
		}
	}

	z.socket = socket
	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLIN)

	if z.conf.Bind {
		z.log.Infof("Receiving ZMQ4 messages on bound URLs: %s\n", z.urls)
	} else {
		z.log.Infof("Receiving ZMQ4 messages on connected URLs: %s\n", z.urls)
	}
	return nil
}

// ReadWithContext attempts to read a new message from the ZMQ socket.
func (z *ZMQ4) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	if z.socket == nil {
		return nil, nil, component.ErrNotConnected
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

	return message.New(data), noopAsyncAckFn, nil
}

// CloseAsync shuts down the ZMQ4 input and stops processing requests.
func (z *ZMQ4) CloseAsync() {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
}

// WaitForClose blocks until the ZMQ4 input has closed down.
func (z *ZMQ4) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
