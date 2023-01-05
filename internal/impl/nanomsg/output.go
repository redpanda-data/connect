package nanomsg

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/push"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"

	// Import all transport types.
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newNanomsgOutput), docs.ComponentSpec{
		Name:        "nanomsg",
		Summary:     `Send messages over a Nanomsg socket.`,
		Description: output.Description(true, false, `Currently only PUSH and PUB sockets are supported.`),
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("urls", "A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.", []string{"tcp://localhost:5556"}).Array(),
			docs.FieldBool("bind", "Whether the URLs listed should be bind (otherwise they are connected to)."),
			docs.FieldString("socket_type", "The socket type to send with.").HasOptions("PUSH", "PUB"),
			docs.FieldString("poll_timeout", "The maximum period of time to wait for a message to send before the request is abandoned and reattempted."),
			docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).ChildDefaultAndTypesFromStruct(output.NewNanomsgConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNanomsgOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	s, err := newNanomsgWriter(conf.Nanomsg, mgr.Logger())
	if err != nil {
		return nil, err
	}
	a, err := output.NewAsyncWriter("nanomsg", conf.Nanomsg.MaxInFlight, s, mgr)
	if err != nil {
		return nil, err
	}
	return output.OnlySinglePayloads(a), nil
}

type nanomsgWriter struct {
	log log.Modular

	urls []string
	conf output.NanomsgConfig

	timeout time.Duration

	socket  mangos.Socket
	sockMut sync.RWMutex
}

func newNanomsgWriter(conf output.NanomsgConfig, log log.Modular) (*nanomsgWriter, error) {
	s := nanomsgWriter{
		log:  log,
		conf: conf,
	}
	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				s.urls = append(s.urls, strings.Replace(splitU, "//*:", "//0.0.0.0:", 1))
			}
		}
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		var err error
		if s.timeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}

	socket, err := getOutputSocketFromType(conf.SocketType)
	if err != nil {
		return nil, err
	}
	socket.Close()
	return &s, nil
}

func getOutputSocketFromType(t string) (mangos.Socket, error) {
	switch t {
	case "PUSH":
		return push.NewSocket()
	case "PUB":
		return pub.NewSocket()
	}
	return nil, errors.New("invalid Scalability Protocols socket type")
}

func (s *nanomsgWriter) Connect(ctx context.Context) error {
	s.sockMut.Lock()
	defer s.sockMut.Unlock()

	if s.socket != nil {
		return nil
	}

	socket, err := getSocketFromType(s.conf.SocketType)
	if err != nil {
		return err
	}

	// Set timeout to prevent endless lock.
	if s.conf.SocketType == "PUSH" {
		if err := socket.SetOption(
			mangos.OptionSendDeadline, s.timeout,
		); err != nil {
			return err
		}
	}

	if s.conf.Bind {
		for _, addr := range s.urls {
			if err = socket.Listen(addr); err != nil {
				break
			}
		}
	} else {
		for _, addr := range s.urls {
			if err = socket.Dial(addr); err != nil {
				break
			}
		}
	}
	if err != nil {
		return err
	}

	if s.conf.Bind {
		s.log.Infof(
			"Sending nanomsg messages to bound URLs: %s\n",
			s.urls,
		)
	} else {
		s.log.Infof(
			"Sending nanomsg messages to connected URLs: %s\n",
			s.urls,
		)
	}
	s.socket = socket
	return nil
}

func (s *nanomsgWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	s.sockMut.RLock()
	socket := s.socket
	s.sockMut.RUnlock()

	if socket == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		return socket.Send(p.AsBytes())
	})
}

func (s *nanomsgWriter) Close(context.Context) (err error) {
	s.sockMut.Lock()
	defer s.sockMut.Unlock()

	if s.socket != nil {
		err = s.socket.Close()
		s.socket = nil
	}
	return
}
