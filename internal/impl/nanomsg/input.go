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
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/sub"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"

	// Import all transport types.
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newNanomsgInput), docs.ComponentSpec{
		Name:        "nanomsg",
		Summary:     `Consumes messages via Nanomsg sockets (scalability protocols).`,
		Description: `Currently only PULL and SUB sockets are supported.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldURL("urls", "A list of URLs to connect to (or as). If an item of the list contains commas it will be expanded into multiple URLs.").Array(),
			docs.FieldBool("bind", "Whether the URLs provided should be connected to, or bound as."),
			docs.FieldString("socket_type", "The socket type to use.").HasOptions("PULL", "SUB"),
			docs.FieldString("sub_filters", "A list of subscription topic filters to use when consuming from a SUB socket. Specifying a single sub_filter of `''` will subscribe to everything.").Array(),
			docs.FieldString("poll_timeout", "The period to wait until a poll is abandoned and reattempted.").Advanced(),
		).ChildDefaultAndTypesFromStruct(input.NewNanomsgConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newNanomsgInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	s, err := newNanomsgReader(conf.Nanomsg, mgr.Logger())
	if err != nil {
		return nil, err
	}
	return input.NewAsyncReader("nanomsg", input.NewAsyncPreserver(s), mgr)
}

type nanomsgReader struct {
	socket mangos.Socket
	cMut   sync.Mutex

	pollTimeout time.Duration
	repTimeout  time.Duration

	urls []string
	conf input.NanomsgConfig
	log  log.Modular
}

func newNanomsgReader(conf input.NanomsgConfig, log log.Modular) (*nanomsgReader, error) {
	s := nanomsgReader{
		conf:       conf,
		log:        log,
		repTimeout: time.Second * 5,
	}

	for _, u := range conf.URLs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				s.urls = append(s.urls, strings.Replace(splitU, "//*:", "//0.0.0.0:", 1))
			}
		}
	}

	if conf.SocketType == "SUB" && len(conf.SubFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
	}

	if tout := conf.PollTimeout; len(tout) > 0 {
		var err error
		if s.pollTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse poll timeout string: %v", err)
		}
	}

	return &s, nil
}

func getSocketFromType(t string) (mangos.Socket, error) {
	switch t {
	case "PULL":
		return pull.NewSocket()
	case "SUB":
		return sub.NewSocket()
	case "PUSH":
		return push.NewSocket()
	case "PUB":
		return pub.NewSocket()
	}
	return nil, errors.New("invalid Scalability Protocols socket type")
}

func (s *nanomsgReader) Connect(ctx context.Context) error {
	s.cMut.Lock()
	defer s.cMut.Unlock()

	if s.socket != nil {
		return nil
	}

	var socket mangos.Socket
	var err error

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	socket, err = getSocketFromType(s.conf.SocketType)
	if err != nil {
		return err
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

	// TODO: This is only used for request/response sockets, and is invalid with
	// other socket types.
	// err = socket.SetOption(mangos.OptionSendDeadline, s.pollTimeout)
	// if err != nil {
	// 	return err
	// }

	// Set timeout to prevent endless lock.
	err = socket.SetOption(mangos.OptionRecvDeadline, s.repTimeout)
	if err != nil {
		return err
	}

	for _, filter := range s.conf.SubFilters {
		if err := socket.SetOption(mangos.OptionSubscribe, []byte(filter)); err != nil {
			return err
		}
	}

	if s.conf.Bind {
		s.log.Infof(
			"Receiving Scalability Protocols messages at bound URLs: %s\n",
			s.urls,
		)
	} else {
		s.log.Infof(
			"Receiving Scalability Protocols messages at connected URLs: %s\n",
			s.urls,
		)
	}

	s.socket = socket
	return nil
}

func (s *nanomsgReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	s.cMut.Lock()
	socket := s.socket
	s.cMut.Unlock()

	if socket == nil {
		return nil, nil, component.ErrNotConnected
	}
	data, err := socket.Recv()
	if err != nil {
		if errors.Is(err, mangos.ErrRecvTimeout) {
			return nil, nil, component.ErrTimeout
		}
		return nil, nil, err
	}
	return message.QuickBatch([][]byte{data}), func(ctx context.Context, err error) error {
		return nil
	}, nil
}

func (s *nanomsgReader) Close(ctx context.Context) (err error) {
	s.cMut.Lock()
	defer s.cMut.Unlock()

	if s.socket != nil {
		err = s.socket.Close()
		s.socket = nil
	}
	return
}
