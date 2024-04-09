package nanomsg

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/push"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/public/service"

	// Import all transport types.
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

const (
	noFieldURLs        = "urls"
	noFieldBind        = "bind"
	noFieldSocketType  = "socket_type"
	noFieldPollTimeout = "poll_timeout"
)

func outputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary(`Send messages over a Nanomsg socket.`).
		Description(output.Description(true, false, `Currently only PUSH and PUB sockets are supported.`)).
		Fields(
			service.NewURLListField(noFieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs."),
			service.NewBoolField(noFieldBind).
				Description("Whether the URLs listed should be bind (otherwise they are connected to).").
				Default(false),
			service.NewStringEnumField(noFieldSocketType, "PUSH", "PUB").
				Description("The socket type to send with.").
				Default("PUSH"),
			service.NewDurationField(noFieldPollTimeout).
				Description("The maximum period of time to wait for a message to send before the request is abandoned and reattempted.").
				Default("5s"),
			service.NewOutputMaxInFlightField(),
		)
}

func init() {
	err := service.RegisterOutput("nanomsg", outputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
		wtr, err := newNanomsgWriterFromParsed(conf, mgr)
		if err != nil {
			return nil, 0, err
		}
		mIF, err := conf.FieldMaxInFlight()
		if err != nil {
			return nil, 0, err
		}
		return wtr, mIF, nil
	})
	if err != nil {
		panic(err)
	}
}

type nanomsgWriter struct {
	log *service.Logger

	urls        []string
	bind        bool
	pollTimeout time.Duration
	socketType  string

	socket  mangos.Socket
	sockMut sync.RWMutex
}

func newNanomsgWriterFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (wtr *nanomsgWriter, err error) {
	wtr = &nanomsgWriter{
		log: mgr.Logger(),
	}

	var cURLs []*url.URL
	if cURLs, err = conf.FieldURLList(noFieldURLs); err != nil {
		return
	}
	for _, u := range cURLs {
		wtr.urls = append(wtr.urls, strings.Replace(u.String(), "//*:", "//0.0.0.0:", 1))
	}

	if wtr.socketType, err = conf.FieldString(noFieldSocketType); err != nil {
		return
	}

	if wtr.bind, err = conf.FieldBool(noFieldBind); err != nil {
		return
	}

	if wtr.pollTimeout, err = conf.FieldDuration(noFieldPollTimeout); err != nil {
		return
	}
	return
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

	socket, err := getOutputSocketFromType(s.socketType)
	if err != nil {
		return err
	}

	// Set timeout to prevent endless lock.
	if s.socketType == "PUSH" {
		if err := socket.SetOption(
			mangos.OptionSendDeadline, s.pollTimeout,
		); err != nil {
			return err
		}
	}

	if s.bind {
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
	s.socket = socket
	return nil
}

func (s *nanomsgWriter) Write(ctx context.Context, msg *service.Message) error {
	s.sockMut.RLock()
	socket := s.socket
	s.sockMut.RUnlock()

	if socket == nil {
		return service.ErrNotConnected
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	return socket.Send(mBytes)
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
