package nanomsg

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/sub"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"

	// Import all transport types.
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

const (
	niFieldURLs        = "urls"
	niFieldBind        = "bind"
	niFieldSocketType  = "socket_type"
	niFieldSubFilters  = "sub_filters"
	niFieldPollTimeout = "poll_timeout"
)

func inputConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary(`Consumes messages via Nanomsg sockets (scalability protocols).`).
		Description(`Currently only PULL and SUB sockets are supported.`).
		Fields(
			service.NewURLListField(niFieldURLs).
				Description("A list of URLs to connect to (or as). If an item of the list contains commas it will be expanded into multiple URLs."),
			service.NewBoolField(niFieldBind).
				Description("Whether the URLs provided should be connected to, or bound as.").
				Default(true),
			service.NewStringEnumField(niFieldSocketType, "PULL", "SUB").
				Description("The socket type to use.").
				Default("PULL"),
			service.NewAutoRetryNacksToggleField(),
			service.NewStringListField(niFieldSubFilters).
				Description("A list of subscription topic filters to use when consuming from a SUB socket. Specifying a single sub_filter of `''` will subscribe to everything.").
				Default([]any{}),
			service.NewDurationField(niFieldPollTimeout).
				Description("The period to wait until a poll is abandoned and reattempted.").
				Advanced().
				Default("5s"),
		)
}

func init() {
	err := service.RegisterInput("nanomsg", inputConfigSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
		rdr, err := newNanomsgReaderFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksToggled(conf, rdr)
	})
	if err != nil {
		panic(err)
	}
}

type nanomsgReader struct {
	socket mangos.Socket
	cMut   sync.Mutex

	urls        []string
	bind        bool
	socketType  string
	subFilters  []string
	pollTimeout time.Duration
	repTimeout  time.Duration

	log *service.Logger
}

func newNanomsgReaderFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (rdr *nanomsgReader, err error) {
	rdr = &nanomsgReader{
		log:        mgr.Logger(),
		repTimeout: time.Second * 5,
	}

	var cURLs []*url.URL
	if cURLs, err = conf.FieldURLList(niFieldURLs); err != nil {
		return
	}
	for _, u := range cURLs {
		rdr.urls = append(rdr.urls, strings.Replace(u.String(), "//*:", "//0.0.0.0:", 1))
	}

	if rdr.socketType, err = conf.FieldString(niFieldSocketType); err != nil {
		return
	}

	if rdr.subFilters, err = conf.FieldStringList(niFieldSubFilters); err != nil {
		return
	}

	if rdr.bind, err = conf.FieldBool(niFieldBind); err != nil {
		return
	}

	if rdr.socketType == "SUB" && len(rdr.subFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
	}

	if rdr.pollTimeout, err = conf.FieldDuration(niFieldPollTimeout); err != nil {
		return
	}
	return
}

func getInputSocketFromType(t string) (mangos.Socket, error) {
	switch t {
	case "PULL":
		return pull.NewSocket()
	case "SUB":
		return sub.NewSocket()
	}
	return nil, errors.New("invalid Scalability Protocols socket type")
}

func (s *nanomsgReader) Connect(ctx context.Context) (err error) {
	s.cMut.Lock()
	defer s.cMut.Unlock()

	if s.socket != nil {
		return nil
	}

	var socket mangos.Socket

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	socket, err = getInputSocketFromType(s.socketType)
	if err != nil {
		return err
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

	for _, filter := range s.subFilters {
		if err := socket.SetOption(mangos.OptionSubscribe, []byte(filter)); err != nil {
			return err
		}
	}
	s.socket = socket
	return nil
}

func (s *nanomsgReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	s.cMut.Lock()
	socket := s.socket
	s.cMut.Unlock()

	if socket == nil {
		return nil, nil, service.ErrNotConnected
	}
	data, err := socket.Recv()
	if err != nil {
		if errors.Is(err, mangos.ErrRecvTimeout) {
			return nil, nil, component.ErrTimeout
		}
		return nil, nil, err
	}
	return service.NewMessage(data), func(ctx context.Context, err error) error {
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
