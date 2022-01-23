package input

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewSocket),
		Summary: `
Connects to a tcp or unix socket and consumes a continuous stream of messages.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("network", "A network type to assume (unix|tcp).").HasOptions(
				"unix", "tcp",
			),
			docs.FieldCommon("address", "The address to connect to.", "/tmp/benthos.sock", "127.0.0.1:6000"),
			codec.ReaderDocs.AtVersion("3.42.0"),
			docs.FieldAdvanced("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed."),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// SocketConfig contains configuration values for the Socket input type.
type SocketConfig struct {
	Network   string `json:"network" yaml:"network"`
	Address   string `json:"address" yaml:"address"`
	Codec     string `json:"codec" yaml:"codec"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
}

// NewSocketConfig creates a new SocketConfig with default values.
func NewSocketConfig() SocketConfig {
	return SocketConfig{
		Network:   "unix",
		Address:   "/tmp/benthos.sock",
		Codec:     "lines",
		MaxBuffer: 1000000,
	}
}

//------------------------------------------------------------------------------

// NewSocket creates a new Socket input type.
func NewSocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	rdr, err := newSocketClient(conf.Socket, log)
	if err != nil {
		return nil, err
	}
	// TODO: Consider removing the async cut off here. It adds an overhead and
	// we can get the same results by making sure that the async readers forward
	// CloseAsync all the way through. We would need it to be configurable as it
	// wouldn't be appropriate for inputs that have real acks.
	return NewAsyncReader(TypeSocket, true, reader.NewAsyncCutOff(reader.NewAsyncPreserver(rdr)), log, stats)
}

//------------------------------------------------------------------------------

type socketClient struct {
	log log.Modular

	conf      SocketConfig
	codecCtor codec.ReaderConstructor

	codecMut sync.Mutex
	codec    codec.Reader
}

func newSocketClient(conf SocketConfig, logger log.Modular) (*socketClient, error) {
	switch conf.Network {
	case "tcp", "unix":
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this input", conf.Network)
	}

	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	return &socketClient{
		log:       logger,
		conf:      conf,
		codecCtor: ctor,
	}, nil
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (s *socketClient) ConnectWithContext(ctx context.Context) error {
	s.codecMut.Lock()
	defer s.codecMut.Unlock()

	if s.codec != nil {
		return nil
	}

	conn, err := net.Dial(s.conf.Network, s.conf.Address)
	if err != nil {
		return err
	}

	if s.codec, err = s.codecCtor("", conn, func(ctx context.Context, err error) error {
		return nil
	}); err != nil {
		conn.Close()
		return err
	}

	s.log.Infof("Consuming from socket at '%v://%v'\n", s.conf.Network, s.conf.Address)
	return nil
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (s *socketClient) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	s.codecMut.Lock()
	codec := s.codec
	s.codecMut.Unlock()

	if codec == nil {
		return nil, nil, types.ErrNotConnected
	}

	parts, codecAckFn, err := codec.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = types.ErrTimeout
		}
		if err != types.ErrTimeout {
			s.codecMut.Lock()
			if s.codec != nil && s.codec == codec {
				s.codec.Close(ctx)
				s.codec = nil
			}
			s.codecMut.Unlock()
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, types.ErrTimeout
		}
		return nil, nil, err
	}

	// We simply bounce rejected messages in a loop downstream so there's no
	// benefit to aggregating acks.
	_ = codecAckFn(context.Background(), nil)

	msg := message.New(nil)
	msg.Append(parts...)

	if msg.Len() == 0 {
		return nil, nil, types.ErrTimeout
	}

	return msg, func(rctx context.Context, res types.Response) error {
		return nil
	}, nil
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (s *socketClient) CloseAsync() {
	s.codecMut.Lock()
	if s.codec != nil {
		s.codec.Close(context.Background())
		s.codec = nil
	}
	s.codecMut.Unlock()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *socketClient) WaitForClose(time.Duration) error {
	return nil
}
