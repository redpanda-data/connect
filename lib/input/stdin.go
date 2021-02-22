package input

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
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
	Constructors[TypeSTDIN] = TypeSpec{
		constructor: fromSimpleConstructor(NewSTDIN),
		Summary: `
Consumes data piped to stdin as line delimited messages.`,
		Description: `
If the multipart option is set to true then lines are interpretted as message
parts, and an empty line indicates the end of the message.

If the delimiter field is left empty then line feed (\n) is used.`,
		FieldSpecs: docs.FieldSpecs{
			codec.ReaderDocs.AtVersion("3.42.0"),
			docs.FieldAdvanced("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed."),
			docs.FieldDeprecated("delimiter"),
			docs.FieldDeprecated("multipart"),
		},
		Categories: []Category{
			CategoryLocal,
		},
	}
}

//------------------------------------------------------------------------------

// STDINConfig contains config fields for the STDIN input type.
type STDINConfig struct {
	Codec     string `json:"codec" yaml:"codec"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewSTDINConfig creates a STDINConfig populated with default values.
func NewSTDINConfig() STDINConfig {
	return STDINConfig{
		Codec:     "lines",
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// NewSTDIN creates a new STDIN input type.
func NewSTDIN(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	rdr, err := newStdinConsumer(conf.STDIN)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(
		TypeSTDIN, true,
		reader.NewAsyncCutOff(reader.NewAsyncPreserver(rdr)),
		log, stats,
	)
}

//------------------------------------------------------------------------------

type stdinConsumer struct {
	scanner codec.Reader
}

func newStdinConsumer(conf STDINConfig) (*stdinConsumer, error) {
	if len(conf.Delim) > 0 {
		conf.Codec = "delim:" + conf.Delim
	}
	if conf.Multipart && !strings.HasSuffix(conf.Codec, "/multipart") {
		conf.Codec = conf.Codec + "/multipart"
	}

	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = conf.MaxBuffer
	ctor, err := codec.GetReader(conf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	scanner, err := ctor("", os.Stdin, func(_ context.Context, err error) error {
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &stdinConsumer{scanner}, nil
}

// ConnectWithContext attempts to establish a connection to the target S3 bucket
// and any relevant queues used to traverse the objects (SQS, etc).
func (s *stdinConsumer) ConnectWithContext(ctx context.Context) error {
	return nil
}

// ReadWithContext attempts to read a new message from the target S3 bucket.
func (s *stdinConsumer) ReadWithContext(ctx context.Context) (types.Message, reader.AsyncAckFn, error) {
	parts, codecAckFn, err := s.scanner.Next(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			err = types.ErrTimeout
		}
		if err != types.ErrTimeout {
			s.scanner.Close(ctx)
		}
		if errors.Is(err, io.EOF) {
			return nil, nil, types.ErrTypeClosed
		}
		return nil, nil, err
	}
	codecAckFn(ctx, nil)

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
func (s *stdinConsumer) CloseAsync() {
	go func() {
		if s.scanner != nil {
			s.scanner.Close(context.Background())
		}
	}()
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (s *stdinConsumer) WaitForClose(time.Duration) error {
	return nil
}
