package input

import (
	"fmt"
	"io"
	"net"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/input/reader"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocket] = TypeSpec{
		constructor: fromSimpleConstructor(NewSocket),
		Summary: `
Connects to a (tcp/unix) socket and consumes a continuous stream of messages.`,
		Description: `
If multipart is set to false each line of data is read as a separate message. If
multipart is set to true each line is read as a message part, and an empty line
indicates the end of a message.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

If the delimiter field is left empty then line feed (\n) is used.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("network", "A network type to assume (unix|tcp).").HasOptions(
				"unix", "tcp",
			),
			docs.FieldCommon("address", "The address to connect to.", "/tmp/benthos.sock", "127.0.0.1:6000"),
			docs.FieldAdvanced("multipart", "Whether messages should be consumed as multiple parts. If so, each line is consumed as a message parts and the full message ends with an empty line."),
			docs.FieldAdvanced("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed."),
			docs.FieldAdvanced("delimiter", "The delimiter to use to detect the end of each message. If left empty line breaks are used."),
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
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewSocketConfig creates a new SocketConfig with default values.
func NewSocketConfig() SocketConfig {
	return SocketConfig{
		Network:   "unix",
		Address:   "/tmp/benthos.sock",
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// NewSocket creates a new Socket input type.
func NewSocket(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	delim := conf.Socket.Delim
	if len(delim) == 0 {
		delim = "\n"
	}
	switch conf.Socket.Network {
	case "tcp", "unix":
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this input", conf.Socket.Network)
	}
	var conn net.Conn
	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			if conn != nil {
				conn.Close()
				conn = nil
			}
			var err error
			conn, err = net.Dial(conf.Socket.Network, conf.Socket.Address)
			return conn, err
		},
		func() {
			if conn != nil {
				conn.Close()
				conn = nil
			}
		},
		reader.OptLinesSetDelimiter(delim),
		reader.OptLinesSetMaxBuffer(conf.Socket.MaxBuffer),
		reader.OptLinesSetMultipart(conf.Socket.Multipart),
	)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(
		TypeSocket,
		true,
		reader.NewAsyncPreserver(rdr),
		log, stats,
	)
}

//------------------------------------------------------------------------------
