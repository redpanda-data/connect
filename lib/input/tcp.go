package input

import (
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
	Constructors[TypeTCP] = TypeSpec{
		constructor: NewTCP,
		Description: `
Connects to a TCP server and consumes a continuous stream of messages.

If multipart is set to false each line of data is read as a separate message. If
multipart is set to true each line is read as a message part, and an empty line
indicates the end of a message.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
` + "`pipeline`" + ` section of a config.

If the delimiter field is left empty then line feed (\n) is used.`,
		Status: docs.StatusDeprecated,
	}
}

//------------------------------------------------------------------------------

// TCPConfig contains configuration values for the TCP input type.
type TCPConfig struct {
	Address   string `json:"address" yaml:"address"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewTCPConfig creates a new TCPConfig with default values.
func NewTCPConfig() TCPConfig {
	return TCPConfig{
		Address:   "localhost:4194",
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// NewTCP creates a new TCP input type.
func NewTCP(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	log.Warnln("The tcp input is deprecated, please use socket instead.")
	delim := conf.TCP.Delim
	if len(delim) == 0 {
		delim = "\n"
	}
	var conn net.Conn
	rdr, err := reader.NewLines(
		func() (io.Reader, error) {
			if conn != nil {
				conn.Close()
				conn = nil
			}
			var err error
			conn, err = net.Dial("tcp", conf.TCP.Address)
			return conn, err
		},
		func() {
			if conn != nil {
				conn.Close()
				conn = nil
			}
		},
		reader.OptLinesSetDelimiter(delim),
		reader.OptLinesSetMaxBuffer(conf.TCP.MaxBuffer),
		reader.OptLinesSetMultipart(conf.TCP.Multipart),
	)
	if err != nil {
		return nil, err
	}
	return NewAsyncReader(
		TypeTCP,
		true,
		reader.NewAsyncPreserver(rdr),
		log, stats,
	)
}

//------------------------------------------------------------------------------
