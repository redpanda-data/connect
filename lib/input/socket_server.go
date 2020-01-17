package input

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocketServer] = TypeSpec{
		constructor: NewSocketServer,
		Description: `
Creates a server that receives messages over Socket. Each connection is parsed as a
continuous stream of line delimited messages.

If multipart is set to false each line of data is read as a separate message. If
multipart is set to true each line is read as a message part, and an empty line
indicates the end of a message.

If the delimiter field is left empty then line feed (\n) is used.

The field ` + "`max_buffer`" + ` specifies the maximum amount of memory to
allocate _per connection_ for buffering lines of data. If a line of data from a
connection exceeds this value then the connection will be closed.`,
	}
}

//------------------------------------------------------------------------------

// SocketServerConfig contains configuration for the SocketServer input type.
type SocketServerConfig struct {
	Network   string `json:"network" yaml:"network"`
	Address   string `json:"address" yaml:"address"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewSocketServerConfig creates a new SocketServerConfig with default values.
func NewSocketServerConfig() SocketServerConfig {
	return SocketServerConfig{
		Network:   "unix",
		Address:   "/tmp/benthos.sock",
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// SocketServer is an input type that binds to an address and consumes streams of
// messages over Socket.
type SocketServer struct {
	running int32

	conf  SocketServerConfig
	stats metrics.Type
	log   log.Modular

	delim    []byte
	listener net.Listener

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewSocketServer creates a new SocketServer input type.
func NewSocketServer(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	ln, err := net.Listen(conf.SocketServer.Network, conf.SocketServer.Address)
	if err != nil {
		return nil, err
	}
	delim := []byte("\n")
	if len(conf.SocketServer.Delim) > 0 {
		delim = []byte(conf.SocketServer.Delim)
	}
	t := SocketServer{
		running: 1,
		conf:    conf.SocketServer,
		stats:   stats,
		log:     log,

		delim:    delim,
		listener: ln,

		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	go t.loop()
	return &t, nil
}

//------------------------------------------------------------------------------

// Addr returns the underlying Socket listeners address.
func (t *SocketServer) Addr() net.Addr {
	return t.listener.Addr()
}

func (t *SocketServer) newScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	if t.conf.MaxBuffer != bufio.MaxScanTokenSize {
		scanner.Buffer([]byte{}, t.conf.MaxBuffer)
	}

	scanner.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}

		if i := bytes.Index(data, t.delim); i >= 0 {
			// We have a full terminated line.
			return i + len(t.delim), data[0:i], nil
		}

		// If we're at EOF, we have a final, non-terminated line. Return it.
		if atEOF {
			return len(data), data, nil
		}

		// Request more data.
		return 0, nil, nil
	})

	return scanner
}

func (t *SocketServer) loop() {
	var (
		mCount     = t.stats.GetCounter("count")
		mRcvd      = t.stats.GetCounter("batch.received")
		mPartsRcvd = t.stats.GetCounter("received")
		mLatency   = t.stats.GetTimer("latency")
	)

	defer func() {
		atomic.StoreInt32(&t.running, 0)

		if t.listener != nil {
			t.listener.Close()
		}

		close(t.transactions)
		close(t.closedChan)
	}()

	t.log.Infof("Receiving Socket messages from address: %v\n", t.listener.Addr())

	sendMsg := func(msg types.Message) error {
		tStarted := time.Now()
		mPartsRcvd.Incr(int64(msg.Len()))
		mRcvd.Incr(1)

		resChan := make(chan types.Response)
		select {
		case t.transactions <- types.NewTransaction(msg, resChan):
		case <-t.closeChan:
			return types.ErrTypeClosed
		}

		select {
		case res, open := <-resChan:
			if !open {
				return types.ErrTypeClosed
			}
			if res != nil {
				if res.Error() != nil {
					return res.Error()
				}
			}
		case <-t.closeChan:
			return types.ErrTypeClosed
		}
		mLatency.Timing(time.Since(tStarted).Nanoseconds())
		return nil
	}

	go func() {
		for {
			conn, err := t.listener.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					t.log.Errorf("Failed to accept Socket connection: %v\n", err)
				}
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := t.newScanner(c)
				var msg types.Message
				msgLoop := func() {
					for msg != nil {
						sendErr := sendMsg(msg)
						if sendErr == nil || sendErr == types.ErrTypeClosed {
							msg = nil
							return
						}
						t.log.Errorf("Failed to send message: %v\n", sendErr)
						<-time.After(time.Second)
					}
				}
				for scanner.Scan() {
					mCount.Incr(1)
					if len(scanner.Bytes()) == 0 {
						if t.conf.Multipart && msg != nil {
							msgLoop()
						}
						continue
					}
					if msg == nil {
						msg = message.New(nil)
					}
					msg.Append(message.NewPart(scanner.Bytes()))
					if !t.conf.Multipart {
						msgLoop()
					}
				}
				if msg != nil {
					msgLoop()
				}
				if cerr := scanner.Err(); cerr != nil {
					if cerr != io.EOF {
						t.log.Errorf("Connection error due to: %v\n", cerr)
					}
				}
			}(conn)
		}
	}()
	<-t.closeChan
}

// TransactionChan returns a transactions channel for consuming messages from
// this input.
func (t *SocketServer) TransactionChan() <-chan types.Transaction {
	return t.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (t *SocketServer) Connected() bool {
	return true
}

// CloseAsync shuts down the SocketServer input and stops processing requests.
func (t *SocketServer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&t.running, 1, 0) {
		close(t.closeChan)
	}
}

// WaitForClose blocks until the SocketServer input has closed down.
func (t *SocketServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-t.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
