// Copyright (c) 2019 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	Constructors[TypeTCPServer] = TypeSpec{
		constructor: NewTCPServer,
		description: `
Creates a server that receives messages over TCP. Each connection is parsed as a
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

// TCPServerConfig contains configuration for the TCPServer input type.
type TCPServerConfig struct {
	Address   string `json:"address" yaml:"address"`
	Multipart bool   `json:"multipart" yaml:"multipart"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewTCPServerConfig creates a new TCPServerConfig with default values.
func NewTCPServerConfig() TCPServerConfig {
	return TCPServerConfig{
		Address:   "127.0.0.1:0",
		Multipart: false,
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// TCPServer is an input type that binds to an address and consumes streams of
// messages over TCP.
type TCPServer struct {
	running int32

	conf  TCPServerConfig
	stats metrics.Type
	log   log.Modular

	delim    []byte
	listener net.Listener

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewTCPServer creates a new TCPServer input type.
func NewTCPServer(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	ln, err := net.Listen("tcp", conf.TCPServer.Address)
	if err != nil {
		return nil, err
	}
	delim := []byte("\n")
	if len(conf.TCPServer.Delim) > 0 {
		delim = []byte(conf.TCPServer.Delim)
	}
	t := TCPServer{
		running: 1,
		conf:    conf.TCPServer,
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

// Addr returns the underlying TCP listeners address.
func (t *TCPServer) Addr() net.Addr {
	return t.listener.Addr()
}

func (t *TCPServer) newScanner(r io.Reader) *bufio.Scanner {
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

func (t *TCPServer) loop() {
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

	t.log.Infof("Receiving TCP messages from address: %v\n", t.listener.Addr())

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
					t.log.Errorf("Failed to accept TCP connection: %v\n", err)
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
func (t *TCPServer) TransactionChan() <-chan types.Transaction {
	return t.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (t *TCPServer) Connected() bool {
	return true
}

// CloseAsync shuts down the TCPServer input and stops processing requests.
func (t *TCPServer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&t.running, 1, 0) {
		close(t.closeChan)
	}
}

// WaitForClose blocks until the TCPServer input has closed down.
func (t *TCPServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-t.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
