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
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeUDPServer] = TypeSpec{
		constructor: NewUDPServer,
		description: `
Creates a server that receives messages over UDP as a continuous stream of data.
Each line is interpretted as an individual message, if the delimiter field is
left empty then line feed (\n) is used.

The field ` + "`max_buffer`" + ` specifies the maximum amount of memory to
allocate for buffering lines of data, this must exceed the largest expected
message size.`,
	}
}

//------------------------------------------------------------------------------

// UDPServerConfig contains configuration for the UDPServer input type.
type UDPServerConfig struct {
	Address   string `json:"address" yaml:"address"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
	Delim     string `json:"delimiter" yaml:"delimiter"`
}

// NewUDPServerConfig creates a new UDPServerConfig with default values.
func NewUDPServerConfig() UDPServerConfig {
	return UDPServerConfig{
		Address:   "127.0.0.1:0",
		MaxBuffer: 1000000,
		Delim:     "",
	}
}

//------------------------------------------------------------------------------

// UDPServer is an input type that binds to an address and consumes streams of
// messages over UDP.
type UDPServer struct {
	running int32

	conf  UDPServerConfig
	stats metrics.Type
	log   log.Modular

	delim []byte
	conn  net.PacketConn

	transactions chan types.Transaction

	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewUDPServer creates a new UDPServer input type.
func NewUDPServer(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	pc, err := net.ListenPacket("udp", conf.UDPServer.Address)
	if err != nil {
		return nil, err
	}
	delim := []byte("\n")
	if len(conf.UDPServer.Delim) > 0 {
		delim = []byte(conf.UDPServer.Delim)
	}
	t := UDPServer{
		running: 1,
		conf:    conf.UDPServer,
		stats:   stats,
		log:     log,

		delim: delim,
		conn:  pc,

		transactions: make(chan types.Transaction),
		closeChan:    make(chan struct{}),
		closedChan:   make(chan struct{}),
	}

	go t.loop()
	return &t, nil
}

//------------------------------------------------------------------------------

// Addr returns the underlying UDP listeners address.
func (t *UDPServer) Addr() net.Addr {
	return t.conn.LocalAddr()
}

type wrapPacketConn struct {
	r net.PacketConn
}

func (w *wrapPacketConn) Read(p []byte) (n int, err error) {
	n, _, err = w.r.ReadFrom(p)
	return
}

func (t *UDPServer) newScanner(r net.PacketConn) *bufio.Scanner {
	scanner := bufio.NewScanner(&wrapPacketConn{r: r})
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

func (t *UDPServer) loop() {
	var (
		mCount     = t.stats.GetCounter("count")
		mRcvd      = t.stats.GetCounter("batch.received")
		mPartsRcvd = t.stats.GetCounter("received")
		mLatency   = t.stats.GetTimer("latency")
	)

	defer func() {
		atomic.StoreInt32(&t.running, 0)

		if t.conn != nil {
			t.conn.Close()
		}

		close(t.transactions)
		close(t.closedChan)
	}()

	t.log.Infof("Receiving UDP messages from address: %v\n", t.conn.LocalAddr())

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
		for {
			scanner := t.newScanner(t.conn)
			for scanner.Scan() {
				mCount.Incr(1)
				if len(scanner.Bytes()) == 0 {
					continue
				}
				if msg == nil {
					msg = message.New(nil)
				}
				msg.Append(message.NewPart(scanner.Bytes()))
				msgLoop()
			}
			if msg != nil {
				msgLoop()
			}
			if cerr := scanner.Err(); cerr != nil {
				if cerr != io.EOF {
					t.log.Errorf("Connection error due to: %v\n", cerr)
				}
			}
		}
	}()
	<-t.closeChan
}

// TransactionChan returns a transactions channel for consuming messages from
// this input.
func (t *UDPServer) TransactionChan() <-chan types.Transaction {
	return t.transactions
}

// Connected returns a boolean indicating whether this input is currently
// connected to its target.
func (t *UDPServer) Connected() bool {
	return true
}

// CloseAsync shuts down the UDPServer input and stops processing requests.
func (t *UDPServer) CloseAsync() {
	if atomic.CompareAndSwapInt32(&t.running, 1, 0) {
		close(t.closeChan)
	}
}

// WaitForClose blocks until the UDPServer input has closed down.
func (t *UDPServer) WaitForClose(timeout time.Duration) error {
	select {
	case <-t.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
