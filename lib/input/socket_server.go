package input

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/codec"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeSocketServer] = TypeSpec{
		constructor: fromSimpleConstructor(NewSocketServer),
		Summary:     `Creates a server that receives a stream of messages over a tcp, udp or unix socket.`,
		Description: `
The field ` + "`max_buffer`" + ` specifies the maximum amount of memory to allocate _per connection_ for buffering lines of data. If a line of data from a connection exceeds this value then the connection will be closed.`,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("network", "A network type to accept (unix|tcp|udp).").HasOptions(
				"unix", "tcp", "udp",
			),
			docs.FieldCommon("address", "The address to listen from.", "/tmp/benthos.sock", "0.0.0.0:6000"),
			codec.ReaderDocs.AtVersion("3.42.0"),
			docs.FieldAdvanced("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed."),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// SocketServerConfig contains configuration for the SocketServer input type.
type SocketServerConfig struct {
	Network   string `json:"network" yaml:"network"`
	Address   string `json:"address" yaml:"address"`
	Codec     string `json:"codec" yaml:"codec"`
	MaxBuffer int    `json:"max_buffer" yaml:"max_buffer"`
}

// NewSocketServerConfig creates a new SocketServerConfig with default values.
func NewSocketServerConfig() SocketServerConfig {
	return SocketServerConfig{
		Network:   "unix",
		Address:   "/tmp/benthos.sock",
		Codec:     "lines",
		MaxBuffer: 1000000,
	}
}

//------------------------------------------------------------------------------

type wrapPacketConn struct {
	net.PacketConn
}

func (w *wrapPacketConn) Read(p []byte) (n int, err error) {
	n, _, err = w.ReadFrom(p)
	return
}

// SocketServer is an input type that binds to an address and consumes streams of
// messages over Socket.
type SocketServer struct {
	conf  SocketServerConfig
	stats metrics.Type
	log   log.Modular

	codecCtor codec.ReaderConstructor
	listener  net.Listener
	conn      net.PacketConn

	retriesMut   sync.RWMutex
	transactions chan types.Transaction

	ctx        context.Context
	closeFn    func()
	closedChan chan struct{}

	mLatency metrics.StatTimer
}

// NewSocketServer creates a new SocketServer input type.
func NewSocketServer(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	var ln net.Listener
	var cn net.PacketConn
	var err error

	sconf := conf.SocketServer

	codecConf := codec.NewReaderConfig()
	codecConf.MaxScanTokenSize = sconf.MaxBuffer
	ctor, err := codec.GetReader(sconf.Codec, codecConf)
	if err != nil {
		return nil, err
	}

	switch sconf.Network {
	case "tcp", "unix":
		ln, err = net.Listen(sconf.Network, sconf.Address)
	case "udp":
		cn, err = net.ListenPacket(sconf.Network, sconf.Address)
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this input", sconf.Network)
	}
	if err != nil {
		return nil, err
	}

	t := SocketServer{
		conf:  conf.SocketServer,
		stats: stats,
		log:   log,

		codecCtor: ctor,
		listener:  ln,
		conn:      cn,

		transactions: make(chan types.Transaction),
		closedChan:   make(chan struct{}),

		mLatency: stats.GetTimer("latency"),
	}
	t.ctx, t.closeFn = context.WithCancel(context.Background())

	if ln == nil {
		go t.udpLoop()
	} else {
		go t.loop()
	}
	return &t, nil
}

//------------------------------------------------------------------------------

// Addr returns the underlying Socket listeners address.
func (t *SocketServer) Addr() net.Addr {
	if t.listener != nil {
		return t.listener.Addr()
	}
	return t.conn.LocalAddr()
}

func (t *SocketServer) sendMsg(msg types.Message) bool {
	tStarted := time.Now()

	// Block whilst retries are happening
	t.retriesMut.Lock()
	// nolint:staticcheck, gocritic // Ignore SA2001 empty critical section, Ignore badLock
	t.retriesMut.Unlock()

	resChan := make(chan types.Response)
	select {
	case t.transactions <- types.NewTransaction(msg, resChan):
	case <-t.ctx.Done():
		return false
	}

	go func() {
		hasLocked := false
		defer func() {
			if hasLocked {
				t.retriesMut.RUnlock()
			}
		}()
		for {
			select {
			case res, open := <-resChan:
				if !open {
					return
				}
				var sendErr error
				if res != nil {
					sendErr = res.Error()
				}
				if sendErr == nil || sendErr == types.ErrTypeClosed {
					if sendErr == nil {
						t.mLatency.Timing(time.Since(tStarted).Nanoseconds())
					}
					return
				}
				if !hasLocked {
					hasLocked = true
					t.retriesMut.RLock()
				}
				t.log.Errorf("failed to send message: %v\n", sendErr)

				// Wait before attempting again
				select {
				case <-time.After(time.Second):
				case <-t.ctx.Done():
					return
				}

				// And then resend the transaction
				select {
				case t.transactions <- types.NewTransaction(msg, resChan):
				case <-t.ctx.Done():
					return
				}
			case <-t.ctx.Done():
				return
			}
		}
	}()
	return true
}

func (t *SocketServer) loop() {
	var (
		mCount     = t.stats.GetCounter("count")
		mRcvd      = t.stats.GetCounter("batch.received")
		mPartsRcvd = t.stats.GetCounter("received")
	)

	var wg sync.WaitGroup

	defer func() {
		wg.Wait()

		t.retriesMut.Lock()
		// nolint:staticcheck, gocritic // Ignore SA2001 empty critical section, Ignore badLock
		t.retriesMut.Unlock()

		t.listener.Close()

		close(t.transactions)
		close(t.closedChan)
	}()

	t.log.Infof("Receiving %v socket messages from address: %v\n", t.conf.Network, t.listener.Addr())

	go func() {
		<-t.ctx.Done()
		t.listener.Close()
	}()

acceptLoop:
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				t.log.Errorf("Failed to accept Socket connection: %v\n", err)
			}
			select {
			case <-time.After(time.Second):
				continue acceptLoop
			case <-t.ctx.Done():
				return
			}
		}
		connCtx, connDone := context.WithCancel(t.ctx)
		go func() {
			<-connCtx.Done()
			conn.Close()
		}()
		wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				connDone()
				wg.Done()
				c.Close()
			}()
			codec, err := t.codecCtor("", c, func(ctx context.Context, err error) error {
				return nil
			})
			if err != nil {
				t.log.Errorf("Failed to create codec for new connection: %v\n", err)
				return
			}

			for {
				parts, ackFn, err := codec.Next(t.ctx)
				if err != nil {
					if err != io.EOF && err != types.ErrTimeout {
						t.log.Errorf("Connection dropped due to: %v\n", err)
					}
					return
				}
				mCount.Incr(1)
				mRcvd.Incr(1)
				mPartsRcvd.Incr(int64(len(parts)))

				// We simply bounce rejected messages in a loop downstream so
				// there's no benefit to aggregating acks.
				_ = ackFn(t.ctx, nil)

				msg := message.New(nil)
				msg.Append(parts...)
				if !t.sendMsg(msg) {
					return
				}
			}
		}(conn)
	}
}

func (t *SocketServer) udpLoop() {
	var (
		mCount     = t.stats.GetCounter("count")
		mRcvd      = t.stats.GetCounter("batch.received")
		mPartsRcvd = t.stats.GetCounter("received")
	)

	defer func() {
		t.retriesMut.Lock()
		// nolint:staticcheck, gocritic // Ignore SA2001 empty critical section, Ignore badLock
		t.retriesMut.Unlock()

		close(t.transactions)
		close(t.closedChan)
	}()

	codec, err := t.codecCtor("", &wrapPacketConn{PacketConn: t.conn}, func(ctx context.Context, err error) error {
		return nil
	})
	if err != nil {
		t.log.Errorf("Connection error due to: %v\n", err)
		return
	}

	go func() {
		<-t.ctx.Done()
		codec.Close(context.Background())
		t.conn.Close()
	}()

	t.log.Infof("Receiving udp socket messages from address: %v\n", t.conn.LocalAddr())

	for {
		parts, ackFn, err := codec.Next(t.ctx)
		if err != nil {
			if err != io.EOF && err != types.ErrTimeout {
				t.log.Errorf("Connection dropped due to: %v\n", err)
			}
			return
		}
		mCount.Incr(1)
		mRcvd.Incr(1)
		mPartsRcvd.Incr(int64(len(parts)))

		// We simply bounce rejected messages in a loop downstream so
		// there's no benefit to aggregating acks.
		_ = ackFn(t.ctx, nil)

		msg := message.New(nil)
		msg.Append(parts...)
		if !t.sendMsg(msg) {
			return
		}
	}
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
	t.closeFn()
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
