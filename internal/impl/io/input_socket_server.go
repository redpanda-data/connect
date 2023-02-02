package io

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/codec"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(func(conf input.Config, nm bundle.NewManagement) (input.Streamed, error) {
		return newSocketServerInput(conf, nm, nm.Logger(), nm.Metrics())
	}), docs.ComponentSpec{
		Name:    "socket_server",
		Summary: `Creates a server that receives a stream of messages over a tcp, udp or unix socket.`,
		Description: `
The field ` + "`max_buffer`" + ` specifies the maximum amount of memory to allocate _per connection_ for buffering lines of data. If a line of data from a connection exceeds this value then the connection will be closed.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("network", "A network type to accept.").HasOptions(
				"unix", "tcp", "udp", "tls",
			),
			docs.FieldString("address", "The address to listen from.", "/tmp/benthos.sock", "0.0.0.0:6000"),
			codec.ReaderDocs.AtVersion("3.42.0"),
			docs.FieldInt("max_buffer", "The maximum message buffer size. Must exceed the largest message to be consumed.").Advanced(),
			docs.FieldObject("tls", "TLS specific configuration, valid when the `network` is set to `tls`.").WithChildren(
				docs.FieldString("cert_file", "PEM encoded certificate for use with TLS.").HasDefault(""),
				docs.FieldString("key_file", "PEM encoded private key for use with TLS.").HasDefault(""),
				docs.FieldBool("self_signed", "Whether to generate self signed certificates.").HasDefault(false),
			),
		).ChildDefaultAndTypesFromStruct(input.NewSocketConfig()),
		Categories: []string{
			"Network",
		},
	})
	if err != nil {
		panic(err)
	}
}

type wrapPacketConn struct {
	net.PacketConn
}

func (w *wrapPacketConn) Read(p []byte) (n int, err error) {
	n, _, err = w.ReadFrom(p)
	return
}

type socketServerInput struct {
	conf  input.SocketServerConfig
	stats metrics.Type
	log   log.Modular

	codecCtor codec.ReaderConstructor
	listener  net.Listener
	conn      net.PacketConn

	retriesMut   sync.RWMutex
	transactions chan message.Transaction

	ctx        context.Context
	closeFn    func()
	closedChan chan struct{}

	mLatency metrics.StatTimer
	mRcvd    metrics.StatCounter
}

func newSocketServerInput(conf input.Config, mgr bundle.NewManagement, log log.Modular, stats metrics.Type) (input.Streamed, error) {
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
	case "tls":
		var cert tls.Certificate
		if cert, err = loadOrCreateCertificate(sconf.TLS); err != nil {
			return nil, err
		}
		config := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		ln, err = tls.Listen("tcp", sconf.Address, config)
	default:
		return nil, fmt.Errorf("socket network '%v' is not supported by this input", sconf.Network)
	}
	if err != nil {
		return nil, err
	}

	t := socketServerInput{
		conf:  conf.SocketServer,
		stats: stats,
		log:   log,

		codecCtor: ctor,
		listener:  ln,
		conn:      cn,

		transactions: make(chan message.Transaction),
		closedChan:   make(chan struct{}),

		mRcvd:    stats.GetCounter("input_received"),
		mLatency: stats.GetTimer("input_latency_ns"),
	}
	t.ctx, t.closeFn = context.WithCancel(context.Background())

	if ln == nil {
		go t.udpLoop()
	} else {
		go t.loop()
	}
	return &t, nil
}

func (t *socketServerInput) Addr() net.Addr {
	if t.listener != nil {
		return t.listener.Addr()
	}
	return t.conn.LocalAddr()
}

func (t *socketServerInput) sendMsg(msg message.Batch) bool {
	tStarted := time.Now()

	// Block whilst retries are happening
	t.retriesMut.Lock()
	// nolint:staticcheck, gocritic // Ignore SA2001 empty critical section, Ignore badLock
	t.retriesMut.Unlock()

	resChan := make(chan error)
	select {
	case t.transactions <- message.NewTransaction(msg, resChan):
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
					sendErr = res
				}
				if sendErr == nil || sendErr == component.ErrTypeClosed {
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
				case t.transactions <- message.NewTransaction(msg, resChan):
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

func (t *socketServerInput) loop() {
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
					if err != io.EOF && err != component.ErrTimeout {
						t.log.Errorf("Connection dropped due to: %v\n", err)
					}
					return
				}
				t.mRcvd.Incr(int64(len(parts)))

				// We simply bounce rejected messages in a loop downstream so
				// there's no benefit to aggregating acks.
				_ = ackFn(t.ctx, nil)

				msg := message.Batch(parts)
				if !t.sendMsg(msg) {
					return
				}
			}
		}(conn)
	}
}

func (t *socketServerInput) udpLoop() {
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
			if err != io.EOF && err != component.ErrTimeout {
				t.log.Errorf("Connection dropped due to: %v\n", err)
			}
			return
		}
		t.mRcvd.Incr(int64(len(parts)))

		// We simply bounce rejected messages in a loop downstream so
		// there's no benefit to aggregating acks.
		_ = ackFn(t.ctx, nil)

		msg := message.Batch(parts)
		if !t.sendMsg(msg) {
			return
		}
	}
}

func (t *socketServerInput) TransactionChan() <-chan message.Transaction {
	return t.transactions
}

func (t *socketServerInput) Connected() bool {
	return true
}

func (t *socketServerInput) TriggerStopConsuming() {
	t.closeFn()
}

func (t *socketServerInput) TriggerCloseNow() {
	t.closeFn()
}

func (t *socketServerInput) WaitForClose(ctx context.Context) error {
	select {
	case <-t.closedChan:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

//------------------------------------------------------------------------------

func createSelfSignedCertificate() (tls.Certificate, error) {
	priv, _ := ecdsa.GenerateKey(elliptic.P521(), rand.Reader)
	certOptions := &x509.Certificate{
		SerialNumber: &big.Int{},
	}

	certBytes, _ := x509.CreateCertificate(rand.Reader, certOptions, certOptions, &priv.PublicKey, priv)
	pemcert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certBytes})
	key, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, err
	}

	keyBytes := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: key})
	cert, err := tls.X509KeyPair(pemcert, keyBytes)
	if err != nil {
		return tls.Certificate{}, err
	}

	return cert, nil
}

func loadOrCreateCertificate(sconf input.SocketServerTLSConfig) (tls.Certificate, error) {
	var cert tls.Certificate
	var err error
	if sconf.CertFile != "" && sconf.KeyFile != "" {
		cert, err = tls.LoadX509KeyPair(sconf.CertFile, sconf.KeyFile)
		if err != nil {
			return tls.Certificate{}, err
		}
		return cert, nil
	}
	if !sconf.SelfSigned {
		return tls.Certificate{}, errors.New("must specify either a certificate file or enable self signed")
	}

	// Either CertFile or KeyFile was not specified, so make our own certificate
	cert, err = createSelfSignedCertificate()
	if err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}
