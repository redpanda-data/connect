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

	"github.com/benthosdev/benthos/v4/internal/codec/interop"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/scanner"
	"github.com/benthosdev/benthos/v4/internal/shutdown"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	issFieldNetwork       = "network"
	issFieldAddress       = "address"
	issFieldAddressCache  = "address_cache"
	issFieldTLS           = "tls"
	issFieldTLSCertFile   = "cert_file"
	issFieldTLSKeyFile    = "key_file"
	issFieldTLSSelfSigned = "self_signed"
)

func socketServerInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Summary(`Creates a server that receives a stream of messages over a tcp, udp or unix socket.`).
		Categories("Network").
		Fields(
			service.NewStringEnumField(issFieldNetwork, "unix", "tcp", "udp", "tls").
				Description("A network type to accept."),
			service.NewStringField(isFieldAddress).
				Description("The address to listen from.").
				Examples("/tmp/benthos.sock", "0.0.0.0:6000"),
			service.NewStringField(issFieldAddressCache).
				Description("An optional [`cache`](/docs/components/caches/about) within which this input should write it's bound address once known. The key of the cache item containing the address will be the label of the component suffixed with `_address` (e.g. `foo_address`), or `socket_server_address` when a label has not been provided. This is useful in situations where the address is dynamically allocated by the server (`127.0.0.1:0`) and you want to store the allocated address somewhere for reference by other systems and components.").
				Optional().
				Version("4.25.0"),
			service.NewObjectField(issFieldTLS,
				service.NewStringField(issFieldTLSCertFile).
					Description("PEM encoded certificate for use with TLS.").
					Optional(),
				service.NewStringField(issFieldTLSKeyFile).
					Description("PEM encoded private key for use with TLS.").
					Optional(),
				service.NewBoolField(issFieldTLSSelfSigned).
					Description("Whether to generate self signed certificates.").
					Default(false),
			).
				Description("TLS specific configuration, valid when the `network` is set to `tls`.").
				Optional(),
			service.NewAutoRetryNacksToggleField(),
		).
		Fields(interop.OldReaderCodecFields("lines")...)
}

func init() {
	err := service.RegisterBatchInput("socket_server", socketServerInputSpec(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		i, err := newSocketServerInputFromParsed(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatchedToggled(conf, i)
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
	log *service.Logger
	mgr *service.Resources

	network       string
	address       string
	addressCache  string
	tlsCert       string
	tlsKey        string
	tlsSelfSigned bool
	codecCtor     interop.FallbackReaderCodec

	messages chan service.MessageBatch
	shutSig  *shutdown.Signaller
}

func newSocketServerInputFromParsed(conf *service.ParsedConfig, mgr *service.Resources) (i *socketServerInput, err error) {
	t := socketServerInput{
		log:      mgr.Logger(),
		mgr:      mgr,
		shutSig:  shutdown.NewSignaller(),
		messages: make(chan service.MessageBatch),
	}

	if t.network, err = conf.FieldString(issFieldNetwork); err != nil {
		return
	}
	if t.address, err = conf.FieldString(issFieldAddress); err != nil {
		return
	}
	t.addressCache, _ = conf.FieldString(issFieldAddressCache)

	tlsConf := conf.Namespace(issFieldTLS)
	t.tlsCert, _ = tlsConf.FieldString(issFieldTLSCertFile)
	t.tlsKey, _ = tlsConf.FieldString(issFieldTLSKeyFile)
	t.tlsSelfSigned, _ = tlsConf.FieldBool(issFieldTLSSelfSigned)

	if t.codecCtor, err = interop.OldReaderCodecFromParsed(conf); err != nil {
		return
	}
	return &t, nil
}

func (t *socketServerInput) Connect(ctx context.Context) error {
	var ln net.Listener
	var cn net.PacketConn

	var err error
	switch t.network {
	case "tcp", "unix":
		ln, err = net.Listen(t.network, t.address)
	case "tls":
		var cert tls.Certificate
		if cert, err = loadOrCreateCertificate(t.tlsCert, t.tlsKey, t.tlsSelfSigned); err != nil {
			return err
		}
		config := &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		ln, err = tls.Listen("tcp", t.address, config)
	case "udp":
		cn, err = net.ListenPacket(t.network, t.address)
	default:
		return fmt.Errorf("socket network '%v' is not supported by this input", t.network)
	}
	if err != nil {
		return err
	}

	if ln == nil {
		go t.udpLoop(cn)
	} else {
		go t.loop(ln)
	}

	var addr net.Addr
	if ln != nil {
		addr = ln.Addr()
		t.log.Infof("Receiving %v socket messages from address: %v", t.network, addr.String())
	} else {
		addr = cn.LocalAddr()
		t.log.Infof("Receiving udp socket messages from address: %v", addr.String())
	}
	if t.addressCache != "" {
		key := "socket_server_address"
		if l := t.mgr.Label(); l != "" {
			key = l + "_address"
		}
		_ = t.mgr.AccessCache(ctx, t.addressCache, func(c service.Cache) {
			if err := c.Set(ctx, key, []byte(addr.String()), nil); err != nil {
				t.log.Errorf("Failed to set address in cache: %v", err)
			}
		})
	}
	return nil
}

func (t *socketServerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	select {
	case b, open := <-t.messages:
		if open {
			return b, func(ctx context.Context, err error) error {
				return nil
			}, nil
		}
		return nil, nil, service.ErrEndOfInput
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

func (t *socketServerInput) loop(listener net.Listener) {
	var wg sync.WaitGroup

	defer func() {
		wg.Wait()
		_ = listener.Close()
		close(t.messages)
		t.shutSig.ShutdownComplete()
	}()

	go func() {
		<-t.shutSig.CloseAtLeisureChan()
		_ = listener.Close()
	}()

	closeCtx, done := t.shutSig.CloseAtLeisureCtx(context.Background())
	defer done()

acceptLoop:
	for {
		conn, err := listener.Accept()
		if err != nil {
			if !strings.Contains(err.Error(), "use of closed network connection") {
				t.log.Errorf("Failed to accept Socket connection: %v", err)
			}
			select {
			case <-time.After(time.Second):
				continue acceptLoop
			case <-t.shutSig.CloseAtLeisureChan():
				return
			}
		}

		go func() {
			<-t.shutSig.CloseAtLeisureChan()
			_ = conn.Close()
		}()

		wg.Add(1)
		go func(c net.Conn) {
			defer func() {
				_ = c.Close()
				wg.Done()
			}()

			codec, err := t.codecCtor.Create(c, func(ctx context.Context, err error) error {
				return nil
			}, scanner.SourceDetails{})
			if err != nil {
				t.log.Errorf("Failed to create codec for new connection: %v", err)
				return
			}

			for {
				parts, ackFn, err := codec.NextBatch(closeCtx)
				if err != nil {
					if !errors.Is(err, io.EOF) {
						t.log.Errorf("Connection dropped due to: %v\n", err)
					}
					return
				}

				// We simply bounce rejected messages in a loop downstream so
				// there's no benefit to aggregating acks.
				_ = ackFn(closeCtx, nil)

				select {
				case t.messages <- parts:
				case <-t.shutSig.CloseAtLeisureChan():
					return
				}
			}
		}(conn)
	}
}

func (t *socketServerInput) udpLoop(conn net.PacketConn) {
	defer func() {
		_ = conn.Close()
		close(t.messages)
		t.shutSig.ShutdownComplete()
	}()

	go func() {
		<-t.shutSig.CloseAtLeisureChan()
		_ = conn.Close()
	}()

	closeCtx, done := t.shutSig.CloseAtLeisureCtx(context.Background())
	defer done()

	codec, err := t.codecCtor.Create(&wrapPacketConn{PacketConn: conn}, func(ctx context.Context, err error) error {
		return nil
	}, scanner.SourceDetails{})
	if err != nil {
		t.log.Errorf("Connection error due to: %v", err)
		return
	}

	for {
		parts, ackFn, err := codec.NextBatch(closeCtx)
		if err != nil {
			if err != io.EOF && err != component.ErrTimeout {
				t.log.Errorf("Connection dropped due to: %v", err)
			}
			return
		}

		// We simply bounce rejected messages in a loop downstream so
		// there's no benefit to aggregating acks.
		_ = ackFn(closeCtx, nil)

		select {
		case t.messages <- parts:
		case <-t.shutSig.CloseAtLeisureChan():
			return
		}
	}
}

func (t *socketServerInput) Close(ctx context.Context) error {
	t.shutSig.CloseAtLeisure()
	select {
	case <-t.shutSig.HasClosedChan():
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

func loadOrCreateCertificate(certFile, keyFile string, selfSigned bool) (tls.Certificate, error) {
	var cert tls.Certificate
	var err error
	if certFile != "" && keyFile != "" {
		cert, err = tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return tls.Certificate{}, err
		}
		return cert, nil
	}
	if !selfSigned {
		return tls.Certificate{}, errors.New("must specify either a certificate file or enable self signed")
	}

	// Either CertFile or KeyFile was not specified, so make our own certificate
	cert, err = createSelfSignedCertificate()
	if err != nil {
		return tls.Certificate{}, err
	}
	return cert, nil
}
