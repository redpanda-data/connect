package nats

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/impl/nats/auth"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	// Stream Output Fields
	soFieldURLs      = "urls"
	soFieldClusterID = "cluster_id"
	soFieldSubject   = "subject"
	soFieldClientID  = "client_id"
	soFieldTLS       = "tls"
	soFieldAuth      = "auth"
)

type soConfig struct {
	URLs       []string
	ClusterID  string
	ClientID   string
	Subject    string
	TLS        *tls.Config
	TLSEnabled bool
	Auth       auth.Config
}

func soConfigFromParsed(pConf *service.ParsedConfig) (conf soConfig, err error) {
	if conf.URLs, err = pConf.FieldStringList(soFieldURLs); err != nil {
		return
	}
	if conf.ClusterID, err = pConf.FieldString(soFieldClusterID); err != nil {
		return
	}
	if conf.ClientID, err = pConf.FieldString(soFieldClientID); err != nil {
		return
	}
	if conf.Subject, err = pConf.FieldString(soFieldSubject); err != nil {
		return
	}
	if conf.TLS, conf.TLSEnabled, err = pConf.FieldTLSToggled(soFieldTLS); err != nil {
		return
	}
	if conf.Auth, err = AuthFromParsedConfig(pConf.Namespace(soFieldAuth)); err != nil {
		return
	}
	return
}

func soSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary(`Publish to a NATS Stream subject.`).
		Description(`
:::caution Deprecation Notice
The NATS Streaming Server is being deprecated. Critical bug fixes and security fixes will be applied until June of 2023. NATS-enabled applications requiring persistence should use [JetStream](https://docs.nats.io/nats-concepts/jetstream).
:::

`+output.Description(true, false, auth.Description())).
		Fields(
			service.NewStringListField(soFieldURLs).
				Description("A list of URLs to connect to. If an item of the list contains commas it will be expanded into multiple URLs.").
				Example([]string{"nats://127.0.0.1:4222"}).
				Example([]string{"nats://username:password@127.0.0.1:4222"}),
			service.NewStringField(soFieldClusterID).
				Description("The cluster ID to publish to."),
			service.NewStringField(soFieldSubject).
				Description("The subject to publish to."),
			service.NewStringField(soFieldClientID).
				Description("The client ID to connect with.").
				Default(""),
			service.NewOutputMaxInFlightField().
				Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
			service.NewTLSToggledField(soFieldTLS),
			service.NewInternalField(auth.FieldSpec()),
		)
}

func init() {
	err := service.RegisterOutput(
		"nats_stream", soSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			pConf, err := soConfigFromParsed(conf)
			if err != nil {
				return nil, 0, err
			}
			maxInFlight, err := conf.FieldMaxInFlight()
			if err != nil {
				return nil, 0, err
			}
			w, err := newNATSStreamWriter(pConf, mgr)
			return w, maxInFlight, err
		})
	if err != nil {
		panic(err)
	}
}

type natsStreamWriter struct {
	log *service.Logger
	fs  *service.FS

	stanConn stan.Conn
	natsConn *nats.Conn
	connMut  sync.RWMutex

	urls string
	conf soConfig
}

func newNATSStreamWriter(conf soConfig, mgr *service.Resources) (*natsStreamWriter, error) {
	if conf.ClientID == "" {
		rgen := rand.New(rand.NewSource(time.Now().UnixNano()))

		// Generate random client id if one wasn't supplied.
		b := make([]byte, 16)
		rgen.Read(b)
		conf.ClientID = fmt.Sprintf("client-%x", b)
	}

	n := natsStreamWriter{
		log:  mgr.Logger(),
		fs:   service.NewFS(mgr.FS()),
		conf: conf,
	}
	n.urls = strings.Join(conf.URLs, ",")
	return &n, nil
}

func (n *natsStreamWriter) Connect(ctx context.Context) error {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		return nil
	}

	var opts []nats.Option
	if n.conf.TLSEnabled && n.conf.TLS != nil {
		opts = append(opts, nats.Secure(n.conf.TLS))
	}

	opts = append(opts, authConfToOptions(n.conf.Auth, n.fs)...)
	opts = append(opts, errorHandlerOption(n.log))

	natsConn, err := nats.Connect(n.urls, opts...)
	if err != nil {
		return err
	}

	stanConn, err := stan.Connect(
		n.conf.ClusterID,
		n.conf.ClientID,
		stan.NatsConn(natsConn),
	)
	if err != nil {
		natsConn.Close()
		return err
	}

	n.stanConn = stanConn
	n.natsConn = natsConn
	n.log.Infof("Sending NATS messages to subject: %v", n.conf.Subject)
	return nil
}

func (n *natsStreamWriter) Write(ctx context.Context, msg *service.Message) error {
	n.connMut.RLock()
	conn := n.stanConn
	n.connMut.RUnlock()

	if conn == nil {
		return service.ErrNotConnected
	}

	mBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}

	err = conn.Publish(n.conf.Subject, mBytes)
	if errors.Is(err, stan.ErrConnectionClosed) {
		conn.Close()
		n.connMut.Lock()
		n.stanConn = nil
		n.natsConn.Close()
		n.natsConn = nil
		n.connMut.Unlock()
		return service.ErrNotConnected
	}
	return err
}

func (n *natsStreamWriter) Close(context.Context) (err error) {
	n.connMut.Lock()
	defer n.connMut.Unlock()

	if n.natsConn != nil {
		n.natsConn.Close()
		n.natsConn = nil
	}
	if n.stanConn != nil {
		err = n.stanConn.Close()
		n.stanConn = nil
	}
	return
}
