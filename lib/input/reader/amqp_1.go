package reader

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/Azure/go-amqp"
	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/response"
	"github.com/Jeffail/benthos/v3/lib/util/amqp/sasl"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
)

// AMQP1Config contains configuration for the AMQP1 input type.
type AMQP1Config struct {
	URL            string      `json:"url" yaml:"url"`
	SourceAddress  string      `json:"source_address" yaml:"source_address"`
	AzureRenewLock bool        `json:"azure_renew_lock" yaml:"azure_renew_lock"`
	TLS            btls.Config `json:"tls" yaml:"tls"`
	SASL           sasl.Config `json:"sasl" yaml:"sasl"`
}

// NewAMQP1Config creates a new AMQP1Config with default values.
func NewAMQP1Config() AMQP1Config {
	return AMQP1Config{
		URL:           "",
		SourceAddress: "",
		TLS:           btls.NewConfig(),
		SASL:          sasl.NewConfig(),
	}
}

//------------------------------------------------------------------------------

type amqp1Conn struct {
	client            *amqp.Client
	session           *amqp.Session
	receiver          *amqp.Receiver
	renewLockReceiver *amqp.Receiver
	renewLockSender   *amqp.Sender

	log                    log.Modular
	lockRenewAddressPrefix string
}

func (c *amqp1Conn) Close(ctx context.Context) {
	if c.renewLockSender != nil {
		if err := c.renewLockSender.Close(ctx); err != nil {
			c.log.Errorf("Failed to cleanly close renew lock sender: %v\n", err)
		}
	}
	if c.renewLockReceiver != nil {
		if err := c.renewLockReceiver.Close(ctx); err != nil {
			c.log.Errorf("Failed to cleanly close renew lock receiver: %v\n", err)
		}
	}
	if c.receiver != nil {
		if err := c.receiver.Close(ctx); err != nil {
			c.log.Errorf("Failed to cleanly close receiver: %v\n", err)
		}
	}
	if c.session != nil {
		if err := c.session.Close(ctx); err != nil {
			c.log.Errorf("Failed to cleanly close session: %v\n", err)
		}
	}
	if c.client != nil {
		if err := c.client.Close(); err != nil {
			c.log.Errorf("Failed to cleanly close client: %v\n", err)
		}
	}
}

//------------------------------------------------------------------------------

// AMQP1 is an input type that reads messages via the AMQP 1.0 protocol.
type AMQP1 struct {
	tlsConf *tls.Config

	conf  AMQP1Config
	stats metrics.Type
	log   log.Modular

	m    sync.RWMutex
	conn *amqp1Conn
}

// NewAMQP1 creates a new AMQP1 input type.
func NewAMQP1(conf AMQP1Config, log log.Modular, stats metrics.Type) (*AMQP1, error) {
	a := AMQP1{
		conf:  conf,
		stats: stats,
		log:   log,
	}
	if conf.TLS.Enabled {
		var err error
		if a.tlsConf, err = conf.TLS.Get(); err != nil {
			return nil, err
		}
	}
	return &a, nil
}

//------------------------------------------------------------------------------

// ConnectWithContext establishes a connection to an AMQP1 server.
func (a *AMQP1) ConnectWithContext(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.conn != nil {
		return nil
	}

	conn := &amqp1Conn{
		log:                    a.log,
		lockRenewAddressPrefix: randomString(15),
	}

	opts, err := a.conf.SASL.ToOptFns()
	if err != nil {
		return err
	}
	if a.conf.TLS.Enabled {
		opts = append(opts, amqp.ConnTLS(true), amqp.ConnTLSConfig(a.tlsConf))
	}

	// Create client
	if conn.client, err = amqp.Dial(a.conf.URL, opts...); err != nil {
		return err
	}

	// Open a session
	if conn.session, err = conn.client.NewSession(); err != nil {
		conn.Close(ctx)
		return err
	}

	// Create a receiver
	if conn.receiver, err = conn.session.NewReceiver(
		amqp.LinkSourceAddress(a.conf.SourceAddress),
		amqp.LinkCredit(10),
	); err != nil {
		conn.Close(ctx)
		return err
	}

	if a.conf.AzureRenewLock {
		managementAddress := a.conf.SourceAddress + "/$management"

		conn.renewLockSender, err = conn.session.NewSender(
			amqp.LinkSourceAddress(conn.lockRenewAddressPrefix+lockRenewRequestSuffix),
			amqp.LinkTargetAddress(managementAddress),
		)
		if err != nil {
			conn.Close(ctx)
			return err
		}
		conn.renewLockReceiver, err = conn.session.NewReceiver(
			amqp.LinkSourceAddress(managementAddress),
			amqp.LinkTargetAddress(conn.lockRenewAddressPrefix+lockRenewResponseSuffix),
		)
		if err != nil {
			conn.Close(ctx)
			return err
		}
	}

	a.conn = conn
	a.log.Infof("Receiving AMQP 1.0 messages from source: %v\n", a.conf.SourceAddress)
	return nil
}

// disconnect safely closes a connection to an AMQP1 server.
func (a *AMQP1) disconnect(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.conn != nil {
		a.conn.Close(ctx)
	}
	a.conn = nil
	return nil
}

//------------------------------------------------------------------------------

// ReadWithContext a new AMQP1 message.
func (a *AMQP1) ReadWithContext(ctx context.Context) (*message.Batch, AsyncAckFn, error) {
	a.m.RLock()
	conn := a.conn
	a.m.RUnlock()

	if conn == nil {
		return nil, nil, component.ErrNotConnected
	}

	// Receive next message
	amqpMsg, err := conn.receiver.Receive(ctx)
	if err != nil {
		if err == amqp.ErrTimeout {
			err = component.ErrTimeout
		} else {
			if dErr, isDetachError := err.(*amqp.DetachError); isDetachError && dErr.RemoteError != nil {
				a.log.Errorf("Lost connection due to: %v\n", dErr.RemoteError)
			} else {
				a.log.Errorf("Lost connection due to: %v\n", err)
			}
			a.disconnect(ctx)
			err = component.ErrNotConnected
		}
		return nil, nil, err
	}

	msg := message.QuickBatch(nil)

	part := message.NewPart(amqpMsg.GetData())

	if amqpMsg.Properties != nil {
		amqpSetMetadata(part, "amqp_content_type", amqpMsg.Properties.ContentType)
		amqpSetMetadata(part, "amqp_content_encoding", amqpMsg.Properties.ContentEncoding)
		amqpSetMetadata(part, "amqp_creation_time", amqpMsg.Properties.CreationTime)
	}
	if amqpMsg.Annotations != nil {
		for k, v := range amqpMsg.Annotations {
			keyStr, keyIsStr := k.(string)
			valStr, valIsStr := v.(string)
			if keyIsStr && valIsStr {
				amqpSetMetadata(part, keyStr, valStr)
			}
		}
	}

	msg.Append(part)

	var done chan struct{}
	if a.conf.AzureRenewLock {
		done = a.startRenewJob(amqpMsg)
	}

	return msg, func(ctx context.Context, res response.Error) error {
		if done != nil {
			close(done)
			done = nil
		}

		// TODO: These methods were moved in v0.16.0, but nacking seems broken
		// (integration tests fail)
		if res.AckError() != nil {
			return conn.receiver.ModifyMessage(ctx, amqpMsg, true, false, amqpMsg.Annotations)
		}
		return conn.receiver.AcceptMessage(ctx, amqpMsg)
	}, nil
}

// CloseAsync shuts down the AMQP1 input and stops processing requests.
func (a *AMQP1) CloseAsync() {
	a.disconnect(context.Background())
}

// WaitForClose blocks until the AMQP1 input has closed down.
func (a *AMQP1) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------

const (
	lockRenewResponseSuffix = "-response"
	lockRenewRequestSuffix  = "-request"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[seededRand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (a *AMQP1) startRenewJob(amqpMsg *amqp.Message) chan struct{} {
	done := make(chan struct{})
	go func() {
		ctx := context.Background()

		lockedUntil, ok := amqpMsg.Annotations["x-opt-locked-until"].(time.Time)
		if !ok {
			a.log.Errorln("Missing x-opt-locked-until annotation in received message")
			return
		}

		for {
			select {
			case <-done:
				return
			case <-time.After(time.Until(lockedUntil) / 10 * 9):
				var err error
				lockedUntil, err = a.renewWithContext(ctx, amqpMsg)
				if err != nil {
					a.log.Errorf("Unable to renew lock err: %v", err)
					return
				}

				a.log.Tracef("Renewed lock until %v", lockedUntil)
			}
		}
	}()
	return done
}

func uuidFromLockTokenBytes(bytes []byte) (*amqp.UUID, error) {
	if len(bytes) != 16 {
		return nil, fmt.Errorf("invalid lock token, token was not 16 bytes long")
	}

	var swapIndex = func(indexOne, indexTwo int, array *[16]byte) {
		array[indexOne], array[indexTwo] = array[indexTwo], array[indexOne]
	}

	// Get lock token from the deliveryTag
	var lockTokenBytes [16]byte
	copy(lockTokenBytes[:], bytes[:16])
	// translate from .net guid byte serialisation format to amqp rfc standard
	swapIndex(0, 3, &lockTokenBytes)
	swapIndex(1, 2, &lockTokenBytes)
	swapIndex(4, 5, &lockTokenBytes)
	swapIndex(6, 7, &lockTokenBytes)
	amqpUUID := amqp.UUID(lockTokenBytes)

	return &amqpUUID, nil
}

func (a *AMQP1) renewWithContext(ctx context.Context, msg *amqp.Message) (time.Time, error) {
	a.m.RLock()
	conn := a.conn
	a.m.RUnlock()

	if conn == nil {
		return time.Time{}, component.ErrNotConnected
	}

	lockToken, err := uuidFromLockTokenBytes(msg.DeliveryTag)
	if err != nil {
		return time.Time{}, err
	}

	replyTo := conn.lockRenewAddressPrefix + lockRenewResponseSuffix
	renewMsg := &amqp.Message{
		Properties: &amqp.MessageProperties{
			MessageID: msg.Properties.MessageID,
			ReplyTo:   &replyTo,
		},
		ApplicationProperties: map[string]interface{}{
			"operation": "com.microsoft:renew-lock",
		},
		Value: map[string]interface{}{
			"lock-tokens": []amqp.UUID{*lockToken},
		},
	}

	err = conn.renewLockSender.Send(ctx, renewMsg)
	if err != nil {
		return time.Time{}, err
	}

	result, err := conn.renewLockReceiver.Receive(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if statusCode, ok := result.ApplicationProperties["statusCode"].(int32); !ok || statusCode != 200 {
		return time.Time{}, fmt.Errorf("unsuccessful status code %d, message %s", statusCode, result.ApplicationProperties["statusDescription"])
	}

	values, ok := result.Value.(map[string]interface{})
	if !ok {
		return time.Time{}, errors.New("missing value in response message")
	}

	expirations, ok := values["expirations"].([]time.Time)
	if !ok || len(expirations) != 1 {
		return time.Time{}, errors.New("missing expirations filed in response message values")
	}

	return expirations[0], nil
}
