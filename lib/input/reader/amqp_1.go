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
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/amqp/sasl"
	btls "github.com/Jeffail/benthos/v3/lib/util/tls"
)

const (
	lockRenewResponseSuffix = "-response"
	lockRenewRequestSuffix  = "-request"
)

//------------------------------------------------------------------------------

// AMQP1Config contains configuration for the AMQP1 input type.
type AMQP1Config struct {
	URL           string      `json:"url" yaml:"url"`
	SourceAddress string      `json:"source_address" yaml:"source_address"`
	RenewLock     bool        `json:"renew_lock" yaml:"renew_lock"`
	TLS           btls.Config `json:"tls" yaml:"tls"`
	SASL          sasl.Config `json:"sasl" yaml:"sasl"`
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

// AMQP1 is an input type that reads messages via the AMQP 1.0 protocol.
type AMQP1 struct {
	client            *amqp.Client
	session           *amqp.Session
	receiver          *amqp.Receiver
	renewLockReceiver *amqp.Receiver
	renewLockSender   *amqp.Sender

	tlsConf *tls.Config

	conf  AMQP1Config
	stats metrics.Type
	log   log.Modular

	lockRenewAddressPrefix string

	m sync.RWMutex
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

	if a.client != nil {
		return nil
	}

	var (
		client   *amqp.Client
		session  *amqp.Session
		receiver *amqp.Receiver
		err      error
	)

	opts, err := a.conf.SASL.ToOptFns()
	if err != nil {
		return err
	}
	if a.conf.TLS.Enabled {
		opts = append(opts, amqp.ConnTLS(true))
		opts = append(opts, amqp.ConnTLSConfig(a.tlsConf))
	}

	// Create client
	if client, err = amqp.Dial(a.conf.URL, opts...); err != nil {
		return err
	}

	// Open a session
	if session, err = client.NewSession(); err != nil {
		client.Close()
		return err
	}

	// Create a receiver
	if receiver, err = session.NewReceiver(
		amqp.LinkSourceAddress(a.conf.SourceAddress),
		amqp.LinkCredit(10),
	); err != nil {
		session.Close(context.Background())
		client.Close()
		return err
	}

	a.client = client
	a.session = session
	a.receiver = receiver

	if a.conf.RenewLock {
		a.lockRenewAddressPrefix = randomString(15)
		managementAddress := a.conf.SourceAddress + "/$management"

		a.renewLockSender, err = a.session.NewSender(amqp.LinkSourceAddress(a.lockRenewAddressPrefix+lockRenewRequestSuffix), amqp.LinkTargetAddress(managementAddress))
		if err != nil {
			receiver.Close(context.Background())
			session.Close(context.Background())
			client.Close()
			return err
		}
		a.renewLockReceiver, err = a.session.NewReceiver(amqp.LinkSourceAddress(managementAddress), amqp.LinkTargetAddress(a.lockRenewAddressPrefix+lockRenewResponseSuffix))
		if err != nil {
			a.renewLockSender.Close(context.Background())
			receiver.Close(context.Background())
			session.Close(context.Background())
			client.Close()
			return err
		}
	}
	a.log.Infof("Receiving AMQP 1.0 messages from source: %v\n", a.conf.SourceAddress)
	return nil
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[seededRand.Intn(len(letterBytes))]
	}
	return string(b)
}

// disconnect safely closes a connection to an AMQP1 server.
func (a *AMQP1) disconnect(ctx context.Context) error {
	a.m.Lock()
	defer a.m.Unlock()

	if a.client == nil {
		return nil
	}

	if a.conf.RenewLock {
		if err := a.renewLockReceiver.Close(ctx); err != nil {
			a.log.Errorf("Failed to cleanly close renew lock receiver: %v\n", err)
		}
		if err := a.renewLockSender.Close(ctx); err != nil {
			a.log.Errorf("Failed to cleanly close renew lock sender: %v\n", err)
		}
	}

	if err := a.receiver.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close receiver: %v\n", err)
	}
	if err := a.session.Close(ctx); err != nil {
		a.log.Errorf("Failed to cleanly close session: %v\n", err)
	}
	if err := a.client.Close(); err != nil {
		a.log.Errorf("Failed to cleanly close client: %v\n", err)
	}
	a.client = nil
	a.session = nil
	a.receiver = nil

	return nil
}

//------------------------------------------------------------------------------
// ReadWithContext a new AMQP1 message.
func (a *AMQP1) ReadWithContext(ctx context.Context) (types.Message, AsyncAckFn, error) {
	var r *amqp.Receiver
	a.m.RLock()
	if a.receiver != nil {
		r = a.receiver
	}
	a.m.RUnlock()

	if r == nil {
		return nil, nil, types.ErrNotConnected
	}

	// Receive next message
	amqpMsg, err := r.Receive(ctx)
	if err != nil {
		if err == amqp.ErrTimeout {
			err = types.ErrTimeout
		} else {
			if dErr, isDetachError := err.(*amqp.DetachError); isDetachError && dErr.RemoteError != nil {
				a.log.Errorf("Lost connection due to: %v\n", dErr.RemoteError)
			} else {
				a.log.Errorf("Lost connection due to: %v\n", err)
			}
			a.disconnect(ctx)
			err = types.ErrNotConnected
		}
		return nil, nil, err
	}

	msg := message.New(nil)

	part := message.NewPart(amqpMsg.GetData())

	if amqpMsg.Properties != nil {
		setMetadata(part, "amqp_content_type", amqpMsg.Properties.ContentType)
		setMetadata(part, "amqp_content_encoding", amqpMsg.Properties.ContentEncoding)
		setMetadata(part, "amqp_creation_time", amqpMsg.Properties.CreationTime)
	}

	msg.Append(part)

	var done chan struct{}
	if a.conf.RenewLock {
		done = a.startRenewJob(amqpMsg)
	}

	return msg, func(ctx context.Context, res types.Response) error {
		if done != nil {
			close(done)
		}

		if res.Error() != nil {
			return amqpMsg.Modify(ctx, true, false, amqpMsg.Annotations)
		}
		return amqpMsg.Accept(ctx)
	}, nil
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
		v1 := array[indexOne]
		array[indexOne] = array[indexTwo]
		array[indexTwo] = v1
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
	var r *amqp.Receiver
	var s *amqp.Sender
	if a.renewLockReceiver != nil {
		r = a.renewLockReceiver
	}
	if a.renewLockSender != nil {
		s = a.renewLockSender
	}
	a.m.RUnlock()

	if r == nil || s == nil {
		return time.Time{}, types.ErrNotConnected
	}

	lockToken, err := uuidFromLockTokenBytes(msg.DeliveryTag)
	if err != nil {
		return time.Time{}, err
	}

	renewMsg := &amqp.Message{
		Properties: &amqp.MessageProperties{
			MessageID: msg.Properties.MessageID,
			ReplyTo:   a.lockRenewAddressPrefix + lockRenewResponseSuffix,
		},
		ApplicationProperties: map[string]interface{}{
			"operation": "com.microsoft:renew-lock",
		},
		Value: map[string]interface{}{
			"lock-tokens": []amqp.UUID{*lockToken},
		},
	}

	err = s.Send(ctx, renewMsg)
	if err != nil {
		return time.Time{}, err
	}

	result, err := r.Receive(ctx)
	if err != nil {
		return time.Time{}, err
	}
	if statusCode, ok := result.ApplicationProperties["statusCode"].(int32); !ok || statusCode != 200 {
		return time.Time{}, fmt.Errorf("unsecessful status code %d, message %s", statusCode, result.ApplicationProperties["statusDescription"])
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

// CloseAsync shuts down the AMQP1 input and stops processing requests.
func (a *AMQP1) CloseAsync() {
	a.disconnect(context.Background())
}

// WaitForClose blocks until the AMQP1 input has closed down.
func (a *AMQP1) WaitForClose(timeout time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
