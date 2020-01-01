package writer

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	sess "github.com/Jeffail/benthos/v3/lib/util/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
)

//------------------------------------------------------------------------------

// SNSConfig contains configuration fields for the output SNS type.
type SNSConfig struct {
	TopicArn      string `json:"topic_arn" yaml:"topic_arn"`
	sessionConfig `json:",inline" yaml:",inline"`
	Timeout       string `json:"timeout" yaml:"timeout"`
	MaxInFlight   int    `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewSNSConfig creates a new Config with default values.
func NewSNSConfig() SNSConfig {
	return SNSConfig{
		sessionConfig: sessionConfig{
			Config: sess.NewConfig(),
		},
		TopicArn:    "",
		Timeout:     "5s",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// SNS is a benthos writer.Type implementation that writes messages to an
// Amazon SNS queue.
type SNS struct {
	conf SNSConfig

	session *session.Session
	sns     *sns.SNS

	tout time.Duration

	log   log.Modular
	stats metrics.Type
}

// NewSNS creates a new Amazon SNS writer.Type.
func NewSNS(conf SNSConfig, log log.Modular, stats metrics.Type) (*SNS, error) {
	s := &SNS{
		conf:  conf,
		log:   log,
		stats: stats,
	}
	if tout := conf.Timeout; len(tout) > 0 {
		var err error
		if s.tout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout period string: %v", err)
		}
	}
	return s, nil
}

// ConnectWithContext attempts to establish a connection to the target SNS queue.
func (a *SNS) ConnectWithContext(ctx context.Context) error {
	return a.Connect()
}

// Connect attempts to establish a connection to the target SNS queue.
func (a *SNS) Connect() error {
	if a.session != nil {
		return nil
	}

	sess, err := a.conf.GetSession()
	if err != nil {
		return err
	}

	a.session = sess
	a.sns = sns.New(sess)

	a.log.Infof("Sending messages to Amazon SNS ARN: %v\n", a.conf.TopicArn)
	return nil
}

// Write attempts to write message contents to a target SNS.
func (a *SNS) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target SNS.
func (a *SNS) WriteWithContext(wctx context.Context, msg types.Message) error {
	if a.session == nil {
		return types.ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(wctx, a.tout)
	defer cancel()

	return msg.Iter(func(i int, p types.Part) error {
		message := &sns.PublishInput{
			TopicArn: aws.String(a.conf.TopicArn),
			Message:  aws.String(string(p.Get())),
		}
		_, err := a.sns.PublishWithContext(ctx, message)
		return err
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *SNS) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *SNS) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
