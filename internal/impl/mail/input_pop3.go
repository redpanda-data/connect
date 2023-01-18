package mail

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/knadh/go-pop3"
)

const (
	hostParam              = "host"
	portParam              = "port"
	usernameParam          = "username"
	passwordParam          = "password"
	tlsParam               = "tls"
	tlsEnabledParam        = "enabled"
	tlsSkipCertVerifyParam = "skip_cert_verify"
	pullParam              = "pull"
	countParam             = "count"
	intervalParam          = "interval"
	maxInFlightParam       = "max_in_flight"
	timeoutParam           = "timeout"
	maxBodySizeParam       = "max_body_size"
	listPageSizeParam      = "list_page_size"
)

func pop3ConfigSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.12.0").
		Summary("Connects to a POP3 server and periodically reads the emails.").
		Description(`
Reads emails from a POP3 server. The emails are permanently deleted when delivered.

### Metadata

This input adds the following metadata fields to each message if present:

` + "```" + `
- Subject
- From
- To
- Cc
- Bcc
- Date
- Received
- Content-Type
- Return-Path
- Delivered-To
` + "```" + `
You can access these metadata fields using [function interpolation](/docs/configuration/interpolation#bloblang-queries).
`).
		Field(service.NewStringField(hostParam).
			Description("The host of the POP3 server").
			Example("pop.gmail.com")).
		Field(service.NewIntField(portParam).
			Description("The port of the POP3 server.").
			Default(995)).
		Field(service.NewStringField(usernameParam).
			Description("The username to login on the POP3 server.")).
		Field(service.NewStringField(passwordParam).
			Description("The password to login on the POP3 server.")).
		Field(service.NewObjectField(tlsParam,
			service.NewBoolField(tlsEnabledParam).
				Description(`Whether TLS is enabled.`),
			service.NewBoolField(tlsSkipCertVerifyParam).
				Description(`Whether to skip server side certificate verification.`)).Advanced()).
		Field(service.NewObjectField(pullParam,
			service.NewIntField(countParam).
				Description(`An optional number of pulls to fetch messages, if set above 0 the specified number of pulls is executed and then the input will shut down.`).
				Default(0),
			service.NewDurationField(intervalParam).
				Description(`The time interval at which message pulls should be performed, expressed as a duration string.`).
				Default("5m")).Advanced()).
		Field(service.NewDurationField(timeoutParam).
			Description("The timeout to establish the connection with the POP3 server.").
			Default("30s").Advanced()).
		Field(service.NewIntField(maxInFlightParam).
			Description("The maximum number of messages to have in flight at a given time. Increase this to improve throughput.").
			Default(1).Advanced()).
		Field(service.NewIntField(maxBodySizeParam).
			Description("The max number of bytes when reading the message body.").
			Default(4096).Advanced()).
		Field(service.NewIntField(listPageSizeParam).
			Description("The max number of emails returned by the POP3 server. This limit may be configured on the server or may be imposed by the server software.").
			Default(100).Advanced())
}

func init() {
	err := service.RegisterInput("pop3", pop3ConfigSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newPop3(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type pop struct {
	client       *pop3.Client
	username     string
	password     string
	count        int
	interval     time.Duration
	maxBodySize  int
	listPageSize int
	messageChan  chan *envelope
	done         chan struct{}
	ack          chan int
	log          *service.Logger
	onlyOnce     sync.Once
}

type envelope struct {
	message *service.Message
	ID      int
}

// newPop creates a new pop3 input type.
func newPop3(conf *service.ParsedConfig, log *service.Logger) (*pop, error) {
	const errorPattern = "error retrieving %s: %v"
	host, err := conf.FieldString(hostParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, hostParam, err)
	}
	port, err := conf.FieldInt(portParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, portParam, err)
	}
	username, err := conf.FieldString(usernameParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, usernameParam, err)
	}
	password, err := conf.FieldString(passwordParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, passwordParam, err)
	}
	tlsEnabled, err := conf.Namespace(tlsParam).FieldBool(tlsEnabledParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, tlsEnabledParam, err)
	}
	tlsSkipCertVerify, err := conf.Namespace(tlsParam).FieldBool(tlsSkipCertVerifyParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, tlsSkipCertVerifyParam, err)
	}
	count, err := conf.Namespace(pullParam).FieldInt(countParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, countParam, err)
	}
	interval, err := conf.Namespace(pullParam).FieldDuration(intervalParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, intervalParam, err)
	}
	maxInFlight, err := conf.FieldInt(maxInFlightParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, maxInFlightParam, err)
	}
	timeout, err := conf.FieldDuration(timeoutParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, timeoutParam, err)
	}
	maxBodySize, err := conf.FieldInt(maxBodySizeParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, maxBodySizeParam, err)
	}
	listPageSize, err := conf.FieldInt(listPageSizeParam)
	if err != nil {
		return nil, fmt.Errorf(errorPattern, listPageSizeParam, err)
	}

	client := pop3.New(pop3.Opt{
		Host:          host,
		Port:          port,
		TLSEnabled:    tlsEnabled,
		TLSSkipVerify: tlsSkipCertVerify,
		DialTimeout:   timeout,
	})

	p := &pop{
		client:       client,
		username:     username,
		password:     password,
		maxBodySize:  maxBodySize,
		listPageSize: listPageSize,
		count:        count,
		interval:     interval,
		messageChan:  make(chan *envelope, maxInFlight),
		done:         make(chan struct{}),
		ack:          make(chan int, maxInFlight),
		log:          log,
	}

	return p, nil
}

// Connect attempts to establish a connection and fetch the emails periodically
func (p *pop) Connect(ctx context.Context) error {
	p.onlyOnce.Do(func() {
		go func() {
			defer func() {
				p.done <- struct{}{}
			}()
			ticker := time.NewTicker(p.interval)
			times := 0
			for ; ; <-ticker.C {
				for hasMessages := true; hasMessages; {
					n, err := p.fetchEmails()
					if err != nil {
						p.log.Errorf("error fetching emails: %v", err)
					}
					hasMessages = n > 0 && n >= p.listPageSize
				}
				times++
				if p.count > 0 && times >= p.count {
					ticker.Stop()
					break
				}
			}
		}()
	})
	return nil
}

func (p *pop) fetchEmails() (int, error) {
	c, err := p.client.NewConn()
	if err != nil {
		return 0, fmt.Errorf("error establishing the connection: %v", err)
	}
	defer func() {
		if err := c.Quit(); err != nil {
			p.log.Errorf("error quiting: %v", err)
		}
	}()
	if err := c.Auth(p.username, p.password); err != nil {
		return 0, fmt.Errorf("error while logging in: %v", err)
	}
	msgs, err := c.List(0)
	if err != nil {
		return 0, fmt.Errorf("error listing emails: %v", err)
	}
	var wg sync.WaitGroup
	for _, m := range msgs {
		entity, err := c.Retr(m.ID)
		if err != nil {
			p.log.Errorf("error retrieving email %d: %v", m.ID, err)
		}
		body := make([]byte, p.maxBodySize)
		n, err := entity.Body.Read(body)
		if err != nil {
			p.log.Debugf("error reading email body %d: %v", m.ID, err)
		}
		message := service.NewMessage(body[:n])
		for k, v := range entity.Header.Map() {
			message.MetaSet(k, v[0])
		}
		wg.Add(1)
		p.messageChan <- &envelope{message, m.ID}
	}
	acked := make(chan bool)
	go func() {
		for {
			select {
			case ID := <-p.ack:
				if err := c.Dele(ID); err != nil {
					p.log.Errorf("error acknowledging email %d: %v", ID, err)
				}
				wg.Done()
			case <-acked:
				return
			}
		}
	}()
	wg.Wait()
	acked <- true
	return len(msgs), nil
}

// Read attempts to read a new page from the Pop3 server.
func (p *pop) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case e := <-p.messageChan:
		return e.message, func(ctx context.Context, err error) error {
			p.ack <- e.ID
			return nil
		}, nil
	case <-p.done:
		return nil, nil, service.ErrEndOfInput
	}
}

// Close is called when the pipeline ends
func (p *pop) Close(ctx context.Context) (err error) {
	return
}
