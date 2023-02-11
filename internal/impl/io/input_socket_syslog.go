package io

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/benthosdev/benthos/v4/public/service"

	"github.com/jeromer/syslogparser/rfc3164"
	"github.com/jeromer/syslogparser/rfc5424"
)

func init() {
	err := service.RegisterInput(
		"socket_syslog",
		service.NewConfigSpec().
			Summary("Creates a unix domain socket that receives syslog messages over a streaming or datagram socket.").
			Description(`

This input currently supports the `+"`rfc3164`"+` and `+"`rfc5424`"+` syslog header formats.

If parsing fails, Benthos will raise an error. If you find this happening often, we recommend using the [socket_server](/docs/components/inputs/socket_server) input component combined with regex codec to implement your own parsing scheme.

`).
			Field(service.NewStringField("address").
				Description("The address to listen from.").
				Default("")).
			Field(service.NewStringEnumField("type", "unix", "unixgram").
				Description("Type of unix domain socket").
				Default("unixgram")).
			Field(service.NewIntField("max_buffer").
				Description("The maximum message buffer size. Must exceed the largest message to be consumed.").
				Default(10000).
				Advanced()).
			Field(service.NewStringEnumField("header_format", "rfc3164", "rfc5424").
				Description("Specify the syslog message header format.").
				Default("rfc3164").
				Advanced()),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newSyslogUnixDomainSocket(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

type syslogUnixReader struct {
	address       string
	maxBufferSize int
	headerFormat  string
	socketType    string
	m             sync.RWMutex
	datagramCn    *net.UnixConn
	streamingCn   net.Listener
	buf           []byte

	log *service.Logger
}

func newSyslogUnixDomainSocket(conf *service.ParsedConfig, log *service.Logger) (p *syslogUnixReader, err error) {
	p = &syslogUnixReader{
		log: log,
	}

	if p.address, err = conf.FieldString("address"); err != nil {
		return
	}
	if p.maxBufferSize, err = conf.FieldInt("max_buffer"); err != nil {
		return
	}
	if p.headerFormat, err = conf.FieldString("header_format"); err != nil {
		return
	}
	if p.socketType, err = conf.FieldString("type"); err != nil {
		return
	}

	if p.address == "" {
		p.log.Errorf("Field address must not be empty")
		return
	}
	switch p.socketType {
	case "unix":
		p.streamingCn, err = p.createStreamingSocket()
		if err != nil {
			p.log.Errorf("Couldn't create socket: %v", err)
		}
	case "unixgram":
		p.datagramCn, err = p.createDatagramSocket()
		if err != nil {
			p.log.Errorf("Couldn't create socket: %v", err)
		}
	}
	return
}

func (p *syslogUnixReader) createStreamingSocket() (net.Listener, error) {
	if _, err := os.Stat(p.address); err == nil {
		os.Remove(p.address)
	}
	conn, err := net.Listen(p.socketType, p.address)
	if err != nil {
		return nil, err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(p.address)
		conn.Close()
		os.Exit(1)
	}()
	return conn, nil
}

func (p *syslogUnixReader) createDatagramSocket() (*net.UnixConn, error) {
	if _, err := os.Stat(p.address); err == nil {
		if err := os.Remove(p.address); err != nil {
			p.log.Errorf("Couldn't create socket: %v", err)
		}
	}

	laddr, err := net.ResolveUnixAddr(p.socketType, p.address)
	if err != nil {
		return nil, err
	}
	unixConn, err := net.ListenUnixgram(p.socketType, laddr)
	if err != nil {
		return nil, err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		os.Remove(p.address)
		unixConn.Close()
		os.Exit(1)
	}()

	return unixConn, nil
}

func (p *syslogUnixReader) recvDatagramMessage() (string, error) {
	p.buf = make([]byte, p.maxBufferSize)
	_, _, err := p.datagramCn.ReadFromUnix(p.buf)
	if err != nil {
		p.log.Errorf("Couldn't read Syslog message: &v", err)
	}
	msg, err := p.parseSyslogMessage(p.buf)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func (p *syslogUnixReader) recvStreamingMessage() (string, error) {
	p.buf = make([]byte, p.maxBufferSize)
	conn, err := p.streamingCn.Accept()
	if err != nil {
		p.log.Errorf("Couldn't accept message on socket: ", err)
	}

	defer conn.Close()

	_, err = conn.Read(p.buf)
	if err != nil {
		p.log.Errorf("Couldn't read Syslog message: &v", err)
		return "", err
	}

	msg, err := p.parseSyslogMessage(p.buf)
	if err != nil {
		return "", err
	}
	return msg, nil
}

func (p *syslogUnixReader) parseSyslogMessage(buf []byte) (string, error) {
	switch p.headerFormat {
	case "rfc5424":
		m := rfc5424.NewParser(buf)
		if err := m.Parse(); err != nil {
			return "", err
		}
		message := fmt.Sprintf("%v", m.Dump()["content"])
		return message, nil
	case "rfc3164":
		m := rfc3164.NewParser(buf)
		if err := m.Parse(); err != nil {
			return "", err
		}
		message := fmt.Sprintf("%v", m.Dump()["content"])
		return message, nil
	default:
		return "", nil
	}
}

func (p *syslogUnixReader) Connect(ctx context.Context) error {
	p.m.Lock()
	defer p.m.Unlock()

	return nil
}

func (p *syslogUnixReader) disconnect(ctx context.Context) error {
	p.m.Lock()
	defer p.m.Unlock()
	os.Remove(p.address)

	return nil
}

func (p *syslogUnixReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	switch p.socketType {
	case "unix":
		message, err := p.recvStreamingMessage()
		if err != nil {
			return nil, func(ctx context.Context, err error) error {
				return err
			}, err
		}
		return service.NewMessage([]byte(message)), func(ctx context.Context, err error) error {
			return nil
		}, nil
	case "unixgram":
		message, err := p.recvDatagramMessage()
		if err != nil {
			return nil, func(ctx context.Context, err error) error {
				return err
			}, err
		}
		return service.NewMessage([]byte(message)), func(ctx context.Context, err error) error {
			return nil
		}, nil

	}
	return nil, nil, nil
}

func (p *syslogUnixReader) Close(ctx context.Context) error {
	return p.disconnect(ctx)
}
