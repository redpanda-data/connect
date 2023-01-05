package beanstalkd

import (
	"context"
	"sync"
	"time"

	"github.com/beanstalkd/go-beanstalk"

	"github.com/benthosdev/benthos/v4/public/service"
)

func beanstalkdOutputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.7.0").
		Summary("Write messages to a Beanstalkd queue.").
		Field(service.NewStringField("address").
			Description("An address to connect to.").
			Example("127.0.0.1:11300")).
		Field(service.NewIntField("max_in_flight").
			Description("The maximum number of messages to have in flight at a given time. Increase to improve throughput.").
			Default(64))
}

func init() {
	err := service.RegisterOutput(
		"beanstalkd", beanstalkdOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			w, err := newBeanstalkdWriterFromConfig(conf, mgr.Logger())
			return w, maxInFlight, err
		})
	if err != nil {
		panic(err)
	}
}

type beanstalkdWriter struct {
	connection *beanstalk.Conn
	connMut    sync.Mutex

	address string
	log     *service.Logger
}

func newBeanstalkdWriterFromConfig(conf *service.ParsedConfig, log *service.Logger) (*beanstalkdWriter, error) {
	bs := beanstalkdWriter{
		log: log,
	}

	tcpAddr, err := conf.FieldString("address")
	if err != nil {
		return nil, err
	}
	bs.address = tcpAddr

	return &bs, nil
}

func (bs *beanstalkdWriter) Connect(ctx context.Context) error {
	bs.connMut.Lock()
	defer bs.connMut.Unlock()

	conn, err := beanstalk.Dial("tcp", bs.address)
	if err != nil {
		return err
	}

	bs.connection = conn
	bs.log.Infof("Sending Beanstalkd messages to address: %s\n", bs.address)
	return nil
}

func (bs *beanstalkdWriter) Write(ctx context.Context, msg *service.Message) error {
	bs.connMut.Lock()
	conn := bs.connection
	bs.connMut.Unlock()

	if conn == nil {
		return service.ErrNotConnected
	}

	msgBytes, err := msg.AsBytes()
	if err != nil {
		return err
	}
	_, err = conn.Put(msgBytes, 2, 0, time.Second*2)
	return err
}

func (bs *beanstalkdWriter) Close(context.Context) error {
	bs.connMut.Lock()
	defer bs.connMut.Unlock()

	if bs.connection != nil {
		if err := bs.connection.Close(); err != nil {
			return err
		}
	}
	return nil
}
