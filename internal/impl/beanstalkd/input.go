package beanstalkd

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/beanstalkd/go-beanstalk"

	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/public/service"
)

func beanstalkdInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.7.0").
		Summary("Reads messages from a Beanstalkd queue.").
		Field(service.NewStringField("address").
			Description("An address to connect to.").
			Example("127.0.0.1:11300"))
}

func init() {
	err := service.RegisterInput(
		"beanstalkd", beanstalkdInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newBeanstalkdReaderFromConfig(conf, mgr.Logger())
		})
	if err != nil {
		panic(err)
	}
}

type beanstalkdReader struct {
	connection *beanstalk.Conn
	connMut    sync.Mutex

	address string
	log     *service.Logger
}

func newBeanstalkdReaderFromConfig(conf *service.ParsedConfig, log *service.Logger) (*beanstalkdReader, error) {
	bs := beanstalkdReader{
		log: log,
	}

	tcpAddr, err := conf.FieldString("address")
	if err != nil {
		return nil, err
	}
	bs.address = tcpAddr

	return &bs, nil
}

func (bs *beanstalkdReader) Connect(ctx context.Context) error {
	bs.connMut.Lock()
	defer bs.connMut.Unlock()

	conn, err := beanstalk.Dial("tcp", bs.address)
	if err != nil {
		return err
	}

	bs.connection = conn
	return nil
}

func (bs *beanstalkdReader) disconnect() error {
	bs.connMut.Lock()
	defer bs.connMut.Unlock()

	if bs.connection != nil {
		if err := bs.connection.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (bs *beanstalkdReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	if bs.connection == nil {
		return nil, nil, service.ErrNotConnected
	}

	id, body, err := bs.connection.Reserve(time.Millisecond * 200)
	if err != nil {
		if errors.Is(err, beanstalk.ErrTimeout) {
			err = component.ErrTimeout
		}
		return nil, nil, err
	}

	msg := service.NewMessage(body)
	return msg, func(ctx context.Context, res error) error {
		if res == nil {
			return bs.connection.Delete(id)
		}
		return bs.connection.Release(id, 2, time.Millisecond*200)
	}, nil
}

func (bs *beanstalkdReader) Close(ctx context.Context) (err error) {
	err = bs.disconnect()
	return
}
