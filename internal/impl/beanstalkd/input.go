package beanstalkd

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/beanstalkd/go-beanstalk"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/input/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllInputs.Add(processors.WrapConstructor(newBeanstalkdInput), docs.ComponentSpec{
		Name:	 "beanstalkd",
		Summary: `Subscribe to a beanstalked instance.`,
		Config: docs.FieldComponent().WithChildren(
				docs.FieldString("tcp_address", "Beanstalkd address to connect to."),
		),
	})
	if err != nil {
		panic(err)
	}
}

func newBeanstalkdInput(conf input.Config, mgr bundle.NewManagement) (input.Streamed, error) {
	var bs input.Async
	var err error
	if bs, err = newBeanstalkdReader(conf.Beanstalkd, mgr.Logger()); err != nil {
		return nil, err
	}
	return input.NewAsyncReader("beanstalkd", true, bs, mgr)
}

type beanstalkdReader struct {
	connection 	*beanstalk.Conn
	bMut		sync.Mutex

	address		string
	conf		input.BeanstalkdConfig
	log			log.Modular
}

func newBeanstalkdReader(conf input.BeanstalkdConfig, log log.Modular) (*beanstalkdReader, error) {
	bs := beanstalkdReader {
		conf:	conf,
		log:	log,
	}
	bs.address = conf.Address

	return &bs, nil
}

func (bs *beanstalkdReader) Connect(ctx context.Context) (err error) {
	bs.bMut.Lock()
	defer bs.bMut.Unlock()

	conn, err := beanstalk.Dial("tcp", bs.address)
	if err != nil {
		return
	}

	bs.connection = conn
	bs.log.Infof("Receiving Beanstalkd messages from address: %s\n", bs.address)
	return
}

func (bs *beanstalkdReader) disconnect() error {
	bs.bMut.Lock()
	defer bs.bMut.Unlock()

	if bs.connection != nil {
		if err := bs.connection.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (bs *beanstalkdReader) ReadBatch(ctx context.Context) (message.Batch, input.AsyncAckFn, error) {
	var mpStats map[string]string
	res := make([][]byte, 0)

	mpStats, err := bs.connection.Stats()
	if err != nil {
		return nil, nil, err
	}

	jobs, err := strconv.Atoi(mpStats["current-jobs-ready"])
	if err != nil {
		return nil, nil, err
	}

	for i := 0; i < jobs; i++ {
		id, body, err := bs.connection.Reserve(time.Millisecond * 200)
		if err != nil {
			return nil, nil, err
		}

		res = append(res, body)
		err = bs.connection.Delete(id)
		if err != nil {
			return nil, nil, err
		}
	}

	return message.QuickBatch(res), func(ctx context.Context, res error) error {
		return nil
	}, nil
}

func (bs *beanstalkdReader) Close(ctx context.Context) (err error) {
	err = bs.disconnect()
	return
}
