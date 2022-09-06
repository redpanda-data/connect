package beanstalkd

import (
	"context"
	"sync"
	"time"

	"github.com/beanstalkd/go-beanstalk"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllOutputs.Add(processors.WrapConstructor(newBeanstalkdOutput), docs.ComponentSpec {
		Name:		"beanstalkd",
		Summary:	`Publish to a beanstalkd queue.`,
		Config: docs.FieldComponent().WithChildren(
				docs.FieldString("tcp_address", "The address of the target Beanstalkd server."),
				docs.FieldInt("max_in_flight", "The maximum number of messages to have in flight at a given time. Increase this to improve throughput."),
		).ChildDefaultAndTypesFromStruct(output.NewBeanstalkdConfig()),
		Categories: []string{
			"Services",
		},
	})
	if err != nil {
		panic(err)
	}
}

func newBeanstalkdOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	w, err := newBeanstalkdWriter(conf.Beanstalkd, mgr)
	if err != nil {
		return nil, err
	}
	return output.NewAsyncWriter("beanstalkd", conf.Beanstalkd.MaxInFlight, w, mgr)
}

type beanstalkdWriter struct {
	connection	*beanstalk.Conn
	connMut		sync.RWMutex

	log		log.Modular
	conf	output.BeanstalkdConfig
}

func newBeanstalkdWriter(conf output.BeanstalkdConfig, mgr bundle.NewManagement) (*beanstalkdWriter, error) {
	bs := beanstalkdWriter {
		log:	mgr.Logger(),
		conf:	conf,
	}
	return &bs, nil
}

func (bs *beanstalkdWriter) Connect(ctx context.Context) error {
	bs.connMut.Lock()
	defer bs.connMut.Unlock()

	conn, err := beanstalk.Dial("tcp", bs.conf.Address)
	if err != nil {
		return err
	}

	bs.connection = conn
	bs.log.Infof("Sending Beanstalkd messages to address: %s\n", bs.conf.Address)
	return nil
}

func (bs *beanstalkdWriter) WriteBatch(ctx context.Context, msg message.Batch) error {
	bs.connMut.Lock()
	conn := bs.connection
	bs.connMut.Unlock()

	if conn == nil {
		return component.ErrNotConnected
	}

	return output.IterateBatchedSend(msg, func(i int, p *message.Part) error {
		_, err := conn.Put(p.AsBytes(), 2, 0, time.Second * 2)
		return err
	})
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
