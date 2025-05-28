// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package beanstalkd

import (
	"context"
	"sync"
	"time"

	"github.com/beanstalkd/go-beanstalk"

	"github.com/redpanda-data/benthos/v4/public/service"
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
	service.MustRegisterOutput(
		"beanstalkd", beanstalkdOutputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Output, int, error) {
			maxInFlight, err := conf.FieldInt("max_in_flight")
			if err != nil {
				return nil, 0, err
			}
			w, err := newBeanstalkdWriterFromConfig(conf, mgr.Logger())
			return w, maxInFlight, err
		})
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
