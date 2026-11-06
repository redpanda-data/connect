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

//go:build x_benthos_extra
// +build x_benthos_extra

package zeromq

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/pebbe/zmq4"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// zmqBuildDescription explains how to get a Redpanda Connect build that
// includes the zmq4 components. It is shared by the input and the output.
const zmqBuildDescription = `
This component links to the ZeroMQ C library (` + "`libzmq`" + `), so only cgo builds of Redpanda Connect include it. The standard release binaries, ` + "`rpk connect`" + `, and the Docker images don't include it.

To get a build that includes this component, either download the ` + "`redpanda-connect-cgo_<version>_linux_amd64.tar.gz`" + ` archive from the https://github.com/redpanda-data/connect/releases[Redpanda Connect releases^], or build from source with cgo enabled and the ` + "`x_benthos_extra`" + ` build tag:

` + "```bash" + `
# Install the libzmq headers first, for example on Debian or Ubuntu
sudo apt-get install libzmq3-dev

CGO_ENABLED=1 go build -tags x_benthos_extra,timetzdata ./cmd/redpanda-connect
` + "```" + `

Both builds link ` + "`libzmq`" + ` dynamically, so the machine that runs Redpanda Connect also needs the ZeroMQ shared library installed.`

// urlsField returns the `urls` field shared by the zmq4 input and output.
func urlsField() *service.ConfigField {
	return service.NewStringListField("urls").
		Description("A list of ZeroMQ endpoints to connect to, or to bind when `bind` is `true`, using a transport such as `tcp://`, `ipc://`, or `inproc://`. If an item in the list contains commas, it is split into multiple endpoints.")
}

// bindField returns the `bind` field shared by the zmq4 input and output.
func bindField() *service.ConfigField {
	return service.NewBoolField("bind").
		Description("Whether to bind to the URLs and wait for peers to connect, instead of connecting to them.")
}

func zmqInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Network").
		Summary("Consumes messages from a ZeroMQ socket.").
		Description(zmqBuildDescription).
		Field(urlsField().
			Example([]string{"tcp://localhost:5555"})).
		Field(bindField().
			Default(false)).
		Field(service.NewStringEnumField("socket_type", "PULL", "SUB").
			Description("The socket type to connect as.")).
		Field(service.NewStringListField("sub_filters").
			Description("The topics that the socket subscribes to when `socket_type` is `SUB`. ZeroMQ delivers a message only when its first frame begins with one of these prefixes, so specifying a single sub_filter of `''` subscribes to everything. At least one filter is required with a SUB socket.").
			ShortDescription("Subscription topic filters for a SUB socket. A single empty filter subscribes to everything.").
			Default([]any{})).
		Field(service.NewIntField("high_water_mark").
			Description("The message high water mark to use.").
			Default(0).
			Advanced()).
		Field(service.NewDurationField("poll_timeout").
			Description("The poll timeout to use.").
			Default("5s").
			Advanced())
}

func init() {
	service.MustRegisterBatchInput("zmq4", zmqInputConfig(), func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
		r, err := zmqInputFromConfig(conf, mgr)
		if err != nil {
			return nil, err
		}
		return service.AutoRetryNacksBatched(r), nil
	})
}

//------------------------------------------------------------------------------

type zmqInput struct {
	log *service.Logger

	urls        []string
	socketType  string
	hwm         int
	bind        bool
	subFilters  []string
	pollTimeout time.Duration

	poller *zmq4.Poller
	socket *zmq4.Socket
}

func zmqInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*zmqInput, error) {
	z := zmqInput{
		log: mgr.Logger(),
	}

	urlStrs, err := conf.FieldStringList("urls")
	if err != nil {
		return nil, err
	}

	for _, u := range urlStrs {
		for _, splitU := range strings.Split(u, ",") {
			if len(splitU) > 0 {
				z.urls = append(z.urls, splitU)
			}
		}
	}

	if z.bind, err = conf.FieldBool("bind"); err != nil {
		return nil, err
	}
	if z.socketType, err = conf.FieldString("socket_type"); err != nil {
		return nil, err
	}
	if _, err := getZMQInputType(z.socketType); err != nil {
		return nil, err
	}

	if z.subFilters, err = conf.FieldStringList("sub_filters"); err != nil {
		return nil, err
	}

	if z.socketType == "SUB" && len(z.subFilters) == 0 {
		return nil, errors.New("must provide at least one sub filter when connecting with a SUB socket, in order to subscribe to all messages add an empty string")
	}

	if z.hwm, err = conf.FieldInt("high_water_mark"); err != nil {
		return nil, err
	}

	if z.pollTimeout, err = conf.FieldDuration("poll_timeout"); err != nil {
		return nil, err
	}
	return &z, nil
}

//------------------------------------------------------------------------------

func getZMQInputType(t string) (zmq4.Type, error) {
	switch t {
	case "SUB":
		return zmq4.SUB, nil
	case "PULL":
		return zmq4.PULL, nil
	}
	return zmq4.PULL, errors.New("invalid ZMQ socket type")
}

func (z *zmqInput) Connect(ignored context.Context) (err error) {
	if z.socket != nil {
		return nil
	}

	t, err := getZMQInputType(z.socketType)
	if err != nil {
		return err
	}

	ctx, err := zmq4.NewContext()
	if err != nil {
		return err
	}

	var socket *zmq4.Socket
	if socket, err = ctx.NewSocket(t); err != nil {
		return err
	}

	defer func() {
		if err != nil && socket != nil {
			socket.Close()
		}
	}()

	_ = socket.SetRcvhwm(z.hwm)

	for _, address := range z.urls {
		if z.bind {
			err = socket.Bind(address)
		} else {
			err = socket.Connect(address)
		}
		if err != nil {
			return err
		}
	}

	for _, filter := range z.subFilters {
		if err := socket.SetSubscribe(filter); err != nil {
			return err
		}
	}

	z.socket = socket
	z.poller = zmq4.NewPoller()
	z.poller.Add(z.socket, zmq4.POLLIN)
	return nil
}

func (z *zmqInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if z.socket == nil {
		return nil, nil, service.ErrNotConnected
	}

	data, err := z.socket.RecvMessageBytes(zmq4.DONTWAIT)
	if err != nil {
		var polled []zmq4.Polled
		if polled, err = z.poller.Poll(z.pollTimeout); len(polled) == 1 {
			data, err = z.socket.RecvMessageBytes(0)
		} else if err == nil {
			return nil, nil, context.Canceled
		}
	}
	if err != nil {
		return nil, nil, err
	}

	var batch service.MessageBatch
	for _, d := range data {
		batch = append(batch, service.NewMessage(d))
	}

	return batch, func(ctx context.Context, err error) error {
		return nil
	}, nil
}

// CloseAsync shuts down the zmqInput input and stops processing requests.
func (z *zmqInput) Close(ctx context.Context) error {
	if z.socket != nil {
		z.socket.Close()
		z.socket = nil
	}
	return nil
}
