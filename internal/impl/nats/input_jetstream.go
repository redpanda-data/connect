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

package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/Jeffail/shutdown"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func natsJetStreamInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Version("3.46.0").
		Summary("Reads messages from NATS JetStream subjects.").
		Description(`
== Consume mirrored streams

In the case where a stream being consumed is mirrored from a different JetStream domain the stream cannot be resolved from the subject name alone, and so the stream name as well as the subject (if applicable) must both be specified.

== Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- nats_subject
- nats_sequence_stream
- nats_sequence_consumer
- nats_num_delivered
- nats_num_pending
- nats_domain
- nats_timestamp_unix_nano
` + "```" + `

You can access these metadata fields using
xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewStringField("queue").
			Description("An optional queue group to consume as. Used to configure a push consumer.").
			Optional()).
		Field(service.NewStringField("subject").
			Description("A subject to consume from. Supports wildcards for consuming multiple subjects. Either a subject or stream must be specified.").
			Optional().
			Example("foo.bar.baz").Example("foo.*.baz").Example("foo.bar.*").Example("foo.>")).
		Field(service.NewStringField("durable").
			Description("Preserve the state of your consumer under a durable name. Used to configure a pull consumer.").
			Optional()).
		LintRule(`root = match {
			this.exists("queue") && this.queue != "" && this.exists("durable") && this.durable != "" => [ "both 'queue' and 'durable' can't be set simultaneously" ],
			}`).
		Field(service.NewStringField("stream").
			Description("A stream to consume from. Either a subject or stream must be specified.").
			Optional()).
		Field(service.NewBoolField("bind").
			Description("Indicates that the subscription should use an existing consumer.").
			Optional()).
		Field(service.NewStringAnnotatedEnumField("deliver", map[string]string{
			"all":              "Deliver all available messages.",
			"last":             "Deliver starting with the last published messages.",
			"last_per_subject": "Deliver starting with the last published message per subject.",
			"new":              "Deliver starting from now, not taking into account any previous messages.",
		}).
			Description("Determines which messages to deliver when consuming without a durable subscriber.").
			Default("all")).
		Field(service.NewStringField("ack_wait").
			Description("The maximum amount of time NATS server should wait for an ack from consumer.").
			Advanced().
			Default("30s").
			Example("100ms").
			Example("5m")).
		Field(service.NewIntField("max_ack_pending").
			Description("The maximum number of outstanding acks to be allowed before consuming is halted.").
			Advanced().
			Default(1024)).
		Field(service.NewBoolField("create_if_not_exists").
			Description("Create the `stream` and `subject` if do not exist.").
			Advanced().
			Default(false)).
		Field(service.NewIntField("num_replicas").
			Description("The number of stream replicas, only supported in clustered mode and if the stream is created when `create_if_not_exists` is set to true. Defaults to 1, maximum is 5.").
			Advanced().
			Default(1)).
		Field(service.NewStringEnumField("storage_type", "memory", "file").
			Description("Storage type to use when the stream does not exist and is created when `create_if_not_exists` is set to true. Can be `memory` or `file` storage.").
			Advanced().
			Default("memory")).
		Field(service.NewDurationField("nak_delay").
			Description("An optional delay duration on redelivering the messages when negatively acknowledged.").
			Example("1m").
			Advanced().
			Optional()).
		Field(service.NewStringField("nak_delay_until_header").
			Description("An optional header name on which will come a unix epoch timestamp in seconds until when the message delivery should be delayed. By default is `nak_delay_until`").
			Advanced().
			Default("nak_delay_until")).
		Fields(connectionTailFields()...).
		Field(inputTracingDocs())
}

func init() {
	err := service.RegisterInput(
		"nats_jetstream", natsJetStreamInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			input, err := newJetStreamReaderFromConfig(conf, mgr)
			if err != nil {
				return nil, err
			}
			return conf.WrapInputExtractTracingSpanMapping("nats_jetstream", input)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type jetStreamReader struct {
	connDetails         connectionDetails
	deliverOpt          nats.SubOpt
	subject             string
	queue               string
	stream              string
	bind                bool
	pull                bool
	durable             string
	ackWait             time.Duration
	nakDelay            time.Duration
	nakDelayUntilHeader string
	maxAckPending       int
	createIfNotExists   bool
	numReplicas         int
	storageType         string

	log *service.Logger

	connMut  sync.Mutex
	natsConn *nats.Conn
	natsSub  *nats.Subscription

	shutSig *shutdown.Signaller
}

func newJetStreamReaderFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*jetStreamReader, error) {
	j := jetStreamReader{
		log:     mgr.Logger(),
		shutSig: shutdown.NewSignaller(),
	}

	var err error
	if j.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	deliver, err := conf.FieldString("deliver")
	if err != nil {
		return nil, err
	}
	switch deliver {
	case "all":
		j.deliverOpt = nats.DeliverAll()
	case "last":
		j.deliverOpt = nats.DeliverLast()
	case "last_per_subject":
		j.deliverOpt = nats.DeliverLastPerSubject()
	case "new":
		j.deliverOpt = nats.DeliverNew()
	default:
		return nil, fmt.Errorf("deliver option %v was not recognised", deliver)
	}

	if conf.Contains("subject") {
		if j.subject, err = conf.FieldString("subject"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("queue") {
		if j.queue, err = conf.FieldString("queue"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("durable") {
		if j.durable, err = conf.FieldString("durable"); err != nil {
			return nil, err
		}
	}
	if j.queue != "" && j.durable != "" {
		return nil, errors.New("both 'queue' and 'durable' cannot be set simultaneously")
	}

	if conf.Contains("stream") {
		if j.stream, err = conf.FieldString("stream"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("bind") {
		if j.bind, err = conf.FieldBool("bind"); err != nil {
			return nil, err
		}
	}
	if j.bind {
		if j.stream == "" && j.durable == "" {
			return nil, errors.New("stream or durable is required, when bind is true")
		}
	} else {
		if j.subject == "" && j.stream == "" {
			return nil, errors.New("subject and stream are empty")
		}
	}

	ackWaitStr, err := conf.FieldString("ack_wait")
	if err != nil {
		return nil, err
	}
	if ackWaitStr != "" {
		j.ackWait, err = time.ParseDuration(ackWaitStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse ack wait duration: %v", err)
		}
	}

	if conf.Contains("create_if_not_exists") {
		if j.createIfNotExists, err = conf.FieldBool("create_if_not_exists"); err != nil {
			return nil, err
		}
	}
	if conf.Contains("num_replicas") {
		if j.numReplicas, err = conf.FieldInt("num_replicas"); err != nil {
			return nil, err
		}
		if j.numReplicas < 1 || j.numReplicas > 5 {
			return nil, fmt.Errorf("num_replicas %d is invalid, it must be between 1 and 5", j.numReplicas)
		}
	}
	if conf.Contains("storage_type") {
		if j.storageType, err = conf.FieldString("storage_type"); err != nil {
			return nil, err
		}
	}

	if conf.Contains("nak_delay") {
		if j.nakDelay, err = conf.FieldDuration("nak_delay"); err != nil {
			return nil, err
		}
	}
	if j.nakDelayUntilHeader, err = conf.FieldString("nak_delay_until_header"); err != nil {
		return nil, err
	}

	if j.maxAckPending, err = conf.FieldInt("max_ack_pending"); err != nil {
		return nil, err
	}
	return &j, nil
}

//------------------------------------------------------------------------------

func (j *jetStreamReader) Connect(ctx context.Context) (err error) {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsConn != nil {
		return nil
	}

	var natsConn *nats.Conn
	var natsSub *nats.Subscription

	defer func() {
		if err != nil {
			if natsSub != nil {
				_ = natsSub.Drain()
			}
			if natsConn != nil {
				natsConn.Close()
			}
		}
	}()

	if natsConn, err = j.connDetails.get(ctx); err != nil {
		return err
	}

	jCtx, err := natsConn.JetStream()
	if err != nil {
		return err
	}

	if j.bind && j.stream != "" && j.durable != "" {
		info, err := jCtx.ConsumerInfo(j.stream, j.durable)
		if err != nil {
			return err
		}

		if j.subject == "" {
			if info.Config.DeliverSubject != "" {
				j.subject = info.Config.DeliverSubject
			} else if info.Config.FilterSubject != "" {
				j.subject = info.Config.FilterSubject
			}
		}

		j.pull = info.Config.DeliverSubject == ""
	}

	options := []nats.SubOpt{
		nats.ManualAck(),
	}

	if j.pull {
		options = append(options, nats.Bind(j.stream, j.durable))

		natsSub, err = jCtx.PullSubscribe(j.subject, j.durable, options...)
	} else {
		if j.durable != "" {
			options = append(options, nats.Durable(j.durable))
		}
		options = append(options, j.deliverOpt)
		if j.ackWait > 0 {
			options = append(options, nats.AckWait(j.ackWait))
		}
		if j.maxAckPending != 0 {
			options = append(options, nats.MaxAckPending(j.maxAckPending))
		}

		if j.bind && j.stream != "" && j.durable != "" {
			options = append(options, nats.Bind(j.stream, j.durable))
		} else if j.stream != "" {
			options = append(options, nats.BindStream(j.stream))
		}

		if j.queue == "" {
			natsSub, err = jCtx.SubscribeSync(j.subject, options...)
		} else {
			natsSub, err = jCtx.QueueSubscribeSync(j.subject, j.queue, options...)
		}
	}

	if err != nil {
		if j.createIfNotExists {
			var natsErr *nats.APIError
			if errors.As(err, &natsErr) {
				if natsErr.ErrorCode == nats.JSErrCodeStreamNotFound {
					// create stream and subject
					_, err = jCtx.AddStream(&nats.StreamConfig{
						Name:     j.stream,
						Subjects: []string{"*"},
						Storage: func() nats.StorageType {
							if j.storageType == "file" {
								return nats.FileStorage
							}
							return nats.MemoryStorage
						}(),
						Replicas: j.numReplicas,
					})
				}
			}
		}
		return err
	}

	j.natsConn = natsConn
	j.natsSub = natsSub
	return nil
}

func (j *jetStreamReader) disconnect() {
	j.connMut.Lock()
	defer j.connMut.Unlock()

	if j.natsSub != nil {
		_ = j.natsSub.Drain()
		j.natsSub = nil
	}
	if j.natsConn != nil {
		j.natsConn.Close()
		j.natsConn = nil
	}
}

func (j *jetStreamReader) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	j.connMut.Lock()
	natsSub := j.natsSub
	j.connMut.Unlock()
	if natsSub == nil {
		return nil, nil, service.ErrNotConnected
	}
	if !j.pull {
		nmsg, err := natsSub.NextMsgWithContext(ctx)
		if err != nil {
			// TODO: Any errors need capturing here to signal a lost connection?
			return nil, nil, err
		}
		return j.convertMessage(nmsg)
	}

	for {
		msgs, err := natsSub.Fetch(1, nats.Context(ctx))
		if err != nil {
			if errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) {
				// NATS enforces its own context that might time out faster than the original context
				// Let's check if it was the original context that timed out
				select {
				case <-ctx.Done():
					return nil, nil, ctx.Err()
				default:
					continue
				}
			}
			return nil, nil, err
		}
		if len(msgs) == 0 {
			continue
		}
		return j.convertMessage(msgs[0])
	}
}

func (j *jetStreamReader) Close(ctx context.Context) error {
	go func() {
		j.disconnect()
		j.shutSig.TriggerHasStopped()
	}()
	select {
	case <-j.shutSig.HasStoppedChan():
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (j *jetStreamReader) convertMessage(m *nats.Msg) (*service.Message, service.AckFunc, error) {
	msg := service.NewMessage(m.Data)
	msg.MetaSet("nats_subject", m.Subject)

	metadata, err := m.Metadata()
	if err == nil {
		msg.MetaSet("nats_sequence_stream", strconv.Itoa(int(metadata.Sequence.Stream)))
		msg.MetaSet("nats_sequence_consumer", strconv.Itoa(int(metadata.Sequence.Consumer)))
		msg.MetaSet("nats_num_delivered", strconv.Itoa(int(metadata.NumDelivered)))
		msg.MetaSet("nats_num_pending", strconv.Itoa(int(metadata.NumPending)))
		msg.MetaSet("nats_domain", metadata.Domain)
		msg.MetaSet("nats_timestamp_unix_nano", strconv.Itoa(int(metadata.Timestamp.UnixNano())))
	}

	for k := range m.Header {
		v := m.Header.Get(k)
		if v != "" {
			msg.MetaSet(k, v)
		}
	}

	return msg, func(ctx context.Context, res error) error {
		if res != nil {
			if val, ok := m.Header[j.nakDelayUntilHeader]; ok {
				if unixTime, err := strconv.ParseInt(val[0], 10, 64); err != nil {
					j.log.Warnf("error parsing unix epoch time from header %s: %s error: %v", j.nakDelayUntilHeader, val[0], err)
					return m.Nak()
				} else {
					return m.NakWithDelay(time.Unix(unixTime, 0).Sub(time.Now().UTC()))
				}
			} else if j.nakDelay > 0 {
				return m.NakWithDelay(j.nakDelay)
			}
			return m.Nak()
		}
		return m.Ack()
	}, nil
}
