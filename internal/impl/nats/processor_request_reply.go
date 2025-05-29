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
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func natsRequestReplyConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Services").
		Version("4.27.0").
		Summary("Sends a message to a NATS subject and expects a reply, from a NATS subscriber acting as a responder, back.").
		Description(`
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

You can access these metadata fields using xref:configuration:interpolation.adoc#bloblang-queries[function interpolation].

` + connectionNameDescription() + authDescription()).
		Fields(connectionHeadFields()...).
		Field(service.NewInterpolatedStringField("subject").
			Description("A subject to write to.").
			Example("foo.bar.baz").
			Example(`${! meta("kafka_topic") }`).
			Example(`foo.${! json("meta.type") }`)).
		Field(service.NewStringField("inbox_prefix").
			Description("Set an explicit inbox prefix for the response subject").
			Optional().
			Advanced().
			Example("_INBOX_joe")).
		Field(service.NewInterpolatedStringMapField("headers").
			Description("Explicit message headers to add to messages.").
			Default(map[string]any{}).
			Example(map[string]any{
				"Content-Type": "application/json",
				"Timestamp":    `${!meta("Timestamp")}`,
			})).
		Field(service.NewMetadataFilterField("metadata").
			Description("Determine which (if any) metadata values should be added to messages as headers.").
			Optional()).
		Field(service.NewStringField("timeout").
			Description("A duration string is a possibly signed sequence of decimal numbers, each with optional fraction and a unit suffix, such as 300ms, -1.5h or 2h45m. Valid time units are ns, us (or µs), ms, s, m, h.").
			Optional().
			Default("3s")).
		Fields(connectionTailFields()...)
}

func init() {
	service.MustRegisterProcessor("nats_request_reply", natsRequestReplyConfig(), newRequestReplyProcessor)
}

type requestReplyProcessor struct {
	connDetails connectionDetails
	headers     map[string]*service.InterpolatedString
	metaFilter  *service.MetadataFilter
	subject     *service.InterpolatedString
	inboxPrefix string
	timeout     time.Duration

	log *service.Logger

	natsConn *nats.Conn
	connMut  sync.RWMutex
}

func newRequestReplyProcessor(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
	p := &requestReplyProcessor{
		log: mgr.Logger(),
	}

	var err error
	if p.connDetails, err = connectionDetailsFromParsed(conf, mgr); err != nil {
		return nil, err
	}

	if p.subject, err = conf.FieldInterpolatedString("subject"); err != nil {
		return nil, err
	}

	if conf.Contains("inbox_prefix") {
		if p.inboxPrefix, err = conf.FieldString("inbox_prefix"); err != nil {
			return nil, err
		}
	}

	if p.headers, err = conf.FieldInterpolatedStringMap("headers"); err != nil {
		return nil, err
	}
	timeoutStr, err := conf.FieldString("timeout")
	if err != nil {
		return nil, err
	}
	if p.timeout, err = time.ParseDuration(timeoutStr); err != nil {
		return nil, err
	}

	err = p.connect(context.Background())
	return p, err
}

func (r *requestReplyProcessor) connect(ctx context.Context) (err error) {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.natsConn != nil {
		return nil
	}

	defer func() {
		if err != nil {
			if r.natsConn != nil {
				r.natsConn.Close()
			}
		}
	}()

	var extraOpts []nats.Option
	if r.inboxPrefix != "" {
		extraOpts = append(extraOpts, nats.CustomInboxPrefix(r.inboxPrefix))
	}

	if r.natsConn, err = r.connDetails.get(ctx, extraOpts...); err != nil {
		return err
	}
	return nil
}

func (r *requestReplyProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	r.connMut.RLock()
	defer r.connMut.RUnlock()

	subject, err := r.subject.TryString(msg)
	if err != nil {
		return nil, err
	}

	nMsg := nats.NewMsg(subject)
	m := msg.Copy()
	nMsg.Data, err = m.AsBytes()
	if err != nil {
		return nil, err
	}

	if r.natsConn.HeadersSupported() {
		for k, v := range r.headers {
			headerStr, err := v.TryString(msg)
			if err != nil {
				return nil, fmt.Errorf("header %v interpolation error: %w", k, err)
			}
			nMsg.Header.Add(k, headerStr)
		}
		_ = r.metaFilter.Walk(msg, func(key, value string) error {
			nMsg.Header.Add(key, value)
			return nil
		})
	}

	callCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	r.log.Debugf("Sending NATS message to subject %s", subject)
	resp, err := r.natsConn.RequestMsgWithContext(callCtx, nMsg)
	if err != nil {
		return nil, err
	}
	m.SetBytes(resp.Data)
	if r.natsConn.HeadersSupported() {
		for key := range resp.Header {
			value := resp.Header.Get(key)
			m.MetaSetMut(key, value)
		}
	}
	return service.MessageBatch{m}, nil
}

func (r *requestReplyProcessor) Close(context.Context) error {
	r.connMut.Lock()
	defer r.connMut.Unlock()

	if r.natsConn != nil {
		r.natsConn.Close()
		r.natsConn = nil
	}
	return nil
}
