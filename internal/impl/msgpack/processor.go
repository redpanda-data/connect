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

package msgpack

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/redpanda-data/benthos/v4/public/service"
)

func processorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary("Converts messages to or from the https://msgpack.org/[MessagePack^] format.").
		Field(service.NewStringAnnotatedEnumField("operator", map[string]string{
			"to_json":   "Convert MessagePack messages to JSON format",
			"from_json": "Convert JSON messages to MessagePack format",
		}).Description("The operation to perform on messages.")).
		Version("3.59.0")
}

func init() {
	service.MustRegisterProcessor(
		"msgpack", processorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProcessorFromConfig(conf)
		})
}

type msgPackOperator func(m *service.Message) (*service.Message, error)

func strToMsgPackOperator(opStr string) (msgPackOperator, error) {
	switch opStr {
	case "to_json":
		return func(m *service.Message) (*service.Message, error) {
			mBytes, err := m.AsBytes()
			if err != nil {
				return nil, err
			}

			var jObj any
			if err := msgpack.Unmarshal(mBytes, &jObj); err != nil {
				return nil, fmt.Errorf("failed to convert MsgPack document to JSON: %v", err)
			}

			m.SetStructuredMut(jObj)
			return m, nil
		}, nil
	case "from_json":
		return func(m *service.Message) (*service.Message, error) {
			jObj, err := m.AsStructured()
			if err != nil {
				return nil, fmt.Errorf("failed to parse message as JSON: %v", err)
			}

			b, err := msgpack.Marshal(jObj)
			if err != nil {
				return nil, fmt.Errorf("failed to convert JSON to MsgPack: %v", err)
			}

			m.SetBytes(b)
			return m, nil
		}, nil
	}
	return nil, fmt.Errorf("operator not recognised: %v", opStr)
}

//------------------------------------------------------------------------------

type processor struct {
	operator msgPackOperator
}

func newProcessorFromConfig(conf *service.ParsedConfig) (*processor, error) {
	operatorStr, err := conf.FieldString("operator")
	if err != nil {
		return nil, err
	}
	return newProcessor(operatorStr)
}

func newProcessor(operatorStr string) (*processor, error) {
	operator, err := strToMsgPackOperator(operatorStr)
	if err != nil {
		return nil, err
	}
	return &processor{
		operator: operator,
	}, nil
}

func (p *processor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	resMsg, err := p.operator(msg)
	if err != nil {
		return nil, err
	}
	return service.MessageBatch{resMsg}, nil
}

func (p *processor) Close(ctx context.Context) error {
	return nil
}
