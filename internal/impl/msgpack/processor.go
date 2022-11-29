package msgpack

import (
	"context"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/benthosdev/benthos/v4/public/service"
)

func processorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Beta().
		Categories("Parsing").
		Summary("Converts messages to or from the [MessagePack](https://msgpack.org/) format.").
		Field(service.NewStringAnnotatedEnumField("operator", map[string]string{
			"to_json":   "Convert MessagePack messages to JSON format",
			"from_json": "Convert JSON messages to MessagePack format",
		}).Description("The operation to perform on messages.")).
		Version("3.59.0")
}

func init() {
	err := service.RegisterProcessor(
		"msgpack", processorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newProcessorFromConfig(conf)
		})
	if err != nil {
		panic(err)
	}
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
