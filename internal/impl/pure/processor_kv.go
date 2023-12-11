package pure

import (
	"context"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func init() {
	err := bundle.AllProcessors.Add(func(conf processor.Config, mgr bundle.NewManagement) (processor.V1, error) {
		p, err := newKV(conf.KV, mgr)
		if err != nil {
			return nil, err
		}
		return processor.NewAutoObservedProcessor("kv", p, mgr), nil
	}, docs.ComponentSpec{
		Name:    "kv",
		Summary: `This processor is designed to convert messages formatted as 'foo=bar bar=baz' into a structured JSON representation.`,
		Config: docs.FieldComponent().WithChildren(
			docs.FieldString("field_split", "Delimiter to use for splitting key-value pairs."),
			docs.FieldString("value_split", "Delimiter to use for splitting the key from the value within a key-value pairs."),
		).ChildDefaultAndTypesFromStruct(processor.NewKVConfig()),
		Examples: []docs.AnnotatedExample{
			{
				Title: "Mapping",
				Summary: `
When providing the following message:
` + "```" + `
level=info msg=this is a message logSource=source
` + "```" + `
The resulting logs will be formatted as:
` + "```json" + `
{
  "level": "info",
  "logSource": "source",
  "msg": "this is a message"
}
` + "```" + `
`,
				Config: `
pipeline:
  processors:
  - kv:
      field_split: " "
      value_split: "="
`,
			},
		},
	})
	if err != nil {
		panic(err)
	}
}

type kvProc struct {
	log log.Modular

	fieldSplit string
	valueSplit string
}

func newKV(conf processor.KVConfig, mgr bundle.NewManagement) (*kvProc, error) {
	return &kvProc{
		log:        mgr.Logger(),
		fieldSplit: conf.FieldSplit,
		valueSplit: conf.ValueSplit,
	}, nil
}

func (k kvProc) Process(ctx context.Context, msg *message.Part) ([]*message.Part, error) {
	msgBytes := msg.AsBytes()
	msgStr := string(msgBytes)

	k.log.Debugf("service.Message: %s", msgStr)

	segments := strings.Split(msgStr, k.fieldSplit)

	obj := make(map[string]string)
	currentKey := ""

	for _, segment := range segments {
		if strings.Contains(segment, k.valueSplit) {
			parts := strings.Split(segment, k.valueSplit)
			if len(parts) > 0 {
				key := parts[0]
				value := parts[1]
				obj[key] = value
				currentKey = key
			}
		} else if segment != "" && currentKey != "" {
			obj[currentKey] += k.fieldSplit + segment
		}
	}

	msg.SetStructured(obj)
	return []*message.Part{msg}, nil
}

func (k kvProc) Close(ctx context.Context) error {
	return nil
}
