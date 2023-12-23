package kv

import (
	"context"
	"github.com/benthosdev/benthos/v4/public/service"
	"regexp"
)

func init() {
	if err := service.RegisterProcessor(
		"kv",
		newKVProcessorConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Processor, error) {
			return newKVProcessor(conf, mgr)
		},
	); err != nil {
		panic(err)
	}
}

type kvProcessor struct {
	logger *service.Logger

	pairDelimiter     string
	keyValueSeparator string
	targetField       string
}

func newKVProcessor(conf *service.ParsedConfig, mgr *service.Resources) (*kvProcessor, error) {
	logger := mgr.Logger()

	pairDelimiter, err := conf.FieldString("pair_delimiter")
	if err != nil {
		return nil, err
	}

	keyValueSeparator, err := conf.FieldString("key_value_separator")
	if err != nil {
		return nil, err
	}

	var targetField string
	if conf.Contains("target_field") {
		targetField, err = conf.FieldString("target_field")
		if err != nil {
			return nil, err
		}
	} else {
		targetField = ""
	}

	return &kvProcessor{
		logger:            logger,
		pairDelimiter:     pairDelimiter,
		keyValueSeparator: keyValueSeparator,
		targetField:       targetField,
	}, nil
}

func newKVProcessorConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Version("4.16.0").
		Summary("This processor is designed to convert messages formatted as 'foo=bar bar=baz' into a structured JSON representation.").
		Fields(
			service.NewStringField("pair_delimiter").
				Description("Regex pattern to use for splitting key-value pairs."),
			service.NewStringField("key_value_separator").
				Description("Regex pattern to use for splitting the key from the value within a key-value pairs."),
			service.NewStringField("target_field").
				Optional().
				Description("The field to insert the extracted keys into."),
		)
}

func (k kvProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	msgBytes, _ := msg.AsBytes()
	msgStr := string(msgBytes)

	pairDelimiterRegex := regexp.MustCompile(k.pairDelimiter)
	keyValueSeparatorRegex := regexp.MustCompile(k.keyValueSeparator)

	obj := make(map[string]string)
	for _, segment := range pairDelimiterRegex.Split(msgStr, -1) {
		parts := keyValueSeparatorRegex.Split(segment, 2)
		if len(parts) == 2 && segment != "" {
			key := parts[0]
			value := parts[1]
			obj[key] = value
		}
	}

	if k.targetField != "" {
		msg.SetStructured(map[string]any{k.targetField: obj})
	} else {
		msg.SetStructured(obj)
	}

	return []*service.Message{msg}, nil
}

func (k kvProcessor) Close(ctx context.Context) error {
	return nil
}
