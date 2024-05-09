package kv

import (
	"context"
	"fmt"
	"github.com/benthosdev/benthos/v4/public/service"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestKVProcessor(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		pairDelimiter     string
		keyValueSeparator string
		output            map[string]string
	}{
		{
			name:              "simple log with key-value pairs",
			input:             "level=info msg=test-message",
			pairDelimiter:     "\\s",
			keyValueSeparator: "=",
			output: map[string]string{
				"level": "info",
				"msg":   "test-message",
			},
		},
		{
			name:              "input with multiple field splits",
			input:             "key1=value1||key2=value2|key3=value3",
			pairDelimiter:     "\\|",
			keyValueSeparator: "=",
			output: map[string]string{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			},
		},
		{
			name:              "input with empty values",
			input:             "key1=|key2=value2|key3=",
			pairDelimiter:     "\\|",
			keyValueSeparator: "=",
			output: map[string]string{
				"key1": "",
				"key2": "value2",
				"key3": "",
			},
		},
		{
			name:              "empty pair and key-value delimiter",
			input:             "key1,val1 key2,value2 key3,val3",
			pairDelimiter:     "\\|",
			keyValueSeparator: "=",
			output: map[string]string{
				"original_message": "key1,val1 key2,value2 key3,val3",
			},
		},
	}

	for _, test := range tests {
		spec := newKVProcessorConfig()
		conf, err := spec.ParseYAML(fmt.Sprintf(`
  pair_delimiter: %v
  key_value_separator: %v
  `, test.pairDelimiter, test.keyValueSeparator), service.GlobalEnvironment())

		require.NoError(t, err, "failed to parse test config")

		proc, err := newKVProcessor(conf, service.MockResources())
		require.NoError(t, err, "failed to create processor")

		t.Run(test.name, func(tt *testing.T) {
			msg := service.NewMessage([]byte(test.input))
			msgsOut, err := proc.Process(context.Background(), msg)

			require.NoError(t, err, "failed to process message")
			require.Len(t, msgsOut, 1, "wrong batch size received")

			structured, err := msgsOut[0].AsStructured()
			require.NoError(t, err, "failed to parse message as structured")
			if exp, act := test.output, structured; !reflect.DeepEqual(exp, act) {
				tt.Errorf("Wrong result: %v != %v", exp, act)
			}
		})
	}
}

func TestKVProcessorWithTargetField(t *testing.T) {
	spec := newKVProcessorConfig()
	conf, err := spec.ParseYAML(`
  pair_delimiter: \s
  key_value_separator: =
  target_field: extracted
  `, service.GlobalEnvironment())
	require.NoError(t, err, "failed to parse test config")

	proc, err := newKVProcessor(conf, service.MockResources())
	require.NoError(t, err, "failed to create processor")

	exp := map[string]any{
		"extracted": map[string]string{
			"level": "info",
			"msg":   "test-message",
		},
	}

	msg := service.NewMessage([]byte("level=info msg=test-message"))
	msgsOut, err := proc.Process(context.Background(), msg)

	require.NoError(t, err, "failed to process message")
	require.Len(t, msgsOut, 1, "wrong batch size received")
	act, err := msgsOut[0].AsStructured()
	require.NoError(t, err, "failed to parse message as structured")
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %v != %v", exp, act)
	}
}
