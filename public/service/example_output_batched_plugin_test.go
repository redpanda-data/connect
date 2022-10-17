package service_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/benthosdev/benthos/v4/public/service"

	// Import only pure Benthos components, switch with `components/all` for all
	// standard components.
	_ "github.com/benthosdev/benthos/v4/public/components/pure"
)

type batchOfJSONWriter struct{}

func (b *batchOfJSONWriter) Connect(ctx context.Context) error {
	return nil
}

func (b *batchOfJSONWriter) WriteBatch(ctx context.Context, msgs service.MessageBatch) error {
	var messageObjs []any
	for _, msg := range msgs {
		msgObj, err := msg.AsStructured()
		if err != nil {
			return err
		}
		messageObjs = append(messageObjs, msgObj)
	}
	outBytes, err := json.Marshal(map[string]any{
		"count":   len(msgs),
		"objects": messageObjs,
	})
	if err != nil {
		return err
	}
	fmt.Println(string(outBytes))
	return nil
}

func (b *batchOfJSONWriter) Close(ctx context.Context) error {
	return nil
}

// This example demonstrates how to create a batched output plugin, which allows
// us to specify a batching mechanism and implement an interface that writes a
// batch of messages in one call.
func Example_outputBatchedPlugin() {
	spec := service.NewConfigSpec().
		Field(service.NewBatchPolicyField("batching"))

	// Register our new output, which doesn't require a config schema.
	err := service.RegisterBatchOutput(
		"batched_json_stdout", spec,
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, policy service.BatchPolicy, maxInFlight int, err error) {
			if policy, err = conf.FieldBatchPolicy("batching"); err != nil {
				return
			}
			maxInFlight = 1
			out = &batchOfJSONWriter{}
			return
		})
	if err != nil {
		panic(err)
	}

	// Use the stream builder API to create a Benthos stream that uses our new
	// output type.
	builder := service.NewStreamBuilder()

	// Set the full Benthos configuration of the stream.
	err = builder.SetYAML(`
input:
  generate:
    count: 5
    interval: 1ms
    mapping: |
      root.id = count("batched output example messages")
      root.text = "some stuff"

output:
  batched_json_stdout:
    batching:
      count: 5
`)
	if err != nil {
		panic(err)
	}

	// Build a stream with our configured components.
	stream, err := builder.Build()
	if err != nil {
		panic(err)
	}

	// And run it, blocking until it gracefully terminates once the generate
	// input has generated a message and it has flushed through the stream.
	if err = stream.Run(context.Background()); err != nil {
		panic(err)
	}

	// Output: {"count":5,"objects":[{"id":1,"text":"some stuff"},{"id":2,"text":"some stuff"},{"id":3,"text":"some stuff"},{"id":4,"text":"some stuff"},{"id":5,"text":"some stuff"}]}
}
