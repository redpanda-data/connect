package pure

import (
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/public/service"
)

func batchedInputConfig() *service.ConfigSpec {
	spec := service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary("Consumes data from a child input and applies a batching policy to the stream.").
		Description(`Batching at the input level is sometimes useful for processing across micro-batches, and can also sometimes be a useful performance trick. However, most inputs are fine without it so unless you have a specific plan for batching this component is not worth using.`).
		Field(service.NewInputField("child").Description("The child input.")).
		Field(service.NewBatchPolicyField("policy")).
		Version("4.11.0")
	return spec
}

func init() {
	err := service.RegisterBatchInput(
		"batched", batchedInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			child, err := conf.FieldInput("child")
			if err != nil {
				return nil, err
			}

			batcherPol, err := conf.FieldBatchPolicy("policy")
			if err != nil {
				return nil, err
			}

			batcher, err := batcherPol.NewBatcher(mgr)
			if err != nil {
				return nil, err
			}

			child = child.BatchedWith(batcher)
			sChild := interop.UnwrapOwnedInput(child)
			return interop.NewUnwrapInternalInput(sChild), nil
		})
	if err != nil {
		panic(err)
	}
}
