package buffer

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/lib/buffer/parallel"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeMemory] = TypeSpec{
		constructor: NewMemory,
		description: `
The memory buffer stores messages in RAM. During shutdown Benthos will make a
best attempt at flushing all remaining messages before exiting cleanly.

This buffer has a configurable limit, where consumption will be stopped with
back pressure upstream if the total size of messages in the buffer reaches this
amount. Since this calculation is only an estimate, and the real size of
messages in RAM is always higher, it is recommended to set the limit
significantly below the amount of RAM available.

### Batching

It is possible to batch up messages sent from this buffer using a
[batch policy](../batching.md#batch-policy).

This is a more powerful way of batching messages than the
` + "[`batch`](../processors/README.md#batch)" + ` processor, as it does not
rely on new messages entering the pipeline in order to trigger the conditions.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			bSanit, err := batch.SanitisePolicyConfig(batch.PolicyConfig(conf.Memory.BatchPolicy.PolicyConfig))
			if err != nil {
				return nil, err
			}
			if bSanitObj, ok := bSanit.(map[string]interface{}); ok {
				bSanitObj["enabled"] = conf.Memory.BatchPolicy.Enabled
			}
			return map[string]interface{}{
				"limit":        conf.Memory.Limit,
				"batch_policy": bSanit,
			}, nil
		},
	}
}

//------------------------------------------------------------------------------

// EnabledBatchPolicyConfig is a batch.PolicyConfig with an enable field.
type EnabledBatchPolicyConfig struct {
	Enabled            bool `json:"enabled" yaml:"enabled"`
	batch.PolicyConfig `json:",inline" yaml:",inline"`
}

// MemoryConfig is config values for a purely memory based ring buffer type.
type MemoryConfig struct {
	Limit       int                      `json:"limit" yaml:"limit"`
	BatchPolicy EnabledBatchPolicyConfig `json:"batch_policy" yaml:"batch_policy"`
}

// NewMemoryConfig creates a new MemoryConfig with default values.
func NewMemoryConfig() MemoryConfig {
	return MemoryConfig{
		Limit: 1024 * 1024 * 500, // 500MB
		BatchPolicy: EnabledBatchPolicyConfig{
			Enabled:      false,
			PolicyConfig: batch.NewPolicyConfig(),
		},
	}
}

//------------------------------------------------------------------------------

// NewMemory creates a buffer held in memory.
func NewMemory(config Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	wrap := NewParallelWrapper(config, parallel.NewMemory(config.Memory.Limit), log, stats)
	if !config.Memory.BatchPolicy.Enabled {
		return wrap, nil
	}
	pol, err := batch.NewPolicy(config.Memory.BatchPolicy.PolicyConfig, mgr, log, stats)
	if err != nil {
		return nil, fmt.Errorf("batch policy config error: %v", err)
	}
	return NewParallelBatcher(pol, wrap, log, stats), nil
}

//------------------------------------------------------------------------------
