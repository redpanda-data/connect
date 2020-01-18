// +build !wasm

package buffer

import (
	"github.com/Jeffail/benthos/v3/lib/buffer/parallel"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
)

//------------------------------------------------------------------------------

/*
func init() {
	Constructors[TypeBolt] = TypeSpec{
		constructor: NewBolt,
		Description: `
EXPERIMENTAL: This buffer is considered experimental and subject to change
outside of major version releases.

Buffers messages within a disk backed [BoltDB](https://github.com/boltdb/bolt)
store.

### Batching

It is possible to batch up messages sent from this buffer using a batch policy.` + batch.PolicyDoc + `

This is a more powerful way of batching messages than the
` + "[`batch`](/docs/components/processors/batch)" + ` processor, as it does not
rely on new messages entering the pipeline in order to trigger the conditions.`,
		sanitiseConfigFunc: func(conf Config) (interface{}, error) {
			bSanit, err := batch.SanitisePolicyConfig(batch.PolicyConfig(conf.Bolt.BatchPolicy.PolicyConfig))
			if err != nil {
				return nil, err
			}
			if bSanitObj, ok := bSanit.(map[string]interface{}); ok {
				bSanitObj["enabled"] = conf.Bolt.BatchPolicy.Enabled
			}
			return map[string]interface{}{
				"file":           conf.Bolt.BoltDBConfig.File,
				"prefetch_count": conf.Bolt.BoltDBConfig.PrefetchCount,
				"batch_policy":   bSanit,
			}, nil
		},
	}
}
*/

//------------------------------------------------------------------------------

// BoltConfig contains configuration parameters for a BoltDB backed buffer.
type BoltConfig struct {
	parallel.BoltDBConfig `json:",inline" yaml:",inline"`
	BatchPolicy           EnabledBatchPolicyConfig `json:"batch_policy" yaml:"batch_policy"`
}

// NewBoltConfig creates a new BoltConfig with default values.
func NewBoltConfig() BoltConfig {
	return BoltConfig{
		BoltDBConfig: parallel.NewBoltDBConfig(),
		BatchPolicy: EnabledBatchPolicyConfig{
			Enabled:      false,
			PolicyConfig: batch.NewPolicyConfig(),
		},
	}
}

//------------------------------------------------------------------------------

/*
// NewBolt creates a buffer backed with BoltDB.
func NewBolt(config Config, log log.Modular, stats metrics.Type) (Type, error) {
	buf, err := parallel.NewBoltDB(config.Bolt.BoltDBConfig)
	if err != nil {
		return nil, err
	}
	wrap := NewParallelWrapper(config, buf, log, stats)
	if !config.Bolt.BatchPolicy.Enabled {
		return wrap, nil
	}
	pol, err := batch.NewPolicy(config.Bolt.BatchPolicy.PolicyConfig, types.NoopMgr(), log, stats)
	if err != nil {
		return nil, fmt.Errorf("batch policy config error: %v", err)
	}
	return NewParallelBatcher(pol, wrap, log, stats), nil
}
*/

//------------------------------------------------------------------------------
