package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"gopkg.in/yaml.v3"
)

// BatchPolicy describes the mechanisms by which batching should be performed of
// messages destined for a Batch output. This is returned by constructors of
// batch outputs.
type BatchPolicy struct {
	ByteSize int
	Count    int
	Check    string
	Period   string

	// Only available when using NewBatchPolicyField.
	procs []processor.Config
}

func (b BatchPolicy) toInternal() batch.PolicyConfig {
	batchConf := batch.NewPolicyConfig()
	batchConf.ByteSize = b.ByteSize
	batchConf.Count = b.Count
	batchConf.Check = b.Check
	batchConf.Period = b.Period
	batchConf.Processors = b.procs
	return batchConf
}

// Batcher provides a batching mechanism where messages can be added one-by-one
// with a boolean return indicating whether the batching policy has been
// triggered.
//
// Upon triggering the policy it is the responsibility of the owner of this
// batcher to call Flush, which returns all the pending messages in the batch.
//
// This batcher may contain processors that are executed during the flush,
// therefore it is important to call Close when this batcher is no longer
// required, having also called Flush if appropriate.
type Batcher struct {
	p *batch.Policy
}

// Add a message to the batch. Returns true if the batching policy has been
// triggered by this new addition, in which case Flush should be called.
func (b *Batcher) Add(msg *Message) bool {
	return b.p.Add(msg.part)
}

// UntilNext returns a duration indicating how long until the current batch
// should be flushed due to a configured period. A boolean is also returned
// indicating whether the batching policy has a timed factor, if this is false
// then the duration returned should be ignored.
func (b *Batcher) UntilNext() (time.Duration, bool) {
	t := b.p.UntilNext()
	if t >= 0 {
		return t, true
	}
	return 0, false
}

// Flush pending messages into a batch, apply any batching processors that are
// part of the batching policy, and then return the result.
func (b *Batcher) Flush(ctx context.Context) (batch MessageBatch, err error) {
	m := b.p.Flush()
	if m == nil || m.Len() == 0 {
		return
	}
	_ = m.Iter(func(i int, part *message.Part) error {
		batch = append(batch, newMessageFromPart(part))
		return nil
	})
	return
}

// Close the batching policy, which cleans up any resources used by batching
// processors.
func (b *Batcher) Close(ctx context.Context) error {
	b.p.CloseAsync()
	for {
		// Gross but will do for now until we replace these with context params.
		if err := b.p.WaitForClose(time.Millisecond * 100); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

// NewBatcher creates a batching mechanism from the policy.
func (b BatchPolicy) NewBatcher(res *Resources) (*Batcher, error) {
	p, err := batch.NewPolicy(b.toInternal(), res.mgr.IntoPath("batching"))
	if err != nil {
		return nil, err
	}
	return &Batcher{p}, nil
}

//------------------------------------------------------------------------------

// NewBatchPolicyField defines a new object type config field that describes a
// batching policy for batched outputs. It is then possible to extract a
// BatchPolicy from the resulting parsed config with the method
// FieldBatchPolicy.
func NewBatchPolicyField(name string) *ConfigField {
	bs := batch.FieldSpec()
	bs.Name = name
	bs.Type = docs.FieldTypeObject
	var newChildren []docs.FieldSpec
	for _, f := range bs.Children {
		if f.Name == "count" {
			f = f.HasDefault(0)
		}
		if !f.IsDeprecated {
			newChildren = append(newChildren, f)
		}
	}
	bs.Children = newChildren
	return &ConfigField{field: bs}
}

// FieldBatchPolicy accesses a field from a parsed config that was defined with
// NewBatchPolicyField and returns a BatchPolicy, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldBatchPolicy(path ...string) (conf BatchPolicy, err error) {
	if conf.Count, err = p.FieldInt(append(path, "count")...); err != nil {
		return conf, err
	}
	if conf.ByteSize, err = p.FieldInt(append(path, "byte_size")...); err != nil {
		return conf, err
	}
	if conf.Check, err = p.FieldString(append(path, "check")...); err != nil {
		return conf, err
	}
	if conf.Period, err = p.FieldString(append(path, "period")...); err != nil {
		return conf, err
	}

	procsNode, exists := p.field(append(path, "processors")...)
	if !exists {
		return
	}

	procsArray, ok := procsNode.([]interface{})
	if !ok {
		err = fmt.Errorf("field 'processors' returned unexpected value, expected array, got %T", procsNode)
		return
	}

	for i, iConf := range procsArray {
		node, ok := iConf.(*yaml.Node)
		if !ok {
			err = fmt.Errorf("field 'processors.%v' returned unexpected value, expected object, got %T", i, iConf)
			return
		}

		var pconf processor.Config
		if err = node.Decode(&pconf); err != nil {
			err = fmt.Errorf("field 'processors.%v': %w", i, err)
			return
		}
		conf.procs = append(conf.procs, pconf)
	}
	return
}
