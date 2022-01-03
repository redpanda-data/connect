package service

import (
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/docs"
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
