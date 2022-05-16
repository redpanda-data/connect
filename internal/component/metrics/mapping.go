package metrics

import (
	"fmt"
	"sort"

	"github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/bloblang/query"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/message"
)

// Mapping is a compiled Bloblang mapping used to rewrite metrics.
type Mapping struct {
	m          *mapping.Executor
	logger     log.Modular
	staticVars map[string]interface{}
}

// NewMapping parses a Bloblang mapping and returns a metrics mapping.
func NewMapping(mapping string, logger log.Modular) (*Mapping, error) {
	if mapping == "" {
		return &Mapping{m: nil, logger: logger}, nil
	}
	m, err := bloblang.GlobalEnvironment().OnlyPure().NewMapping(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(mapping)))
		}
		return nil, err
	}
	return &Mapping{m, logger, map[string]interface{}{}}, nil
}

// WithStaticVars adds a map of key/value pairs to the static variables of the
// metrics mapping. These are variables that will be made available to each
// invocation of the metrics mapping.
func (m *Mapping) WithStaticVars(kvs map[string]interface{}) *Mapping {
	newM := *m

	newM.staticVars = map[string]interface{}{}
	for k, v := range m.staticVars {
		newM.staticVars[k] = v
	}
	for k, v := range kvs {
		newM.staticVars[k] = v
	}

	return &newM
}

func (m *Mapping) mapPath(path string, labelNames, labelValues []string) (outPath string, outLabelNames, outLabelValues []string) {
	if m == nil || m.m == nil {
		return path, labelNames, labelValues
	}

	part := message.NewPart(nil)
	part.SetJSON(path)
	for i, v := range labelNames {
		part.MetaSet(v, labelValues[i])
	}
	msg := message.QuickBatch(nil)
	msg.Append(part)

	outPart := part.Copy()

	var input interface{} = path
	vars := map[string]interface{}{}
	for k, v := range m.staticVars {
		vars[k] = v
	}

	var v interface{} = query.Nothing(nil)
	if err := m.m.ExecOnto(query.FunctionContext{
		Maps:     m.m.Maps(),
		Vars:     vars,
		MsgBatch: msg,
		NewMeta:  outPart,
		NewValue: &v,
	}.WithValue(input), mapping.AssignmentContext{
		Vars:  vars,
		Meta:  outPart,
		Value: &v,
	}); err != nil {
		m.logger.Errorf("Failed to apply path mapping on '%v': %v\n", path, err)
		return path, nil, nil
	}

	_ = outPart.MetaIter(func(k, v string) error {
		outLabelNames = append(outLabelNames, k)
		return nil
	})
	if len(outLabelNames) > 0 {
		sort.Strings(outLabelNames)
		for _, k := range outLabelNames {
			v := outPart.MetaGet(k)
			m.logger.Tracef("Metrics label '%v' created with static value '%v'.\n", k, v)
			outLabelValues = append(outLabelValues, v)
		}
	}

	switch t := v.(type) {
	case query.Delete:
		m.logger.Tracef("Deleting metrics path: %v\n", path)
		return "", nil, nil
	case query.Nothing:
		m.logger.Tracef("Metrics path '%v' registered unchanged.\n", path)
		outPath = path
		return
	case string:
		m.logger.Tracef("Updated metrics path '%v' to: %v\n", path, t)
		outPath = t
		return
	}
	m.logger.Errorf("Path mapping returned invalid result, expected string, found %T\n", v)
	return path, labelNames, labelValues
}
