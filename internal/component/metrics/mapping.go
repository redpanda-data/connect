package metrics

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
)

// Mapping is a compiled Bloblang mapping used to rewrite metrics.
type Mapping struct {
	m      *mapping.Executor
	logger log.Modular
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
	return &Mapping{m, logger}, nil
}

func (m *Mapping) mapPath(path string, labelNames, labelValues []string) (outPath string, outLabelNames, outLabelValues []string) {
	if m == nil || m.m == nil {
		return path, labelNames, labelValues
	}

	part := message.NewPart(nil)
	if err := part.SetJSON(path); err != nil {
		m.logger.Errorf("Failed to apply path mapping on '%v': %v\n", path, err)
		return path, labelNames, labelValues
	}
	for i, v := range labelNames {
		part.MetaSet(v, labelValues[i])
	}
	msg := message.QuickBatch(nil)
	msg.Append(part)

	outPart := part.Copy()

	var input interface{} = path
	vars := map[string]interface{}{}

	var v interface{} = query.Nothing(nil)
	if err := m.m.ExecOnto(query.FunctionContext{
		Maps:     m.m.Maps(),
		Vars:     vars,
		MsgBatch: msg,
		NewMsg:   outPart,
	}.WithValue(input), mapping.AssignmentContext{
		Vars:  vars,
		Msg:   outPart,
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
