package metrics

import (
	"fmt"
	"sort"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/mapping"
	"github.com/Jeffail/benthos/v3/internal/bloblang/parser"
	"github.com/Jeffail/benthos/v3/internal/bloblang/query"
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
)

type pathMapping struct {
	m      *mapping.Executor
	logger log.Modular
}

func pathMappingDocs(allowLabels, forPrometheus bool) docs.FieldSpec {
	examples := []interface{}{
		`this.replace("input", "source").replace("output", "sink")`,
	}
	if forPrometheus {
		examples = append(examples, `if ![
  "benthos_input_received",
  "benthos_input_latency",
  "benthos_output_sent"
].contains(this) { deleted() }`)
	} else {
		examples = append(examples, `if ![
  "benthos.input.received",
  "benthos.input.latency",
  "benthos.output.sent"
].contains(this) { deleted() }`)
	}
	summary := "An optional [Bloblang mapping](/docs/guides/bloblang/about) that allows you to rename or prevent certain metrics paths from being exported. When metric paths are created, renamed and dropped a trace log is written, enabling TRACE level logging is therefore a good way to diagnose path mappings."

	if allowLabels {
		examples = append(examples, `let matches = this.re_find_all_submatch("resource_processor_([a-zA-Z]+)_(.*)")
meta processor = $matches.0.1 | deleted()
root = $matches.0.2 | deleted()`)
		summary += " BETA FEATURE: Labels can also be created for the metric path by mapping meta fields."
	}
	return docs.FieldBloblang("path_mapping", summary, examples...)
}

func newPathMapping(mapping string, logger log.Modular) (*pathMapping, error) {
	if mapping == "" {
		return &pathMapping{m: nil, logger: logger}, nil
	}
	m, err := bloblang.GlobalEnvironment().NewMapping(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(mapping)))
		}
		return nil, err
	}
	return &pathMapping{m, logger}, nil
}

func (m *pathMapping) mapPathNoTags(path string) string {
	path, _, _ = m.mapPath(path, false)
	return path
}

func (m *pathMapping) mapPathWithTags(path string) (outPath string, labelNames, labelValues []string) {
	return m.mapPath(path, true)
}

func (m *pathMapping) mapPath(path string, allowLabels bool) (outPath string, labelNames, labelValues []string) {
	if m == nil || m.m == nil {
		return path, nil, nil
	}

	var input interface{} = path
	msg := message.NewPart(nil)
	vars := map[string]interface{}{}

	var v interface{} = query.Nothing(nil)

	if err := m.m.ExecOnto(query.FunctionContext{
		Maps:     map[string]query.Function{},
		Vars:     vars,
		MsgBatch: message.QuickBatch(nil),
	}.WithValue(input), mapping.AssignmentContext{
		Vars:  vars,
		Msg:   msg,
		Value: &v,
	}); err != nil {
		m.logger.Errorf("Failed to apply path mapping on '%v': %v\n", path, err)
		return path, nil, nil
	}

	_ = msg.MetaIter(func(k, v string) error {
		labelNames = append(labelNames, k)
		return nil
	})
	if len(labelNames) > 0 && !allowLabels {
		for _, k := range labelNames {
			m.logger.Tracef("Metrics label '%v' was not created as this metrics target does not support them.\n", k)
		}
		labelNames = nil
	}
	if len(labelNames) > 0 {
		sort.Strings(labelNames)
		for _, k := range labelNames {
			v := msg.MetaGet(k)
			m.logger.Tracef("Metrics label '%v' created with static value '%v'.\n", k, v)
			labelValues = append(labelValues, v)
		}
	}

	switch t := v.(type) {
	case query.Delete:
		m.logger.Tracef("Deleting metrics path: %v\n", path)
		return "", nil, nil
	case query.Nothing:
		m.logger.Tracef("Metrics path '%v' registered unchanged.\n", path)
		return path, labelNames, labelValues
	case string:
		m.logger.Tracef("Updated metrics path '%v' to: %v\n", path, t)
		return t, labelNames, labelValues
	}
	m.logger.Errorf("Path mapping returned invalid result, expected string, found %T\n", v)
	return path, labelNames, labelValues
}
