package metrics

import (
	"fmt"

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

func pathMappingDocs() docs.FieldSpec {
	return docs.FieldCommon(
		"path_mapping",
		"An optional [Bloblang mapping](/docs/guides/bloblang/about) that allows you to rename or prevent certain metrics paths from being exported.",
		`this.replace("input", "source").replace("output", "sink")`,
		`if ![
  "benthos_input_received",
  "benthos_input_latency",
  "benthos_output_sent"
].contains(this) { deleted() }`,
	)
}

func newPathMapping(mapping string, logger log.Modular) (*pathMapping, error) {
	if len(mapping) == 0 {
		return &pathMapping{m: nil, logger: logger}, nil
	}
	m, err := bloblang.NewMapping("", mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			return nil, fmt.Errorf("%v", perr.ErrorAtPosition([]rune(mapping)))
		}
		return nil, err
	}
	return &pathMapping{m, logger}, nil
}

func (m *pathMapping) mapPath(path string) string {
	if m == nil || m.m == nil {
		return path
	}
	var input interface{} = path
	v, err := m.m.Exec(query.FunctionContext{
		Value:    &input,
		Maps:     map[string]query.Function{},
		Vars:     map[string]interface{}{},
		MsgBatch: message.New(nil),
	})
	if err != nil {
		m.logger.Errorf("Failed to apply path mapping on '%v': %v\n", path, err)
		return path
	}
	switch t := v.(type) {
	case query.Delete:
		m.logger.Tracef("Deleting metrics path: %v\n", path)
		return ""
	case query.Nothing:
		m.logger.Tracef("Metrics path '%v' registered unchanged.\n", path)
		return path
	case string:
		m.logger.Tracef("Updated metrics path '%v' to: %v\n", path, t)
		return t
	}
	m.logger.Errorf("Path mapping returned invalid result, expected string, found %T\n", v)
	return path
}
