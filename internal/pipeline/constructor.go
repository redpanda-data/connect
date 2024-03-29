package pipeline

import (
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/value"
)

var threadsField = docs.FieldInt("threads", "The number of threads to execute processing pipelines across.").HasDefault(-1)

func ConfigSpec() docs.FieldSpec {
	return docs.FieldObject(
		"pipeline", "Describes optional processing pipelines used for mutating messages.",
	).WithChildren(
		threadsField,
		docs.FieldProcessor("processors", "A list of processors to apply to messages.").Array().HasDefault([]any{}),
	)
}

// Config is a configuration struct for creating parallel processing pipelines.
// The number of resuling parallel processing pipelines will match the number of
// threads specified. Processors are executed on each message in the order that
// they are written.
//
// In order to fully utilise each processing thread you must either have a
// number of parallel inputs that matches or surpasses the number of pipeline
// threads, or use a memory buffer.
type Config struct {
	Threads    int                `json:"threads"    yaml:"threads"`
	Processors []processor.Config `json:"processors" yaml:"processors"`
}

// NewConfig returns a configuration struct fully populated with default values.
func NewConfig() Config {
	return Config{
		Threads:    -1,
		Processors: []processor.Config{},
	}
}

//------------------------------------------------------------------------------

// New creates an input type based on an input configuration.
func New(conf Config, mgr bundle.NewManagement) (processor.Pipeline, error) {
	processors := make([]processor.V1, len(conf.Processors))
	for j, procConf := range conf.Processors {
		var err error
		pMgr := mgr.IntoPath("processors", strconv.Itoa(j))
		processors[j], err = pMgr.NewProcessor(procConf)
		if err != nil {
			return nil, err
		}
	}
	if conf.Threads == 1 {
		return NewProcessor(processors...), nil
	}
	return NewPool(conf.Threads, mgr.Logger(), processors...)
}

func FromAny(prov docs.Provider, value any) (conf Config, err error) {
	switch t := value.(type) {
	case Config:
		return t, nil
	case *yaml.Node:
		return fromYAML(prov, t)
	case map[string]any:
		return fromMap(prov, t)
	}
	err = fmt.Errorf("unexpected value, expected object, got %T", value)
	return
}

func fromMap(prov docs.Provider, val map[string]any) (conf Config, err error) {
	conf = NewConfig()

	if threadsV, exists := val["threads"]; exists {
		var threads64 int64
		if threads64, err = value.IGetInt(threadsV); err != nil {
			return
		}
		conf.Threads = int(threads64)
	}

	if procVs, ok := val["processors"].([]any); ok {
		for _, iv := range procVs {
			var tmpProc processor.Config
			if tmpProc, err = processor.FromAny(prov, iv); err != nil {
				return
			}
			conf.Processors = append(conf.Processors, tmpProc)
		}
	}
	return
}

func fromYAML(prov docs.Provider, val *yaml.Node) (conf Config, err error) {
	conf = NewConfig()
	for i := 0; i < len(val.Content)-1; i += 2 {
		switch val.Content[i].Value {
		case "threads":
			if err = val.Content[i+1].Decode(&conf.Threads); err != nil {
				return
			}
		case "processors":
			node := val.Content[i+1]
			if node.Kind != yaml.SequenceNode {
				err = fmt.Errorf("line %v: expected array value, got %v", node.Line, node.Kind)
				return
			}
			for _, pNode := range node.Content {
				var tmpProc processor.Config
				if tmpProc, err = processor.FromAny(prov, pNode); err != nil {
					return
				}
				conf.Processors = append(conf.Processors, tmpProc)
			}
		}
	}
	return
}
