package service

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewOutputField defines a new output field, it is then possible to extract an
// OwnedOutput from the resulting parsed config with the method FieldOutput.
func NewOutputField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldOutput(name, ""),
	}
}

// FieldOutput accesses a field from a parsed config that was defined with
// NewOutputField and returns an OwnedOutput, or an error if the configuration
// was invalid.
func (p *ParsedConfig) FieldOutput(path ...string) (*OwnedOutput, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	conf, err := output.FromAny(p.mgr.Environment(), field)
	if err != nil {
		return nil, err
	}

	iproc, err := p.mgr.IntoPath(path...).NewOutput(conf)
	if err != nil {
		return nil, err
	}
	return newOwnedOutput(iproc)
}

// NewOutputListField defines a new output list field, it is then possible
// to extract a list of OwnedOutput from the resulting parsed config with the
// method FieldOutputList.
func NewOutputListField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldOutput(name, "").Array(),
	}
}

// FieldOutputList accesses a field from a parsed config that was defined
// with NewOutputListField and returns a slice of OwnedOutput, or an error
// if the configuration was invalid.
func (p *ParsedConfig) FieldOutputList(path ...string) ([]*OwnedOutput, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	fieldArray, ok := field.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected array, got %T", field)
	}

	var configs []output.Config
	for i, iConf := range fieldArray {
		conf, err := output.FromAny(p.mgr.Environment(), iConf)
		if err != nil {
			return nil, fmt.Errorf("value %v: %w", i, err)
		}
		configs = append(configs, conf)
	}

	tmpMgr := p.mgr.IntoPath(path...)
	ins := make([]*OwnedOutput, len(configs))
	for i, c := range configs {
		iproc, err := tmpMgr.IntoPath(strconv.Itoa(i)).NewOutput(c)
		if err != nil {
			return nil, fmt.Errorf("output %v: %w", i, err)
		}
		if ins[i], err = newOwnedOutput(iproc); err != nil {
			return nil, fmt.Errorf("output %v: %w", i, err)
		}
	}

	return ins, nil
}

// NewOutputMapField defines a new output list field, it is then possible
// to extract a map of OwnedOutput from the resulting parsed config with the
// method FieldOutputMap.
func NewOutputMapField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldOutput(name, "").Map(),
	}
}

// FieldOutputMap accesses a field from a parsed config that was defined
// with NewOutputMapField and returns a map of OwnedOutput, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldOutputMap(path ...string) (map[string]*OwnedOutput, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	fieldMap, ok := field.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected object, got %T", field)
	}

	tmpMgr := p.mgr.IntoPath(path...)
	outs := make(map[string]*OwnedOutput, len(fieldMap))
	for k, v := range fieldMap {
		conf, err := output.FromAny(p.mgr.Environment(), v)
		if err != nil {
			return nil, fmt.Errorf("value %v: %w", k, err)
		}

		iproc, err := tmpMgr.IntoPath(k).NewOutput(conf)
		if err != nil {
			return nil, fmt.Errorf("output %v: %w", k, err)
		}
		if outs[k], err = newOwnedOutput(iproc); err != nil {
			return nil, fmt.Errorf("output %v: %w", k, err)
		}
	}

	return outs, nil
}
