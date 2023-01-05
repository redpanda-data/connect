package service

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewInputField defines a new input field, it is then possible to extract an
// OwnedInput from the resulting parsed config with the method FieldInput.
func NewInputField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldInput(name, ""),
	}
}

// FieldInput accesses a field from a parsed config that was defined with
// NewInputField and returns an OwnedInput, or an error if the configuration was
// invalid.
func (p *ParsedConfig) FieldInput(path ...string) (*OwnedInput, error) {
	field, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	pNode, ok := field.(*yaml.Node)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected object, got %T", field)
	}

	var conf input.Config
	if err := pNode.Decode(&conf); err != nil {
		return nil, err
	}

	iproc, err := p.mgr.IntoPath(path...).NewInput(conf)
	if err != nil {
		return nil, err
	}
	return &OwnedInput{iproc}, nil
}

// NewInputListField defines a new input list field, it is then possible
// to extract a list of OwnedInput from the resulting parsed config with the
// method FieldInputList.
func NewInputListField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldInput(name, "").Array(),
	}
}

// FieldInputList accesses a field from a parsed config that was defined
// with NewInputListField and returns a slice of OwnedInput, or an error
// if the configuration was invalid.
func (p *ParsedConfig) FieldInputList(path ...string) ([]*OwnedInput, error) {
	field, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	fieldArray, ok := field.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected array, got %T", field)
	}

	var configs []input.Config
	for i, iConf := range fieldArray {
		node, ok := iConf.(*yaml.Node)
		if !ok {
			return nil, fmt.Errorf("value %v returned unexpected value, expected object, got %T", i, iConf)
		}

		var conf input.Config
		if err := node.Decode(&conf); err != nil {
			return nil, fmt.Errorf("value %v: %w", i, err)
		}
		configs = append(configs, conf)
	}

	tmpMgr := p.mgr.IntoPath(path...)
	ins := make([]*OwnedInput, len(configs))
	for i, c := range configs {
		iproc, err := tmpMgr.IntoPath(strconv.Itoa(i)).NewInput(c)
		if err != nil {
			return nil, fmt.Errorf("input %v: %w", i, err)
		}
		ins[i] = &OwnedInput{iproc}
	}

	return ins, nil
}
