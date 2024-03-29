package service

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

const AutoRetryNacksToggleFieldName = "auto_replay_nacks"

// NewAutoRetryNacksToggleField creates a configuration field for toggling the
// behaviour of an input where nacks (rejections) of data results in the
// automatic replay of that data (the default). This field should be used for
// conditionally wrapping inputs with AutoRetryNacksToggled or
// AutoRetryNacksBatchedToggled.
func NewAutoRetryNacksToggleField() *ConfigField {
	return NewBoolField(AutoRetryNacksToggleFieldName).
		Description("Whether messages that are rejected (nacked) at the output level should be automatically replayed indefinitely, eventually resulting in back pressure if the cause of the rejections is persistent. If set to `false` these messages will instead be deleted. Disabling auto replays can greatly improve memory efficiency of high throughput streams as the original shape of the data can be discarded immediately upon consumption and mutation.").
		Default(true)
}

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
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	conf, err := input.FromAny(p.mgr.Environment(), field)
	if err != nil {
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
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	fieldArray, ok := field.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected array, got %T", field)
	}

	var configs []input.Config
	for i, iConf := range fieldArray {
		conf, err := input.FromAny(p.mgr.Environment(), iConf)
		if err != nil {
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

// NewInputMapField defines a new input map field, it is then possible to
// extract a map of OwnedInput from the resulting parsed config with the
// method FieldInputMap.
func NewInputMapField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldInput(name, "").Map(),
	}
}

// FieldInputMap accesses a field from a parsed config that was defined
// with NewInputMapField and returns a map of OwnedInput, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldInputMap(path ...string) (map[string]*OwnedInput, error) {
	field, exists := p.i.Field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	fieldMap, ok := field.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected object, got %T", field)
	}

	tmpMgr := p.mgr.IntoPath(path...)
	ins := make(map[string]*OwnedInput, len(fieldMap))
	for k, v := range fieldMap {
		conf, err := input.FromAny(p.mgr.Environment(), v)
		if err != nil {
			return nil, fmt.Errorf("value %v: %w", k, err)
		}

		iproc, err := tmpMgr.IntoPath(k).NewInput(conf)
		if err != nil {
			return nil, fmt.Errorf("input %v: %w", k, err)
		}
		ins[k] = &OwnedInput{iproc}
	}

	return ins, nil
}
