package service

import (
	"fmt"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/docs"
)

// NewProcessorField defines a new processor field, it is then possible to
// extract an OwnedProcessor from the resulting parsed config with the method
// FieldProcessor.
func NewProcessorField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldProcessor(name, ""),
	}
}

func procConfigFromAny(v any) (conf processor.Config, err error) {
	pNode, ok := v.(*yaml.Node)
	if !ok {
		err = fmt.Errorf("unexpected value, expected object, got %T", v)
		return
	}

	err = pNode.Decode(&conf)
	return
}

// FieldProcessor accesses a field from a parsed config that was defined with
// NewProcessorField and returns an OwnedProcessor, or an error if the
// configuration was invalid.
func (p *ParsedConfig) FieldProcessor(path ...string) (*OwnedProcessor, error) {
	v, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}
	procConf, err := procConfigFromAny(v)
	if err != nil {
		return nil, err
	}
	iproc, err := p.mgr.IntoPath(path...).NewProcessor(procConf)
	if err != nil {
		return nil, err
	}
	return &OwnedProcessor{iproc}, nil
}

// NewProcessorListField defines a new processor list field, it is then possible
// to extract a list of OwnedProcessor from the resulting parsed config with the
// method FieldProcessorList.
func NewProcessorListField(name string) *ConfigField {
	return &ConfigField{
		field: docs.FieldProcessor(name, "").Array(),
	}
}

func (p *ParsedConfig) fieldProcessorListConfigs(path ...string) ([]processor.Config, error) {
	proc, exists := p.field(path...)
	if !exists {
		return nil, fmt.Errorf("field '%v' was not found in the config", strings.Join(path, "."))
	}

	procsArray, ok := proc.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected value, expected array, got %T", proc)
	}

	var procConfigs []processor.Config
	for i, iConf := range procsArray {
		pconf, err := procConfigFromAny(iConf)
		if err != nil {
			return nil, fmt.Errorf("value %v: %w", i, err)
		}
		procConfigs = append(procConfigs, pconf)
	}
	return procConfigs, nil
}

// FieldProcessorList accesses a field from a parsed config that was defined
// with NewProcessorListField and returns a slice of OwnedProcessor, or an error
// if the configuration was invalid.
func (p *ParsedConfig) FieldProcessorList(path ...string) ([]*OwnedProcessor, error) {
	procConfigs, err := p.fieldProcessorListConfigs(path...)
	if err != nil {
		return nil, err
	}

	tmpMgr := p.mgr.IntoPath(path...)
	procs := make([]*OwnedProcessor, len(procConfigs))
	for i, c := range procConfigs {
		iproc, err := tmpMgr.IntoPath(strconv.Itoa(i)).NewProcessor(c)
		if err != nil {
			return nil, fmt.Errorf("processor %v: %w", i, err)
		}
		procs[i] = &OwnedProcessor{iproc}
	}

	return procs, nil
}
