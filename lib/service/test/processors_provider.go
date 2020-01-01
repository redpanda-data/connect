package test

import (
	"fmt"
	"os"

	"github.com/Jeffail/benthos/v3/lib/config"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/types"
	yaml "gopkg.in/yaml.v3"
)

//------------------------------------------------------------------------------

type cachedConfig struct {
	mgr   manager.Config
	procs []processor.Config
}

// ProcessorsProvider consumes a Benthos config and, given a JSON Pointer,
// extracts and constructs the target processors from the config file.
type ProcessorsProvider struct {
	targetPath    string
	cachedConfigs map[string]cachedConfig
}

// NewProcessorsProvider returns a new processors provider aimed at a filepath.
func NewProcessorsProvider(targetPath string) *ProcessorsProvider {
	return &ProcessorsProvider{
		targetPath:    targetPath,
		cachedConfigs: map[string]cachedConfig{},
	}
}

//------------------------------------------------------------------------------

// Provide attempts to extract an array of processors from a Benthos config. If
// the JSON Pointer targets a single processor config it will be constructed and
// returned as an array of one element.
func (p *ProcessorsProvider) Provide(jsonPtr string, environment map[string]string) ([]types.Processor, error) {
	confs, err := p.getConfs(jsonPtr, environment)
	if err != nil {
		return nil, err
	}
	return p.initProcs(confs)
}

//------------------------------------------------------------------------------

func (p *ProcessorsProvider) initProcs(confs cachedConfig) ([]types.Processor, error) {
	mgr, err := manager.New(confs.mgr, types.NoopMgr(), log.Noop(), metrics.Noop())
	if err != nil {
		return nil, fmt.Errorf("failed to initialise resources: %v", err)
	}

	procs := make([]types.Processor, len(confs.procs))
	for i, conf := range confs.procs {
		if procs[i], err = processor.New(conf, mgr, log.Noop(), metrics.Noop()); err != nil {
			return nil, fmt.Errorf("failed to initialise processor index '%v': %v", i, err)
		}
	}
	return procs, nil
}

func confTargetID(jsonPtr string, environment map[string]string) string {
	return fmt.Sprintf("%v-%v", jsonPtr, environment)
}

func (p *ProcessorsProvider) getConfs(jsonPtr string, environment map[string]string) (cachedConfig, error) {
	cacheKey := confTargetID(jsonPtr, environment)

	confs, exists := p.cachedConfigs[cacheKey]
	if exists {
		return confs, nil
	}

	// Set custom environment vars.
	ogEnvVars := map[string]string{}
	if environment != nil {
		for k, v := range environment {
			ogEnvVars[k] = os.Getenv(k)
			os.Setenv(k, v)
		}
	}

	// Reset env vars back to original values after config parse.
	defer func() {
		for k, v := range ogEnvVars {
			os.Setenv(k, v)
		}
	}()

	configBytes, err := config.ReadWithJSONPointers(p.targetPath, true)
	if err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", p.targetPath, err)
	}

	mgrWrapper := struct {
		Manager manager.Config `yaml:"resources"`
	}{
		Manager: manager.NewConfig(),
	}
	if err = yaml.Unmarshal(configBytes, &mgrWrapper); err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", p.targetPath, err)
	}
	confs.mgr = mgrWrapper.Manager

	var root interface{}
	if err = yaml.Unmarshal(configBytes, &root); err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", p.targetPath, err)
	}

	var procs interface{}
	if procs, err = config.JSONPointer(jsonPtr, root); err != nil {
		return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", p.targetPath, err)
	}

	var rawBytes []byte
	if rawBytes, err = yaml.Marshal(procs); err != nil {
		return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", p.targetPath, err)
	}

	switch procs.(type) {
	case []interface{}:
		if err = yaml.Unmarshal(rawBytes, &confs.procs); err != nil {
			return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", p.targetPath, err)
		}
	default:
		var procConf processor.Config
		if err = yaml.Unmarshal(rawBytes, &procConf); err != nil {
			return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", p.targetPath, err)
		}
		confs.procs = append(confs.procs, procConf)
	}

	p.cachedConfigs[cacheKey] = confs
	return confs, nil
}

//------------------------------------------------------------------------------
