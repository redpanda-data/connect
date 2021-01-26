package test

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

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
	targetPath     string
	resourcesPaths []string
	cachedConfigs  map[string]cachedConfig

	logger log.Modular
}

// NewProcessorsProvider returns a new processors provider aimed at a filepath.
func NewProcessorsProvider(targetPath string, opts ...func(*ProcessorsProvider)) *ProcessorsProvider {
	p := &ProcessorsProvider{
		targetPath:    targetPath,
		cachedConfigs: map[string]cachedConfig{},
		logger:        log.Noop(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

//------------------------------------------------------------------------------

// OptAddResourcesPaths adds paths to files where resources should be parsed.
func OptAddResourcesPaths(paths []string) func(*ProcessorsProvider) {
	return func(p *ProcessorsProvider) {
		p.resourcesPaths = paths
	}
}

// OptProcessorsProviderSetLogger sets the logger used by tested components.
func OptProcessorsProviderSetLogger(logger log.Modular) func(*ProcessorsProvider) {
	return func(p *ProcessorsProvider) {
		p.logger = logger
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
	mgr, err := manager.New(confs.mgr, types.NoopMgr(), p.logger, metrics.Noop())
	if err != nil {
		return nil, fmt.Errorf("failed to initialise resources: %v", err)
	}

	procs := make([]types.Processor, len(confs.procs))
	for i, conf := range confs.procs {
		if procs[i], err = processor.New(conf, mgr, p.logger, metrics.Noop()); err != nil {
			return nil, fmt.Errorf("failed to initialise processor index '%v': %v", i, err)
		}
	}
	return procs, nil
}

func confTargetID(jsonPtr string, environment map[string]string) string {
	return fmt.Sprintf("%v-%v", jsonPtr, environment)
}

func setEnvironment(vars map[string]string) func() {
	if vars == nil {
		return func() {}
	}

	// Set custom environment vars.
	ogEnvVars := map[string]string{}
	for k, v := range vars {
		if ogV, exists := os.LookupEnv(k); exists {
			ogEnvVars[k] = ogV
		}
		os.Setenv(k, v)
	}

	// Reset env vars back to original values after config parse.
	return func() {
		for k := range vars {
			if og, exists := ogEnvVars[k]; exists {
				os.Setenv(k, og)
			} else {
				os.Unsetenv(k)
			}
		}
	}
}

func resolveProcessorsPointer(targetFile string, jsonPtr string) (filePath, procPath string, err error) {
	var u *url.URL
	if u, err = url.Parse(jsonPtr); err != nil {
		return
	}
	if u.Scheme != "" && u.Scheme != "file" {
		err = fmt.Errorf("target processors '%v' contains non-path scheme value", jsonPtr)
		return
	}

	if len(u.Fragment) > 0 {
		procPath = u.Fragment
		filePath = filepath.Join(filepath.Dir(targetFile), u.Path)
	} else {
		procPath = u.Path
		filePath = targetFile
	}
	if len(procPath) == 0 {
		err = fmt.Errorf("failed to target processors '%v': reference URI must contain a path or fragment", jsonPtr)
	}
	return
}

func (p *ProcessorsProvider) getConfs(jsonPtr string, environment map[string]string) (cachedConfig, error) {
	cacheKey := confTargetID(jsonPtr, environment)

	confs, exists := p.cachedConfigs[cacheKey]
	if exists {
		return confs, nil
	}

	targetPath, procPath, err := resolveProcessorsPointer(p.targetPath, jsonPtr)
	if err != nil {
		return confs, err
	}
	if len(targetPath) == 0 {
		targetPath = p.targetPath
	}

	// Set custom environment vars.
	ogEnvVars := map[string]string{}
	for k, v := range environment {
		ogEnvVars[k] = os.Getenv(k)
		os.Setenv(k, v)
	}

	cleanupEnv := setEnvironment(environment)
	defer cleanupEnv()

	configBytes, err := config.ReadWithJSONPointers(targetPath, true)
	if err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", targetPath, err)
	}

	mgrWrapper := struct {
		Manager manager.Config `yaml:"resources"`
	}{
		Manager: manager.NewConfig(),
	}
	if err = yaml.Unmarshal(configBytes, &mgrWrapper); err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", targetPath, err)
	}

	for _, path := range p.resourcesPaths {
		resourceBytes, err := config.ReadWithJSONPointers(path, true)
		if err != nil {
			return confs, fmt.Errorf("failed to parse resources config file '%v': %v", path, err)
		}
		extraMgrWrapper := struct {
			Manager manager.Config `yaml:"resources"`
		}{
			Manager: manager.NewConfig(),
		}
		if err = yaml.Unmarshal(resourceBytes, &extraMgrWrapper); err != nil {
			return confs, fmt.Errorf("failed to parse resources config file '%v': %v", path, err)
		}
		if err = mgrWrapper.Manager.AddFrom(&extraMgrWrapper.Manager); err != nil {
			return confs, fmt.Errorf("failed to merge resources from '%v': %v", path, err)
		}
	}

	confs.mgr = mgrWrapper.Manager

	var root interface{}
	if err = yaml.Unmarshal(configBytes, &root); err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", targetPath, err)
	}

	var procs interface{}
	if procs, err = config.JSONPointer(procPath, root); err != nil {
		return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
	}

	var rawBytes []byte
	if rawBytes, err = yaml.Marshal(procs); err != nil {
		return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
	}

	switch procs.(type) {
	case []interface{}:
		if err = yaml.Unmarshal(rawBytes, &confs.procs); err != nil {
			return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
		}
	default:
		var procConf processor.Config
		if err = yaml.Unmarshal(rawBytes, &procConf); err != nil {
			return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
		}
		confs.procs = append(confs.procs, procConf)
	}

	p.cachedConfigs[cacheKey] = confs
	return confs, nil
}

//------------------------------------------------------------------------------
