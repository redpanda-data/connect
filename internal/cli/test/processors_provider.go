package test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/Jeffail/gabs/v2"
	yaml "gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/bloblang/mapping"
	"github.com/benthosdev/benthos/v4/internal/bloblang/parser"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/filepath/ifs"
	"github.com/benthosdev/benthos/v4/internal/log"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/internal/tracing"
)

type cachedConfig struct {
	mgr   manager.ResourceConfig
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

// Provide attempts to extract an array of processors from a Benthos config.
// Supports injected mocked components in the parsed config. If the JSON Pointer
// targets a single processor config it will be constructed and returned as an
// array of one element.
func (p *ProcessorsProvider) Provide(jsonPtr string, environment map[string]string, mocks map[string]yaml.Node) ([]processor.V1, error) {
	confs, err := p.getConfs(jsonPtr, environment, mocks)
	if err != nil {
		return nil, err
	}
	return p.initProcs(confs)
}

// ProvideBloblang attempts to parse a Bloblang mapping and returns a processor
// slice that executes it.
func (p *ProcessorsProvider) ProvideBloblang(pathStr string) ([]processor.V1, error) {
	if !filepath.IsAbs(pathStr) {
		pathStr = filepath.Join(filepath.Dir(p.targetPath), pathStr)
	}

	mappingBytes, err := ifs.ReadFile(ifs.OS(), pathStr)
	if err != nil {
		return nil, err
	}

	pCtx := parser.GlobalContext().WithImporterRelativeToFile(pathStr)
	exec, mapErr := parser.ParseMapping(pCtx, string(mappingBytes))
	if mapErr != nil {
		return nil, mapErr
	}

	return []processor.V1{
		processor.NewV2BatchedToV1Processor("bloblang", newBloblang(exec, p.logger), mock.NewManager()),
	}, nil
}

type bloblangProc struct {
	exec *mapping.Executor
	log  log.Modular
}

func newBloblang(exec *mapping.Executor, log log.Modular) processor.V2Batched {
	return &bloblangProc{
		exec: exec,
		log:  log,
	}
}

func (b *bloblangProc) ProcessBatch(ctx context.Context, spans []*tracing.Span, msg message.Batch) ([]message.Batch, error) {
	newParts := make([]*message.Part, 0, msg.Len())
	_ = msg.Iter(func(i int, part *message.Part) error {
		p, err := b.exec.MapPart(i, msg)
		if err != nil {
			p = part.ShallowCopy()
			b.log.Errorf("%v\n", err)
			processor.MarkErr(p, spans[i], err)
		}
		if p != nil {
			newParts = append(newParts, p)
		}
		return nil
	})
	if len(newParts) == 0 {
		return nil, nil
	}

	newMsg := message.Batch(newParts)
	return []message.Batch{newMsg}, nil
}

func (b *bloblangProc) Close(context.Context) error {
	return nil
}

//------------------------------------------------------------------------------

func (p *ProcessorsProvider) initProcs(confs cachedConfig) ([]processor.V1, error) {
	mgr, err := manager.New(confs.mgr, manager.OptSetLogger(p.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to initialise resources: %v", err)
	}

	procs := make([]processor.V1, len(confs.procs))
	for i, conf := range confs.procs {
		if procs[i], err = mgr.NewProcessor(conf); err != nil {
			return nil, fmt.Errorf("failed to initialise processor index '%v': %v", i, err)
		}
	}
	return procs, nil
}

func confTargetID(jsonPtr string, environment map[string]string, mocks map[string]yaml.Node) string {
	mocksBytes, _ := yaml.Marshal(mocks)
	return fmt.Sprintf("%v-%v-%s", jsonPtr, environment, mocksBytes)
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

func resolveProcessorsPointer(targetFile, jsonPtr string) (filePath, procPath string, err error) {
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
	if procPath == "" {
		err = fmt.Errorf("failed to target processors '%v': reference URI must contain a path or fragment", jsonPtr)
	}
	return
}

func (p *ProcessorsProvider) getConfs(jsonPtr string, environment map[string]string, mocks map[string]yaml.Node) (cachedConfig, error) {
	cacheKey := confTargetID(jsonPtr, environment, mocks)

	confs, exists := p.cachedConfigs[cacheKey]
	if exists {
		return confs, nil
	}

	targetPath, procPath, err := resolveProcessorsPointer(p.targetPath, jsonPtr)
	if err != nil {
		return confs, err
	}
	if targetPath == "" {
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

	remainingMocks := map[string]yaml.Node{}
	for k, v := range mocks {
		remainingMocks[k] = v
	}

	configBytes, _, _, err := config.ReadFileEnvSwap(ifs.OS(), targetPath)
	if err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", targetPath, err)
	}

	mgrWrapper := manager.NewResourceConfig()
	if err = yaml.Unmarshal(configBytes, &mgrWrapper); err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", targetPath, err)
	}

	for _, path := range p.resourcesPaths {
		resourceBytes, _, _, err := config.ReadFileEnvSwap(ifs.OS(), path)
		if err != nil {
			return confs, fmt.Errorf("failed to parse resources config file '%v': %v", path, err)
		}
		extraMgrWrapper := manager.NewResourceConfig()
		if err = yaml.Unmarshal(resourceBytes, &extraMgrWrapper); err != nil {
			return confs, fmt.Errorf("failed to parse resources config file '%v': %v", path, err)
		}
		if err = mgrWrapper.AddFrom(&extraMgrWrapper); err != nil {
			return confs, fmt.Errorf("failed to merge resources from '%v': %v", path, err)
		}
	}

	confs.mgr = mgrWrapper

	root := &yaml.Node{}
	if err = yaml.Unmarshal(configBytes, root); err != nil {
		return confs, fmt.Errorf("failed to parse config file '%v': %v", targetPath, err)
	}

	// Replace mock components, starting with all absolute paths in JSON pointer
	// form, then parsing remaining mock targets as label names.
	confSpec := config.Spec()
	for k, v := range remainingMocks {
		if !strings.HasPrefix(k, "/") {
			continue
		}
		mockPathSlice, err := gabs.JSONPointerToSlice(k)
		if err != nil {
			return confs, fmt.Errorf("failed to parse mock path '%v': %w", k, err)
		}
		if err = confSpec.SetYAMLPath(docs.DeprecatedProvider, root, &v, mockPathSlice...); err != nil {
			return confs, fmt.Errorf("failed to set mock '%v': %w", k, err)
		}
		delete(remainingMocks, k)
	}

	labelsToPaths := map[string][]string{}
	if len(remainingMocks) > 0 {
		confSpec.YAMLLabelsToPaths(docs.DeprecatedProvider, root, labelsToPaths, nil)
		for k, v := range remainingMocks {
			mockPathSlice, exists := labelsToPaths[k]
			if !exists {
				return confs, fmt.Errorf("mock for label '%v' could not be applied as the label was not found in the test target file, it is not currently possible to mock resources imported separate to the test file", k)
			}
			if err = confSpec.SetYAMLPath(docs.DeprecatedProvider, root, &v, mockPathSlice...); err != nil {
				return confs, fmt.Errorf("failed to set mock '%v': %w", k, err)
			}
			delete(remainingMocks, k)
		}
	}

	var pathSlice []string
	if strings.HasPrefix(procPath, "/") {
		if pathSlice, err = gabs.JSONPointerToSlice(procPath); err != nil {
			return confs, fmt.Errorf("failed to parse case processors path '%v': %w", procPath, err)
		}
	} else {
		if len(labelsToPaths) == 0 {
			confSpec.YAMLLabelsToPaths(docs.DeprecatedProvider, root, labelsToPaths, nil)
		}
		if pathSlice, exists = labelsToPaths[procPath]; !exists {
			return confs, fmt.Errorf("target for label '%v' failed as the label was not found in the test target file, it is not currently possible to target resources imported separate to the test file", procPath)
		}
	}

	if root, err = docs.GetYAMLPath(root, pathSlice...); err != nil {
		return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
	}

	if root.Kind == yaml.SequenceNode {
		if err = root.Decode(&confs.procs); err != nil {
			return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
		}
	} else {
		var procConf processor.Config
		if err = root.Decode(&procConf); err != nil {
			return confs, fmt.Errorf("failed to resolve case processors from '%v': %v", targetPath, err)
		}
		confs.procs = append(confs.procs, procConf)
	}

	p.cachedConfigs[cacheKey] = confs
	return confs, nil
}
