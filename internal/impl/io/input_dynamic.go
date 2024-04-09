package io

import (
	"context"
	"path"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	diFieldInputs = "inputs"
	diFieldPrefix = "prefix"
)

func dynInputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Utility").
		Summary(`A special broker type where the inputs are identified by unique labels and can be created, changed and removed during runtime via a REST HTTP interface.`).
		Footnotes(`
## Endpoints

### GET `+"`/inputs`"+`

Returns a JSON object detailing all dynamic inputs, providing information such as their current uptime and configuration.

### GET `+"`/inputs/{id}`"+`

Returns the configuration of an input.

### POST `+"`/inputs/{id}`"+`

Creates or updates an input with a configuration provided in the request body (in YAML or JSON format).

### DELETE `+"`/inputs/{id}`"+`

Stops and removes an input.

### GET `+"`/inputs/{id}/uptime`"+`

Returns the uptime of an input as a duration string (of the form "72h3m0.5s"), or "stopped" in the case where the input has gracefully terminated.`).
		Fields(
			service.NewInputMapField(diFieldInputs).
				Description("A map of inputs to statically create.").
				Default(map[string]any{}),
			service.NewStringField(diFieldPrefix).
				Description("A path prefix for HTTP endpoints that are registered.").
				Default(""),
		)
}

func init() {
	err := service.RegisterBatchInput("dynamic", dynInputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			i, err := newDynamicInputFromParsed(conf, mgr)
			if err != nil {
				return nil, err
			}
			return interop.NewUnwrapInternalInput(i), nil
		})
	if err != nil {
		panic(err)
	}
}

func dynInputAnyToYAMLConf(v any) []byte {
	var node yaml.Node
	if err := node.Encode(v); err != nil {
		return nil
	}

	sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
	sanitConf.RemoveTypeField = true
	sanitConf.ScrubSecrets = true
	if err := docs.FieldInput("input", "").SanitiseYAML(&node, sanitConf); err != nil {
		return nil
	}

	confBytes, _ := yaml.Marshal(node)
	return confBytes
}

func newDynamicInputFromParsed(conf *service.ParsedConfig, res *service.Resources) (input.Streamed, error) {
	inputsMap, err := conf.FieldInputMap(diFieldInputs)
	if err != nil {
		return nil, err
	}

	inputsAnyMap, err := conf.FieldAnyMap(diFieldInputs)
	if err != nil {
		return nil, err
	}

	prefix, err := conf.FieldString(diFieldPrefix)
	if err != nil {
		return nil, err
	}

	inputs := map[string]input.Streamed{}
	for k, v := range inputsMap {
		inputs[k] = interop.UnwrapOwnedInput(v)
	}

	var inputConfigsMut sync.Mutex
	inputYAMLConfs := map[string][]byte{}
	for k, v := range inputsAnyMap {
		a, _ := v.FieldAny()
		inputYAMLConfs[k] = dynInputAnyToYAMLConf(a)
	}

	dynAPI := api.NewDynamic()
	mgr := interop.UnwrapManagement(res)
	fanIn, err := newDynamicFanInInput(
		inputs, mgr.Logger(),
		func(ctx context.Context, l string) {
			inputConfigsMut.Lock()
			defer inputConfigsMut.Unlock()

			confBytes, exists := inputYAMLConfs[l]
			if !exists {
				return
			}

			dynAPI.Started(l, confBytes)
			delete(inputYAMLConfs, l)
		},
		func(ctx context.Context, l string) {
			dynAPI.Stopped(l)
		},
	)
	if err != nil {
		return nil, err
	}

	dynAPI.OnUpdate(func(ctx context.Context, id string, c []byte) error {
		confNode, err := docs.UnmarshalYAML(c)
		if err != nil {
			return err
		}

		newConf, err := input.FromAny(bundle.GlobalEnvironment, confNode)
		if err != nil {
			return err
		}

		iMgr := mgr.IntoPath("dynamic", "inputs", id)
		newInput, err := iMgr.NewInput(newConf)
		if err != nil {
			return err
		}

		inputConfigsMut.Lock()
		inputYAMLConfs[id] = dynInputAnyToYAMLConf(newConf)
		inputConfigsMut.Unlock()
		if err = fanIn.SetInput(ctx, id, newInput); err != nil {
			mgr.Logger().Error("Failed to set input '%v': %v", id, err)
			inputConfigsMut.Lock()
			delete(inputYAMLConfs, id)
			inputConfigsMut.Unlock()
		}
		return err
	})

	dynAPI.OnDelete(func(ctx context.Context, id string) error {
		err := fanIn.SetInput(ctx, id, nil)
		if err != nil {
			mgr.Logger().Error("Failed to close input '%v': %v", id, err)
		}
		return err
	})

	mgr.RegisterEndpoint(
		path.Join(prefix, "/inputs/{id}/uptime"),
		`Returns the uptime of a specific input as a duration string, or "stopped" for inputs that are no longer running and have gracefully terminated.`,
		dynAPI.HandleUptime,
	)
	mgr.RegisterEndpoint(
		path.Join(prefix, "/inputs/{id}"),
		"Perform CRUD operations on the configuration of dynamic inputs. For"+
			" more information read the `dynamic` input type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(prefix, "/inputs"),
		"Get a map of running input identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanIn, nil
}
