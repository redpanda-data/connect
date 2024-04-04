package io

import (
	"context"
	"path"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/interop"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	doFieldPrefix  = "prefix"
	doFieldOutputs = "outputs"
)

func dynOutputSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Categories("Utility").
		Stable().
		Summary(`A special broker type where the outputs are identified by unique labels and can be created, changed and removed during runtime via a REST API.`).
		Description(`The broker pattern used is always `+"`fan_out`"+`, meaning each message will be delivered to each dynamic output.`).
		Footnotes(`
## Endpoints

### GET `+"`/outputs`"+`

Returns a JSON object detailing all dynamic outputs, providing information such as their current uptime and configuration.

### GET `+"`/outputs/{id}`"+`

Returns the configuration of an output.

### POST `+"`/outputs/{id}`"+`

Creates or updates an output with a configuration provided in the request body (in YAML or JSON format).

### DELETE `+"`/outputs/{id}`"+`

Stops and removes an output.

### GET `+"`/outputs/{id}/uptime`"+`

Returns the uptime of an output as a duration string (of the form "72h3m0.5s").`).
		Fields(
			service.NewOutputMapField(doFieldOutputs).
				Description("A map of outputs to statically create.").
				Default(map[string]any{}),
			service.NewStringField(doFieldPrefix).
				Description("A path prefix for HTTP endpoints that are registered.").
				Default(""),
		)
}

func init() {
	err := service.RegisterBatchOutput("dynamic", dynOutputSpec(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.BatchOutput, batchPolicy service.BatchPolicy, maxInFlight int, err error) {
			maxInFlight = 1

			var o output.Streamed
			if o, err = newDynamicOutputFromParsed(conf, mgr); err != nil {
				return
			}

			out = interop.NewUnwrapInternalOutput(o)
			return
		})
	if err != nil {
		panic(err)
	}
}

func newDynamicOutputFromParsed(conf *service.ParsedConfig, res *service.Resources) (output.Streamed, error) {
	mgr := interop.UnwrapManagement(res)

	prefix, err := conf.FieldString(doFieldPrefix)
	if err != nil {
		return nil, err
	}

	outputsAnyMap, err := conf.FieldAnyMap(doFieldOutputs)
	if err != nil {
		return nil, err
	}

	wOutsMap, err := conf.FieldOutputMap(doFieldOutputs)
	if err != nil {
		return nil, err
	}

	dynAPI := api.NewDynamic()

	outputs := map[string]output.Streamed{}
	for k, v := range wOutsMap {
		newOutput := interop.UnwrapOwnedOutput(v)
		if outputs[k], err = pure.RetryOutputIndefinitely(mgr, newOutput); err != nil {
			return nil, err
		}
	}

	var outputConfigsMut sync.RWMutex
	outputYAMLConfs := map[string][]byte{}
	for k, v := range outputsAnyMap {
		a, _ := v.FieldAny()
		outputYAMLConfs[k] = dynOutputAnyToYAMLConf(a)
	}

	fanOut, err := newDynamicFanOutOutputBroker(outputs, mgr.Logger(),
		func(l string) {
			outputConfigsMut.Lock()
			defer outputConfigsMut.Unlock()

			confBytes, exists := outputYAMLConfs[l]
			if !exists {
				return
			}

			dynAPI.Started(l, confBytes)
			delete(outputYAMLConfs, l)
		},
		func(l string) {
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

		newConf, err := output.FromAny(bundle.GlobalEnvironment, confNode)
		if err != nil {
			return err
		}

		oMgr := mgr.IntoPath("dynamic", "outputs", id)
		newOutput, err := oMgr.NewOutput(newConf)
		if err != nil {
			return err
		}
		if newOutput, err = pure.RetryOutputIndefinitely(mgr, newOutput); err != nil {
			return err
		}

		outputConfigsMut.Lock()
		outputYAMLConfs[id] = dynOutputAnyToYAMLConf(newConf)
		outputConfigsMut.Unlock()
		if err = fanOut.SetOutput(ctx, id, newOutput); err != nil {
			mgr.Logger().Error("Failed to set output '%v': %v", id, err)
			outputConfigsMut.Lock()
			delete(outputYAMLConfs, id)
			outputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(ctx context.Context, id string) error {
		err := fanOut.SetOutput(ctx, id, nil)
		if err != nil {
			mgr.Logger().Error("Failed to close output '%v': %v", id, err)
		}
		return err
	})

	mgr.RegisterEndpoint(
		path.Join(prefix, "/outputs/{id}/uptime"),
		`Returns the uptime of a specific output as a duration string.`,
		dynAPI.HandleUptime,
	)
	mgr.RegisterEndpoint(
		path.Join(prefix, "/outputs/{id}"),
		"Perform CRUD operations on the configuration of dynamic outputs. For"+
			" more information read the `dynamic` output type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(prefix, "/outputs"),
		"Get a map of running output identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanOut, nil
}

func dynOutputAnyToYAMLConf(v any) []byte {
	var node yaml.Node
	if err := node.Encode(v); err != nil {
		return nil
	}

	sanitConf := docs.NewSanitiseConfig(bundle.GlobalEnvironment)
	sanitConf.RemoveTypeField = true
	sanitConf.ScrubSecrets = true
	if err := docs.FieldOutput("output", "").SanitiseYAML(&node, sanitConf); err != nil {
		return nil
	}

	confBytes, _ := yaml.Marshal(node)
	return confBytes
}
