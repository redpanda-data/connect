package io

import (
	"context"
	"path"
	"sync"

	"gopkg.in/yaml.v3"

	"github.com/benthosdev/benthos/v4/internal/api"
	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/output/processors"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func init() {
	err := bundle.AllOutputs.Add(
		processors.WrapConstructor(newDynamicOutput),
		docs.ComponentSpec{
			Name: "dynamic",
			Summary: `
A special broker type where the outputs are identified by unique labels and can
be created, changed and removed during runtime via a REST API.`,
			Description: `
The broker pattern used is always ` + "`fan_out`" + `, meaning each message will
be delivered to each dynamic output.

To GET a JSON map of output identifiers with their current uptimes use the
'/outputs' endpoint.

To perform CRUD actions on the outputs themselves use POST, DELETE, and GET
methods on the ` + "`/outputs/{output_id}`" + ` endpoint. When using POST the
body of the request should be a YAML configuration for the output, if the output
already exists it will be changed.`,
			Config: docs.FieldComponent().WithChildren(
				docs.FieldOutput("outputs", "A map of outputs to statically create.").Map().HasDefault(map[string]any{}),
				docs.FieldString("prefix", "A path prefix for HTTP endpoints that are registered.").HasDefault(""),
			),
			Categories: []string{
				"Utility",
			},
		})
	if err != nil {
		panic(err)
	}
}

func newDynamicOutput(conf output.Config, mgr bundle.NewManagement) (output.Streamed, error) {
	dynAPI := api.NewDynamic()

	outputs := map[string]output.Streamed{}
	for k, v := range conf.Dynamic.Outputs {
		oMgr := mgr.IntoPath("dynamic", "outputs", k)
		newOutput, err := oMgr.NewOutput(v)
		if err != nil {
			return nil, err
		}
		if outputs[k], err = pure.RetryOutputIndefinitely(mgr, newOutput); err != nil {
			return nil, err
		}
	}

	outputConfigs := conf.Dynamic.Outputs
	outputConfigsMut := sync.RWMutex{}

	fanOut, err := newDynamicFanOutOutputBroker(outputs, mgr.Logger(),
		func(l string) {
			outputConfigsMut.Lock()
			defer outputConfigsMut.Unlock()

			uConf, exists := outputConfigs[l]
			if !exists {
				return
			}

			var confBytes []byte
			var node yaml.Node
			if err := node.Encode(uConf); err == nil {
				sanitConf := docs.NewSanitiseConfig()
				sanitConf.RemoveTypeField = true
				sanitConf.ScrubSecrets = true
				if err := docs.FieldOutput("output", "").SanitiseYAML(&node, sanitConf); err == nil {
					confBytes, _ = yaml.Marshal(node)
				}
			}

			dynAPI.Started(l, confBytes)
			delete(outputConfigs, l)
		},
		func(l string) {
			dynAPI.Stopped(l)
		},
	)
	if err != nil {
		return nil, err
	}

	dynAPI.OnUpdate(func(ctx context.Context, id string, c []byte) error {
		newConf := output.NewConfig()
		if err := yaml.Unmarshal(c, &newConf); err != nil {
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
		outputConfigs[id] = newConf
		outputConfigsMut.Unlock()
		if err = fanOut.SetOutput(ctx, id, newOutput); err != nil {
			mgr.Logger().Errorf("Failed to set output '%v': %v", id, err)
			outputConfigsMut.Lock()
			delete(outputConfigs, id)
			outputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(ctx context.Context, id string) error {
		err := fanOut.SetOutput(ctx, id, nil)
		if err != nil {
			mgr.Logger().Errorf("Failed to close output '%v': %v", id, err)
		}
		return err
	})

	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/outputs/{id}"),
		"Perform CRUD operations on the configuration of dynamic outputs. For"+
			" more information read the `dynamic` output type documentation.",
		dynAPI.HandleCRUD,
	)
	mgr.RegisterEndpoint(
		path.Join(conf.Dynamic.Prefix, "/outputs"),
		"Get a map of running output identifiers with their current uptimes.",
		dynAPI.HandleList,
	)

	return fanOut, nil
}
