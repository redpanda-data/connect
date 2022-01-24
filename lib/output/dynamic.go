package output

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/internal/interop"
	"github.com/Jeffail/benthos/v3/lib/api"
	"github.com/Jeffail/benthos/v3/lib/broker"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"gopkg.in/yaml.v3"
)

func init() {
	Constructors[TypeDynamic] = TypeSpec{
		constructor: fromSimpleConstructor(NewDynamic),
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
		FieldSpecs: docs.FieldSpecs{
			// TODO: Update with component type.
			docs.FieldCommon("outputs", "A map of outputs to statically create.").Map().HasType(docs.FieldTypeOutput),
			docs.FieldCommon("prefix", "A path prefix for HTTP endpoints that are registered."),
			docs.FieldCommon("timeout", "The server side timeout of HTTP requests."),
			docs.FieldCommon(
				"max_in_flight", "The maximum number of messages to dispatch across child outputs at any given time.",
			),
		},
		Categories: []Category{
			CategoryUtility,
		},
	}
}

//------------------------------------------------------------------------------

// DynamicConfig contains configuration fields for the Dynamic output type.
type DynamicConfig struct {
	Outputs     map[string]Config `json:"outputs" yaml:"outputs"`
	Prefix      string            `json:"prefix" yaml:"prefix"`
	Timeout     string            `json:"timeout" yaml:"timeout"`
	MaxInFlight int               `json:"max_in_flight" yaml:"max_in_flight"`
}

// NewDynamicConfig creates a new DynamicConfig with default values.
func NewDynamicConfig() DynamicConfig {
	return DynamicConfig{
		Outputs:     map[string]Config{},
		Prefix:      "",
		Timeout:     "5s",
		MaxInFlight: 1,
	}
}

//------------------------------------------------------------------------------

// NewDynamic creates a new Dynamic output type.
func NewDynamic(
	conf Config,
	mgr types.Manager,
	log log.Modular,
	stats metrics.Type,
) (Type, error) {
	dynAPI := api.NewDynamic()

	outputs := map[string]broker.DynamicOutput{}
	for k, v := range conf.Dynamic.Outputs {
		newOutput, err := New(v, mgr, log, stats)
		if err != nil {
			return nil, err
		}
		outputs[k] = newOutput
	}

	var reqTimeout time.Duration
	if tout := conf.Dynamic.Timeout; len(tout) > 0 {
		var err error
		if reqTimeout, err = time.ParseDuration(tout); err != nil {
			return nil, fmt.Errorf("failed to parse timeout string: %v", err)
		}
	}

	outputConfigs := conf.Dynamic.Outputs
	outputConfigsMut := sync.RWMutex{}

	fanOut, err := broker.NewDynamicFanOut(
		outputs, log, stats,
		broker.OptDynamicFanOutSetOnAdd(func(l string) {
			outputConfigsMut.Lock()
			defer outputConfigsMut.Unlock()

			uConf, exists := outputConfigs[l]
			if !exists {
				return
			}
			_ = uConf

			// TODO: V4 Implement this
			var confBytes []byte
			dynAPI.Started(l, confBytes)
			delete(outputConfigs, l)
		}),
		broker.OptDynamicFanOutSetOnRemove(func(l string) {
			dynAPI.Stopped(l)
		}),
	)
	if err != nil {
		return nil, err
	}
	fanOut = fanOut.WithMaxInFlight(conf.Dynamic.MaxInFlight)

	dynAPI.OnUpdate(func(id string, c []byte) error {
		newConf := NewConfig()
		if err := yaml.Unmarshal(c, &newConf); err != nil {
			return err
		}
		oMgr, oLog, oStats := interop.LabelChild(fmt.Sprintf("dynamic.outputs.%v", id), mgr, log, stats)
		oStats = metrics.Combine(stats, oStats)
		newOutput, err := New(newConf, oMgr, oLog, oStats)
		if err != nil {
			return err
		}
		outputConfigsMut.Lock()
		outputConfigs[id] = newConf
		outputConfigsMut.Unlock()
		if err = fanOut.SetOutput(id, newOutput, reqTimeout); err != nil {
			log.Errorf("Failed to set output '%v': %v", id, err)
			outputConfigsMut.Lock()
			delete(outputConfigs, id)
			outputConfigsMut.Unlock()
		}
		return err
	})
	dynAPI.OnDelete(func(id string) error {
		err := fanOut.SetOutput(id, nil, reqTimeout)
		if err != nil {
			log.Errorf("Failed to close output '%v': %v", id, err)
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

//------------------------------------------------------------------------------
