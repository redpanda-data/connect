package service_test

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/Jeffail/benthos/v3/public/x/service"
)

type RandomRateLimit struct {
	max time.Duration
}

func (r *RandomRateLimit) Access(context.Context) (time.Duration, error) {
	return time.Duration(rand.Int() % int(r.max)), nil
}

func (r *RandomRateLimit) Close(ctx context.Context) error {
	return nil
}

// This example demonstrates how to create a rate limit plugin, which is
// configured by providing a struct containing the fields to be parsed from
// within the Benthos configuration.
func Example_rateLimitPlugin() {
	type randomRLConfig struct {
		MaxDuration string `yaml:"maximum_duration"`
	}

	configSpec := service.NewStructConfigSpec(func() interface{} {
		return &randomRLConfig{
			MaxDuration: "1s",
		}
	})

	constructor := func(conf *service.ParsedConfig, mgr *service.Resources) (service.RateLimit, error) {
		c := conf.AsStruct().(*randomRLConfig)
		maxDuration, err := time.ParseDuration(c.MaxDuration)
		if err != nil {
			return nil, fmt.Errorf("invalid max duration: %w", err)
		}
		return &RandomRateLimit{maxDuration}, nil
	}

	err := service.RegisterRateLimit("random", configSpec, constructor)
	if err != nil {
		panic(err)
	}

	// And then execute Benthos with:
	// service.RunCLI()
}
