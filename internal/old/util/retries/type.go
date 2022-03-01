package retries

import (
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

// Backoff contains configuration params for the exponential backoff of the
// retry mechanism.
type Backoff struct {
	InitialInterval string `json:"initial_interval" yaml:"initial_interval"`
	MaxInterval     string `json:"max_interval" yaml:"max_interval"`
	MaxElapsedTime  string `json:"max_elapsed_time" yaml:"max_elapsed_time"`
}

// Config contains configuration params for a retries mechanism.
type Config struct {
	MaxRetries uint64  `json:"max_retries" yaml:"max_retries"`
	Backoff    Backoff `json:"backoff" yaml:"backoff"`
}

// NewConfig creates a new Config with default values.
func NewConfig() Config {
	return Config{
		MaxRetries: 0,
		Backoff: Backoff{
			InitialInterval: "500ms",
			MaxInterval:     "3s",
			MaxElapsedTime:  "0s",
		},
	}
}

//------------------------------------------------------------------------------

// Get returns a valid *backoff.ExponentialBackoff based on the configuration
// values of Config.
func (c *Config) Get() (backoff.BackOff, error) {
	ctor, err := c.GetCtor()
	if err != nil {
		return nil, err
	}
	return ctor(), nil
}

// GetCtor returns a constructor for a backoff.Backoff based on the
// configuration values of Config.
func (c *Config) GetCtor() (func() backoff.BackOff, error) {
	var initInterval, maxInterval, maxElapsed time.Duration
	var err error
	if c.Backoff.InitialInterval != "" {
		if initInterval, err = time.ParseDuration(c.Backoff.InitialInterval); err != nil {
			return nil, fmt.Errorf("invalid backoff initial interval: %v", err)
		}
	}
	if c.Backoff.MaxInterval != "" {
		if maxInterval, err = time.ParseDuration(c.Backoff.MaxInterval); err != nil {
			return nil, fmt.Errorf("invalid backoff max interval: %v", err)
		}
	}
	if c.Backoff.MaxElapsedTime != "" {
		if maxElapsed, err = time.ParseDuration(c.Backoff.MaxElapsedTime); err != nil {
			return nil, fmt.Errorf("invalid backoff max elapsed interval: %v", err)
		}
	}

	return func() backoff.BackOff {
		boff := backoff.NewExponentialBackOff()

		boff.InitialInterval = initInterval
		boff.MaxInterval = maxInterval
		boff.MaxElapsedTime = maxElapsed

		if c.MaxRetries > 0 {
			return backoff.WithMaxRetries(boff, c.MaxRetries)
		}
		return boff
	}, nil
}

//------------------------------------------------------------------------------
