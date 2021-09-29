package backoff

import (
	"fmt"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
)

//------------------------------------------------------------------------------

// Documentation is a markdown description of how and why to use Backoff settings.
const Documentation = `### Backoff

Custom Backoff settings can be used to override benthos defaults.
`

//------------------------------------------------------------------------------

// Config is a configuration struct for an Backoff type.
type Config struct {
	Type                string  `json:"type" yaml:"type"`
	Interval            string  `json:"interval" yaml:"interval"`
	MaxInterval         string  `json:"max_interval" yaml:"max_interval"`
	MaxElapsedTime      string  `json:"max_elapsed_time" yaml:"max_elapsed_time"`
	Multiplier          float64 `json:"multiplier" yaml:"multiplier"`
	RandomizationFactor float64 `json:"randomization_factor" yaml:"randomization_factor"`
	MaxRetries          int     `json:"max_retries" yaml:"max_retries"`
}

// NewConfig creates a new Config with default values.
func NewConfig() Config {
	return Config{
		Type:                "exponential",
		Interval:            "100ms",
		MaxInterval:         "1s",
		MaxElapsedTime:      "",
		Multiplier:          1.5,
		RandomizationFactor: 0.5,
		MaxRetries:          0,
	}
}

//------------------------------------------------------------------------------

// ErrTypeNotSupported is returned if a Backoff Type was not recognized.
type ErrTypeNotSupported string

// Error implements the standard error interface.
func (e ErrTypeNotSupported) Error() string {
	return fmt.Sprintf("Backoff type %v was not recognised", string(e))
}

// Get returns a valid github.com/cenkalti/backoff/v4.Backoff based on the configuration values of Config.
// If unknown type of the backoff are set or interval is empty then a nil backoff is returned.
func (c *Config) Get() (backoff.BackOff, error) {
	if c.Interval == "" {
		return nil, nil
	}
	var boff backoff.BackOff
	switch strings.ToLower(c.Type) {
	case "constant":
		interval, err := parseDurationString("interval", c.Interval)
		if err != nil {
			return nil, err
		}
		boff = backoff.NewConstantBackOff(interval)
	case "exponential":
		var (
			err   error
			eboff = backoff.NewExponentialBackOff()
		)
		if eboff.InitialInterval, err = parseDurationString("interval", c.Interval); err != nil {
			return nil, err
		}
		if len(c.MaxInterval) > 0 {
			if eboff.MaxInterval, err = parseDurationString("max_interval", c.MaxInterval); err != nil {
				return nil, err
			}
		}
		eboff.MaxElapsedTime = 0
		if len(c.MaxElapsedTime) > 0 {
			if eboff.MaxElapsedTime, err = parseDurationString("max_elapsed_time", c.MaxElapsedTime); err != nil {
				return nil, err
			}
		}
		eboff.Multiplier = c.Multiplier
		eboff.RandomizationFactor = c.RandomizationFactor
		boff = eboff
	default:
		return nil, ErrTypeNotSupported(c.Type)
	}
	if c.MaxRetries > 0 {
		boff = backoff.WithMaxRetries(boff, uint64(c.MaxRetries))
	}
	return boff, nil
}

func parseDurationString(name, value string) (time.Duration, error) {
	d, err := time.ParseDuration(value)
	return d, fmt.Errorf("failed to parse %s duration string: %s", name, err)
}
