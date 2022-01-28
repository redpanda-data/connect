package service

import (
	"github.com/cenkalti/backoff/v4"
)

// NewBackOffField defines a new object type config field that describes an
// exponential back off policy, often used for timing retry attempts. It is then
// possible to extract a *backoff.ExponentialBackOff from the resulting parsed
// config with the method FieldBackOff.
//
// It is possible to configure a back off policy that has no upper bound (no
// maximum elapsed time set). In cases where this would be problematic the field
// allowUnbounded should be set `false` in order to add linting rules that
// ensure an upper bound is set.
func NewBackOffField(name string, allowUnbounded bool) *ConfigField {
	maxElapsedTime := NewDurationField("max_elapsed_time").
		Description("The maximum overall period of time to spend on retry attempts before the request is aborted.").
		Default("15m").Example("1m").Example("1h")
	if allowUnbounded {
		maxElapsedTime.field.Description += " Setting this value to a zeroed duration (such as `0s`) will result in unbounded retries."
	}

	// TODO: Add linting rule to ensure we aren't unbounded if necessary.
	return NewObjectField(name,
		NewDurationField("initial_interval").
			Description("The initial period to wait between retry attempts.").
			Default("500ms").Example("50ms").Example("1s"),
		NewDurationField("max_interval").
			Description("The maximum period to wait between retry attempts").
			Default("60s").Example("5s").Example("1m"),
		maxElapsedTime,
	)
}

// FieldBackOff accesses a field from a parsed config that was defined with
// NewBackoffField and returns a *tls.Config, or an error if the configuration was
// invalid.
func (p *ParsedConfig) FieldBackOff(path ...string) (*backoff.ExponentialBackOff, error) {
	b := backoff.NewExponentialBackOff()

	var err error
	if b.InitialInterval, err = p.FieldDuration(append(path, "initial_interval")...); err != nil {
		return nil, err
	}
	if b.MaxInterval, err = p.FieldDuration(append(path, "max_interval")...); err != nil {
		return nil, err
	}
	if b.MaxElapsedTime, err = p.FieldDuration(append(path, "max_elapsed_time")...); err != nil {
		return nil, err
	}

	return b, nil
}
