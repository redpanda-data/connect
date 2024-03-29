// Package config contains reusable config definitions and parsers for inputs
// defined via the public/service package. We could eventually consider moving
// this out into public/service for others to use.
package config

import (
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/public/service"
)

// Connection fields.
const (
	fieldConn           = "connection"
	fieldConnMaxRetries = "max_retries"
)

func connectionField() *service.ConfigField {
	return service.NewObjectField(fieldConn,
		service.NewIntField(fieldConnMaxRetries).
			Description("An optional limit to the number of consecutive retry attempts that will be made before abandoning the connection altogether and gracefully terminating the input. When all inputs terminate in this way the service (or stream) will shut down. If set to zero connections will never be reattempted upon a failure. If set below zero this field is ignored (effectively unset).").
			Advanced().
			Example(-1).
			Example(10).
			Optional(),
	).
		Description("Customise how websocket connection attempts are made.").
		Optional().
		Advanced()
}

func connectionBackOffFromParsed(conf *service.ParsedConfig) (boff backoff.BackOff, err error) {
	{
		eboff := backoff.NewExponentialBackOff()
		eboff.InitialInterval = time.Millisecond * 100
		eboff.MaxInterval = time.Second
		eboff.MaxElapsedTime = 0
		boff = eboff
	}

	if conf.Contains(fieldConnMaxRetries) {
		maxRetries, err := conf.FieldInt(fieldConnMaxRetries)
		if err != nil {
			return nil, err
		}
		if maxRetries >= 0 {
			boff = backoff.WithMaxRetries(boff, uint64(maxRetries))
		}
	}
	return
}

// AsyncOptsFields returns a list of config fields with the intended purpose of
// exposing AsyncReader configuration opts.
func AsyncOptsFields() []*service.ConfigField {
	return []*service.ConfigField{
		connectionField(),
	}
}

// AsyncOptsFromParsed returns a slice of AsyncReader opt funcs from a parsed
// config, allowing users to customise behaviour such as connection retry back
// off.
func AsyncOptsFromParsed(conf *service.ParsedConfig) (opts []func(*input.AsyncReader), err error) {
	if conf.Contains(fieldConn) {
		var boff backoff.BackOff
		if boff, err = connectionBackOffFromParsed(conf.Namespace(fieldConn)); err != nil {
			return
		}
		opts = append(opts, input.AsyncReaderWithConnBackOff(boff))
	}
	return
}
