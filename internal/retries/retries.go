// Copyright 2024 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package retries

import (
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/redpanda-data/benthos/v4/public/service"
)

const (
	crboFieldMaxRetries     = "max_retries"
	crboFieldBackOff        = "backoff"
	crboFieldInitInterval   = "initial_interval"
	crboFieldMaxInterval    = "max_interval"
	crboFieldMaxElapsedTime = "max_elapsed_time"
)

// CommonRetryBackOffFields returns the common retry with backoff fields.
func CommonRetryBackOffFields(
	defaultMaxRetries int,
	defaultInitInterval string,
	defaultMaxInterval string,
	defaultMaxElapsed string,
) []*service.ConfigField {
	return []*service.ConfigField{
		service.NewIntField(crboFieldMaxRetries).
			Description(service.RetryMaxRetriesDescription).
			Default(defaultMaxRetries).
			Advanced(),
		service.NewObjectField(crboFieldBackOff,
			service.NewDurationField(crboFieldInitInterval).
				Description(service.RetryInitialIntervalDescription).
				Default(defaultInitInterval),
			service.NewDurationField(crboFieldMaxInterval).
				Description(service.RetryMaxIntervalDescription).
				Default(defaultMaxInterval),
			service.NewDurationField(crboFieldMaxElapsedTime).
				Description(service.RetryMaxElapsedTimeDescription).
				Default(defaultMaxElapsed),
		).
			Description(service.RetryBackOffDescription).
			Advanced(),
	}
}

func fieldDurationOrEmptyStr(pConf *service.ParsedConfig, path ...string) (time.Duration, error) {
	if dStr, err := pConf.FieldString(path...); err == nil && dStr == "" {
		return 0, nil
	}
	return pConf.FieldDuration(path...)
}

// CommonRetryBackOffCtorFromParsed extracts the common retry with backoff fields from a parsed config.
func CommonRetryBackOffCtorFromParsed(pConf *service.ParsedConfig) (ctor func() backoff.BackOff, err error) {
	var maxRetries int
	if maxRetries, err = pConf.FieldInt(crboFieldMaxRetries); err != nil {
		return
	}

	var initInterval, maxInterval, maxElapsed time.Duration
	if pConf.Contains(crboFieldBackOff) {
		bConf := pConf.Namespace(crboFieldBackOff)
		if initInterval, err = fieldDurationOrEmptyStr(bConf, crboFieldInitInterval); err != nil {
			return
		}
		if maxInterval, err = fieldDurationOrEmptyStr(bConf, crboFieldMaxInterval); err != nil {
			return
		}
		if maxElapsed, err = fieldDurationOrEmptyStr(bConf, crboFieldMaxElapsedTime); err != nil {
			return
		}
	}

	return func() backoff.BackOff {
		boff := backoff.NewExponentialBackOff()

		boff.InitialInterval = initInterval
		boff.MaxInterval = maxInterval
		boff.MaxElapsedTime = maxElapsed

		if maxRetries > 0 {
			return backoff.WithMaxRetries(boff, uint64(maxRetries))
		}
		return boff
	}, nil
}
