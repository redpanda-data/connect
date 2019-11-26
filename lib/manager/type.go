// Copyright (c) 2018 Ashley Jeffs
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package manager

import (
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Jeffail/benthos/v3/lib/cache"
	"github.com/Jeffail/benthos/v3/lib/condition"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/benthos/v3/lib/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/Jeffail/benthos/v3/lib/util/config"
)

//------------------------------------------------------------------------------

// APIReg is an interface representing an API builder.
type APIReg interface {
	RegisterEndpoint(path, desc string, h http.HandlerFunc)
}

//------------------------------------------------------------------------------

// Config contains all configuration fields for a Benthos service manager.
type Config struct {
	Caches     map[string]cache.Config     `json:"caches" yaml:"caches"`
	Conditions map[string]condition.Config `json:"conditions" yaml:"conditions"`
	Processors map[string]processor.Config `json:"processors" yaml:"processors"`
	RateLimits map[string]ratelimit.Config `json:"rate_limits" yaml:"rate_limits"`
	Plugins    map[string]PluginConfig     `json:"plugins,omitempty" yaml:"plugins,omitempty"`
}

// NewConfig returns a Config with default values.
func NewConfig() Config {
	return Config{
		Caches:     map[string]cache.Config{},
		Conditions: map[string]condition.Config{},
		Processors: map[string]processor.Config{},
		RateLimits: map[string]ratelimit.Config{},
		Plugins:    map[string]PluginConfig{},
	}
}

// AddExamples inserts example caches and conditions if none exist in the
// config.
func AddExamples(c *Config) {
	if len(c.Caches) == 0 {
		c.Caches["example"] = cache.NewConfig()
	}
	if len(c.Conditions) == 0 {
		c.Conditions["example"] = condition.NewConfig()
	}
	if len(c.Processors) == 0 {
		c.Processors["example"] = processor.NewConfig()
	}
	if len(c.RateLimits) == 0 {
		c.RateLimits["example"] = ratelimit.NewConfig()
	}
}

//------------------------------------------------------------------------------

// SanitiseConfig creates a sanitised version of a manager config.
func SanitiseConfig(conf Config) (interface{}, error) {
	var err error

	caches := map[string]interface{}{}
	for k, v := range conf.Caches {
		if caches[k], err = cache.SanitiseConfig(v); err != nil {
			return nil, err
		}
	}

	conditions := map[string]interface{}{}
	for k, v := range conf.Conditions {
		if conditions[k], err = condition.SanitiseConfig(v); err != nil {
			return nil, err
		}
	}

	processors := map[string]interface{}{}
	for k, v := range conf.Processors {
		if processors[k], err = processor.SanitiseConfig(v); err != nil {
			return nil, err
		}
	}

	rateLimits := map[string]interface{}{}
	for k, v := range conf.RateLimits {
		if rateLimits[k], err = ratelimit.SanitiseConfig(v); err != nil {
			return nil, err
		}
	}

	plugins := map[string]interface{}{}
	for k, v := range conf.Plugins {
		if spec, exists := pluginSpecs[v.Type]; exists {
			if spec.confSanitiser != nil {
				outputMap := config.Sanitised{}
				outputMap["type"] = v.Type
				outputMap["plugin"] = spec.confSanitiser(v.Plugin)
				plugins[k] = outputMap
			} else {
				plugins[k] = v
			}
		}
	}

	m := map[string]interface{}{
		"caches":      caches,
		"conditions":  conditions,
		"processors":  processors,
		"rate_limits": rateLimits,
	}
	if len(plugins) > 0 {
		m["plugins"] = plugins
	}
	return m, nil
}

//------------------------------------------------------------------------------

// Type is an implementation of types.Manager, which is expected by Benthos
// components that need to register service wide behaviours such as HTTP
// endpoints and event listeners, and obtain service wide shared resources such
// as caches and labelled conditions.
type Type struct {
	apiReg     APIReg
	caches     map[string]types.Cache
	conditions map[string]types.Condition
	processors map[string]types.Processor
	rateLimits map[string]types.RateLimit
	plugins    map[string]interface{}

	pipes    map[string]<-chan types.Transaction
	pipeLock sync.RWMutex
}

// New returns an instance of manager.Type, which can be shared amongst
// components and logical threads of a Benthos service.
func New(
	conf Config,
	apiReg APIReg,
	log log.Modular,
	stats metrics.Type,
) (*Type, error) {
	t := &Type{
		apiReg:     apiReg,
		caches:     map[string]types.Cache{},
		conditions: map[string]types.Condition{},
		processors: map[string]types.Processor{},
		rateLimits: map[string]types.RateLimit{},
		plugins:    map[string]interface{}{},
		pipes:      map[string]<-chan types.Transaction{},
	}

	for k, conf := range conf.Caches {
		newCache, err := cache.New(conf, t, log.NewModule(".resource.cache."+k), metrics.Namespaced(stats, "resource.cache."+k))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create cache resource '%v' of type '%v': %v",
				k, conf.Type, err,
			)
		}
		t.caches[k] = newCache
	}

	// Sometimes condition resources might refer to other condition resources.
	// When they are constructed they will check with the manager to ensure the
	// resource they point to is valid, but not use the condition. Since we
	// cannot guarantee an order of initialisation we create placeholder
	// conditions during construction.
	for k := range conf.Conditions {
		t.conditions[k] = nil
	}

	// TODO: Prevent recursive conditions.
	for k, newConf := range conf.Conditions {
		newCond, err := condition.New(newConf, t, log.NewModule(".resource.condition."+k), metrics.Namespaced(stats, "resource.condition."+k))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create condition resource '%v' of type '%v': %v",
				k, newConf.Type, err,
			)
		}

		t.conditions[k] = newCond
	}

	// Sometimes processor resources might refer to other processor resources.
	// When they are constructed they will check with the manager to ensure the
	// resource they point to is valid, but not use the processor. Since we
	// cannot guarantee an order of initialisation we create placeholder
	// processors during construction.
	for k := range conf.Processors {
		t.processors[k] = nil
	}

	// TODO: Prevent recursive processors.
	for k, newConf := range conf.Processors {
		newProc, err := processor.New(newConf, t, log.NewModule(".resource.processor."+k), metrics.Namespaced(stats, "resource.processor."+k))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create processor resource '%v' of type '%v': %v",
				k, newConf.Type, err,
			)
		}

		t.processors[k] = newProc
	}

	for k, conf := range conf.RateLimits {
		newRL, err := ratelimit.New(conf, t, log.NewModule(".resource.rate_limit."+k), metrics.Namespaced(stats, "resource.rate_limit."+k))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create rate_limit resource '%v' of type '%v': %v",
				k, conf.Type, err,
			)
		}
		t.rateLimits[k] = newRL
	}

	for k, conf := range conf.Plugins {
		spec, exists := pluginSpecs[conf.Type]
		if !exists {
			return nil, fmt.Errorf("unrecognised plugin type '%v'", conf.Type)
		}
		newP, err := spec.constructor(conf.Plugin, t, log.NewModule(".resource.plugin."+k), metrics.Namespaced(stats, "resource.plugin."+k))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to create plugin resource '%v' of type '%v': %v",
				k, conf.Type, err,
			)
		}
		t.plugins[k] = newP
	}

	// Note: Caches, conditions and rate limits are considered READONLY from
	// this point onwards and are therefore NOT protected by mutexes or
	// channels.

	return t, nil
}

//------------------------------------------------------------------------------

// RegisterEndpoint registers a server wide HTTP endpoint.
func (t *Type) RegisterEndpoint(path, desc string, h http.HandlerFunc) {
	t.apiReg.RegisterEndpoint(path, desc, h)
}

// GetCache attempts to find a service wide cache by its name.
func (t *Type) GetCache(name string) (types.Cache, error) {
	if c, exists := t.caches[name]; exists {
		return c, nil
	}
	return nil, types.ErrCacheNotFound
}

// GetPipe attempts to obtain and return a named output Pipe
func (t *Type) GetPipe(name string) (<-chan types.Transaction, error) {
	t.pipeLock.RLock()
	pipe, exists := t.pipes[name]
	t.pipeLock.RUnlock()
	if exists {
		return pipe, nil
	}
	return nil, types.ErrPipeNotFound
}

// SetPipe registers a new transaction chan to a named pipe.
func (t *Type) SetPipe(name string, tran <-chan types.Transaction) {
	t.pipeLock.Lock()
	t.pipes[name] = tran
	t.pipeLock.Unlock()
}

// UnsetPipe removes a named pipe transaction chan.
func (t *Type) UnsetPipe(name string, tran <-chan types.Transaction) {
	t.pipeLock.Lock()
	if otran, exists := t.pipes[name]; exists && otran == tran {
		delete(t.pipes, name)
	}
	t.pipeLock.Unlock()
}

// GetCondition attempts to find a service wide condition by its name.
func (t *Type) GetCondition(name string) (types.Condition, error) {
	if c, exists := t.conditions[name]; exists {
		return c, nil
	}
	return nil, types.ErrConditionNotFound
}

// GetProcessor attempts to find a service wide processor by its name.
func (t *Type) GetProcessor(name string) (types.Processor, error) {
	if p, exists := t.processors[name]; exists {
		return p, nil
	}
	return nil, types.ErrProcessorNotFound
}

// GetRateLimit attempts to find a service wide rate limit by its name.
func (t *Type) GetRateLimit(name string) (types.RateLimit, error) {
	if rl, exists := t.rateLimits[name]; exists {
		return rl, nil
	}
	return nil, types.ErrRateLimitNotFound
}

// GetPlugin attempts to find a service wide resource plugin by its name.
func (t *Type) GetPlugin(name string) (interface{}, error) {
	if pl, exists := t.plugins[name]; exists {
		return pl, nil
	}
	return nil, types.ErrPluginNotFound
}

//------------------------------------------------------------------------------

// CloseAsync triggers the shut down of all resource types that implement the
// lifetime interface types.Closable.
func (t *Type) CloseAsync() {
	for _, c := range t.caches {
		c.CloseAsync()
	}
	for _, c := range t.conditions {
		if closer, ok := c.(types.Closable); ok {
			closer.CloseAsync()
		}
	}
	for _, p := range t.processors {
		p.CloseAsync()
	}
	for _, c := range t.plugins {
		if closer, ok := c.(types.Closable); ok {
			closer.CloseAsync()
		}
	}
	for _, c := range t.rateLimits {
		c.CloseAsync()
	}
}

// WaitForClose blocks until either all closable resource types are shut down or
// a timeout occurs.
func (t *Type) WaitForClose(timeout time.Duration) error {
	timesOut := time.Now().Add(timeout)
	for k, c := range t.caches {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
	}
	for k, c := range t.conditions {
		if closer, ok := c.(types.Closable); ok {
			if err := closer.WaitForClose(time.Until(timesOut)); err != nil {
				return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
			}
		}
	}
	for k, p := range t.processors {
		if err := p.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
	}
	for k, c := range t.rateLimits {
		if err := c.WaitForClose(time.Until(timesOut)); err != nil {
			return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
		}
	}
	for k, c := range t.plugins {
		if closer, ok := c.(types.Closable); ok {
			if err := closer.WaitForClose(time.Until(timesOut)); err != nil {
				return fmt.Errorf("resource '%s' failed to cleanly shutdown: %v", k, err)
			}
		}
	}
	return nil
}

//------------------------------------------------------------------------------
