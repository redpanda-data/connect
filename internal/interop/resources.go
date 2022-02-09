package interop

import (
	"context"
	"errors"
	"fmt"

	"github.com/Jeffail/benthos/v3/internal/component"
	"github.com/Jeffail/benthos/v3/internal/component/cache"
	"github.com/Jeffail/benthos/v3/internal/component/processor"
	"github.com/Jeffail/benthos/v3/internal/component/ratelimit"
	"github.com/Jeffail/benthos/v3/lib/types"
)

// ProbeCache checks whether a cache resource has been configured, and returns
// an error if not.
func ProbeCache(ctx context.Context, mgr types.Manager, name string) error {
	if _, err := mgr.GetCache(name); err != nil {
		return fmt.Errorf("cache resource '%v' was not found", name)
	}
	return nil
}

// AccessCache attempts to access a cache resource by a unique identifier and
// executes a closure function with the cache as an argument. Returns an error
// if the cache does not exist (or is otherwise inaccessible).
func AccessCache(ctx context.Context, mgr types.Manager, name string, fn func(cache.V1)) error {
	if nm, ok := mgr.(interface {
		AccessCache(ctx context.Context, name string, fn func(cache.V1)) error
	}); ok {
		return nm.AccessCache(ctx, name, fn)
	}
	c, err := mgr.GetCache(name)
	if err != nil {
		return err
	}
	if c == nil {
		return component.ErrCacheNotFound
	}
	fn(c)
	return nil
}

// ProbeInput checks whether an input resource has been configured, and returns
// an error if not.
func ProbeInput(ctx context.Context, mgr types.Manager, name string) error {
	if gi, ok := mgr.(interface {
		GetInput(name string) (types.Input, error)
	}); ok {
		if _, err := gi.GetInput(name); err != nil {
			return fmt.Errorf("input resource '%v' was not found", name)
		}
	} else {
		return errors.New("manager does not support input resources")
	}
	return nil
}

// AccessInput attempts to access an input resource by a unique identifier and
// executes a closure function with the input as an argument. Returns an error
// if the input does not exist (or is otherwise inaccessible).
func AccessInput(ctx context.Context, mgr types.Manager, name string, fn func(types.Input)) error {
	if nm, ok := mgr.(interface {
		AccessInput(ctx context.Context, name string, fn func(types.Input)) error
	}); ok {
		return nm.AccessInput(ctx, name, fn)
	}
	if gi, ok := mgr.(interface {
		GetInput(name string) (types.Input, error)
	}); ok {
		c, err := gi.GetInput(name)
		if err != nil {
			return err
		}
		if c == nil {
			return component.ErrInputNotFound
		}
		fn(c)
		return nil
	}
	return errors.New("manager does not support input resources")
}

// ProbeOutput checks whether an output resource has been configured, and
// returns an error if not.
func ProbeOutput(ctx context.Context, mgr types.Manager, name string) error {
	if gi, ok := mgr.(interface {
		GetOutput(name string) (types.OutputWriter, error)
	}); ok {
		if _, err := gi.GetOutput(name); err != nil {
			return fmt.Errorf("output resource '%v' was not found", name)
		}
	} else {
		return errors.New("manager does not support output resources")
	}
	return nil
}

// AccessOutput attempts to access an output resource by a unique identifier and
// executes a closure function with the output as an argument. Returns an error
// if the output does not exist (or is otherwise inaccessible).
func AccessOutput(ctx context.Context, mgr types.Manager, name string, fn func(types.OutputWriter)) error {
	if nm, ok := mgr.(interface {
		AccessOutput(ctx context.Context, name string, fn func(types.OutputWriter)) error
	}); ok {
		return nm.AccessOutput(ctx, name, fn)
	}
	if gi, ok := mgr.(interface {
		GetOutput(name string) (types.OutputWriter, error)
	}); ok {
		o, err := gi.GetOutput(name)
		if err != nil {
			return err
		}
		if o == nil {
			return component.ErrOutputNotFound
		}
		fn(o)
		return nil
	}
	return errors.New("manager does not support output resources")
}

// ProbeProcessor checks whether a processor resource has been configured, and
// returns an error if not.
func ProbeProcessor(ctx context.Context, mgr types.Manager, name string) error {
	if gi, ok := mgr.(interface {
		GetProcessor(name string) (processor.V1, error)
	}); ok {
		if _, err := gi.GetProcessor(name); err != nil {
			return fmt.Errorf("processor resource '%v' was not found", name)
		}
	} else {
		return errors.New("manager does not support processor resources")
	}
	return nil
}

// AccessProcessor attempts to access a processor resource by a unique
// identifier and executes a closure function with the processor as an argument.
// Returns an error if the processor does not exist (or is otherwise
// inaccessible).
func AccessProcessor(ctx context.Context, mgr types.Manager, name string, fn func(processor.V1)) error {
	if nm, ok := mgr.(interface {
		AccessProcessor(ctx context.Context, name string, fn func(processor.V1)) error
	}); ok {
		return nm.AccessProcessor(ctx, name, fn)
	}
	if gi, ok := mgr.(interface {
		GetProcessor(name string) (processor.V1, error)
	}); ok {
		o, err := gi.GetProcessor(name)
		if err != nil {
			return err
		}
		if o == nil {
			return component.ErrProcessorNotFound
		}
		fn(o)
		return nil
	}
	return errors.New("manager does not support processor resources")
}

// ProbeRateLimit checks whether a rate limit resource has been configured, and
// returns an error if not.
func ProbeRateLimit(ctx context.Context, mgr types.Manager, name string) error {
	if _, err := mgr.GetRateLimit(name); err != nil {
		return fmt.Errorf("rate limit resource '%v' was not found", name)
	}
	return nil
}

// AccessRateLimit attempts to access a rate limit resource by a unique
// identifier and executes a closure function with the rate limit as an
// argument. Returns an error if the rate limit does not exist (or is otherwise
// inaccessible).
func AccessRateLimit(ctx context.Context, mgr types.Manager, name string, fn func(ratelimit.V1)) error {
	if nm, ok := mgr.(interface {
		AccessRateLimit(ctx context.Context, name string, fn func(ratelimit.V1)) error
	}); ok {
		return nm.AccessRateLimit(ctx, name, fn)
	}
	c, err := mgr.GetRateLimit(name)
	if err != nil {
		return err
	}
	if c == nil {
		return component.ErrRateLimitNotFound
	}
	fn(c)
	return nil
}
