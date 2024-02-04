package testutil

import (
	"fmt"

	"github.com/benthosdev/benthos/v4/internal/bundle"
	"github.com/benthosdev/benthos/v4/internal/component/buffer"
	"github.com/benthosdev/benthos/v4/internal/component/cache"
	"github.com/benthosdev/benthos/v4/internal/component/input"
	"github.com/benthosdev/benthos/v4/internal/component/metrics"
	"github.com/benthosdev/benthos/v4/internal/component/output"
	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/component/ratelimit"
	"github.com/benthosdev/benthos/v4/internal/component/tracer"
	"github.com/benthosdev/benthos/v4/internal/config"
	"github.com/benthosdev/benthos/v4/internal/docs"
	"github.com/benthosdev/benthos/v4/internal/manager"
	"github.com/benthosdev/benthos/v4/internal/stream"
)

func BufferFromYAML(confStr string, args ...any) (buffer.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return buffer.Config{}, err
	}
	return buffer.FromAny(bundle.GlobalEnvironment, node)
}

func CacheFromYAML(confStr string, args ...any) (cache.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return cache.Config{}, err
	}
	return cache.FromAny(bundle.GlobalEnvironment, node)
}

func InputFromYAML(confStr string, args ...any) (input.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return input.Config{}, err
	}
	return input.FromAny(bundle.GlobalEnvironment, node)
}

func MetricsFromYAML(confStr string, args ...any) (metrics.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return metrics.Config{}, err
	}
	return metrics.FromAny(bundle.GlobalEnvironment, node)
}

func OutputFromYAML(confStr string, args ...any) (output.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return output.Config{}, err
	}
	return output.FromAny(bundle.GlobalEnvironment, node)
}

func ProcessorFromYAML(confStr string, args ...any) (processor.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return processor.Config{}, err
	}
	return processor.FromAny(bundle.GlobalEnvironment, node)
}

func RateLimitFromYAML(confStr string, args ...any) (ratelimit.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return ratelimit.Config{}, err
	}
	return ratelimit.FromAny(bundle.GlobalEnvironment, node)
}

func TracerFromYAML(confStr string, args ...any) (tracer.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return tracer.Config{}, err
	}
	return tracer.FromAny(bundle.GlobalEnvironment, node)
}

func ManagerFromYAML(confStr string, args ...any) (manager.ResourceConfig, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return manager.ResourceConfig{}, err
	}
	return manager.FromAny(bundle.GlobalEnvironment, node)
}

func StreamFromYAML(confStr string, args ...any) (stream.Config, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return stream.Config{}, err
	}

	var rawSource any
	_ = node.Decode(&rawSource)

	pConf, err := stream.Spec().ParsedConfigFromAny(node)
	if err != nil {
		return stream.Config{}, err
	}
	return stream.FromParsed(bundle.GlobalEnvironment, pConf, rawSource)
}

func ConfigFromYAML(confStr string, args ...any) (config.Type, error) {
	node, err := docs.UnmarshalYAML(fmt.Appendf(nil, confStr, args...))
	if err != nil {
		return config.Type{}, err
	}

	var rawSource any
	_ = node.Decode(&rawSource)

	pConf, err := config.Spec().ParsedConfigFromAny(node)
	if err != nil {
		return config.Type{}, err
	}
	return config.FromParsed(bundle.GlobalEnvironment, pConf, rawSource)
}
