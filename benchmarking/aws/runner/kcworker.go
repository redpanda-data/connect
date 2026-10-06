// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License included
// in the licenses/BSL.md file.

package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Keys of the scenario's kafka_connect: block that this file interprets. The
// block is also read by renderKCConfig for `config` (a shallow merge over the
// connector properties); the keys below shape the Kafka Connect WORKER and
// the spec choice instead.
const (
	kcKeySpec          = "spec"
	kcKeyWorkerProps   = "worker_properties"
	kcKeyJVMPerfOpts   = "jvm_performance_opts"
	kcKeyEnv           = "env"
	kcKeyConnectorConf = "config"
)

var (
	kcWorkerPropKeyRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	kcEnvKeyRe        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// kcProtectedWorkerProps are worker properties the bench scripts and cloud-init
// own. Overriding them would detach the per-point JVM from the broker, the
// plugin directory, or the REST port the script polls.
var kcProtectedWorkerProps = map[string]bool{
	"bootstrap.servers":         true,
	"plugin.path":               true,
	"rest.port":                 true,
	"listeners":                 true,
	"group.id":                  true,
	"offset.storage.topic":      true,
	"config.storage.topic":      true,
	"status.storage.topic":      true,
	"rest.advertised.host.name": true,
}

// kcKV is one ordered key/value pair. Slices of these (sorted by key) keep
// rendered scripts deterministic across Go's randomized map iteration.
type kcKV struct {
	Key, Value string
}

// kcWorkerOverrides is the per-scenario Kafka Connect worker shape. The zero
// value (what every pre-existing scenario gets) renders a script
// byte-identical to the one that existed before these knobs.
type kcWorkerOverrides struct {
	// Props are written to a per-point copy of worker.properties, replacing
	// same-named entries of the cloud-init file, which is left untouched.
	Props []kcKV
	// JVMPerfOpts is exported as KAFKA_JVM_PERFORMANCE_OPTS and replaces
	// Kafka's default performance flags wholesale. Heap (-Xmx) is separate and
	// stays the bench's per-vCPU sizing.
	JVMPerfOpts string
	// Env is exported into the JVM's environment.
	Env []kcKV
}

// empty reports whether the scenario changes nothing about the worker.
func (o kcWorkerOverrides) empty() bool {
	return len(o.Props) == 0 && o.JVMPerfOpts == "" && len(o.Env) == 0
}

// kcSpecKey is the kcConnectorSpecs key a scenario renders: kafka_connect.spec
// when set, else the connector name (the historical behaviour).
func kcSpecKey(s *Scenario) string {
	if s.KafkaConnect == nil {
		return s.Connector
	}
	if v, ok := s.KafkaConnect[kcKeySpec].(string); ok && v != "" {
		return v
	}
	return s.Connector
}

// kcWorkerOverridesFor extracts and validates the worker knobs of a scenario.
func kcWorkerOverridesFor(s *Scenario) (kcWorkerOverrides, error) {
	var o kcWorkerOverrides
	if s == nil || s.KafkaConnect == nil {
		return o, nil
	}

	if raw, present := s.KafkaConnect[kcKeyWorkerProps]; present {
		m, ok := raw.(map[string]any)
		if !ok {
			return o, fmt.Errorf("kafka_connect.%s must be a map of property -> value (got %T)", kcKeyWorkerProps, raw)
		}
		for k, v := range m {
			if !kcWorkerPropKeyRe.MatchString(k) {
				return o, fmt.Errorf("kafka_connect.%s: key %q is not a valid property name", kcKeyWorkerProps, k)
			}
			if kcProtectedWorkerProps[k] {
				return o, fmt.Errorf("kafka_connect.%s: %q is managed by the bench (cloud-init / bench script) and cannot be overridden", kcKeyWorkerProps, k)
			}
			val, err := kcScalarString(v)
			if err != nil {
				return o, fmt.Errorf("kafka_connect.%s[%q]: %w", kcKeyWorkerProps, k, err)
			}
			o.Props = append(o.Props, kcKV{k, val})
		}
		sortKV(o.Props)
	}

	if raw, present := s.KafkaConnect[kcKeyJVMPerfOpts]; present {
		str, ok := raw.(string)
		if !ok {
			return o, fmt.Errorf("kafka_connect.%s must be a string (got %T)", kcKeyJVMPerfOpts, raw)
		}
		if strings.ContainsAny(str, "\r\n") {
			return o, fmt.Errorf("kafka_connect.%s must be a single line", kcKeyJVMPerfOpts)
		}
		o.JVMPerfOpts = strings.TrimSpace(str)
	}

	if raw, present := s.KafkaConnect[kcKeyEnv]; present {
		m, ok := raw.(map[string]any)
		if !ok {
			return o, fmt.Errorf("kafka_connect.%s must be a map of variable -> value (got %T)", kcKeyEnv, raw)
		}
		for k, v := range m {
			if !kcEnvKeyRe.MatchString(k) {
				return o, fmt.Errorf("kafka_connect.%s: %q is not a valid environment variable name", kcKeyEnv, k)
			}
			// KAFKA_HEAP_OPTS is the bench's per-vCPU heap sizing; the JVM
			// perf flags have their own field so they cannot be set twice.
			if k == "KAFKA_HEAP_OPTS" || k == "KAFKA_JVM_PERFORMANCE_OPTS" {
				return o, fmt.Errorf("kafka_connect.%s: %s is managed by the bench (use kafka_connect.%s for JVM flags; heap follows go_mem_limit_per_vcpu)", kcKeyEnv, k, kcKeyJVMPerfOpts)
			}
			val, err := kcScalarString(v)
			if err != nil {
				return o, fmt.Errorf("kafka_connect.%s[%q]: %w", kcKeyEnv, k, err)
			}
			o.Env = append(o.Env, kcKV{k, val})
		}
		sortKV(o.Env)
	}
	return o, nil
}

func sortKV(kv []kcKV) {
	sort.Slice(kv, func(i, j int) bool { return kv[i].Key < kv[j].Key })
}

// kcScalarString renders a YAML scalar as the string a properties file or an
// environment variable holds. Multi-line values are rejected because they
// would break the line-oriented properties file and its heredoc.
func kcScalarString(v any) (string, error) {
	switch x := v.(type) {
	case string:
		if strings.ContainsAny(x, "\r\n") {
			return "", fmt.Errorf("value must be a single line")
		}
		return x, nil
	case int, int64, uint64, float64, bool:
		return fmt.Sprint(x), nil
	default:
		return "", fmt.Errorf("value must be a string, number or bool (got %T)", v)
	}
}

// validateKafkaConnect checks the kafka_connect: block at scenario load, so a
// typo fails in milliseconds rather than after infra apply.
func validateKafkaConnect(s *Scenario) error {
	if s.KafkaConnect == nil {
		return nil
	}
	if raw, present := s.KafkaConnect[kcKeySpec]; present {
		key, ok := raw.(string)
		if !ok || key == "" {
			return fmt.Errorf("kafka_connect.%s must be a non-empty string", kcKeySpec)
		}
		if _, known := kcConnectorSpecFor(key); !known {
			return fmt.Errorf("kafka_connect.%s %q has no kcConnectorSpec; known: %s", kcKeySpec, key, strings.Join(kcSpecNames(), ", "))
		}
	}
	_, err := kcWorkerOverridesFor(s)
	return err
}

func kcSpecNames() []string {
	names := make([]string, 0, len(kcConnectorSpecs))
	for k := range kcConnectorSpecs {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

// shellSingleQuote quotes s as one shell word.
func shellSingleQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'"'"'`) + "'"
}
