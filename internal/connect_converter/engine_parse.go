// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"encoding/json"
	"errors"
	"fmt"
)

// stripLineComments removes // line comments that appear outside of JSON
// string literals, so JSONC-style configs parse. // inside a string value
// (e.g. a jdbc:postgresql://host URL) is preserved.
func stripLineComments(in []byte) []byte {
	out := make([]byte, 0, len(in))
	inString := false
	escaped := false
	for i := 0; i < len(in); i++ {
		c := in[i]
		if inString {
			out = append(out, c)
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
			continue
		}
		if c == '"' {
			inString = true
			out = append(out, c)
			continue
		}
		if c == '/' && i+1 < len(in) && in[i+1] == '/' {
			for i < len(in) && in[i] != '\n' {
				i++
			}
			if i < len(in) {
				out = append(out, '\n')
			}
			continue
		}
		out = append(out, c)
	}
	return out
}

// parse normalizes a Kafka Connect config. It accepts the REST-wrapped form
// ({"name":..., "config":{...}}) and the flat form (a bare property map).
func parse(input []byte) (ConnectConfig, error) {
	var raw map[string]any
	if err := json.Unmarshal(stripLineComments(input), &raw); err != nil {
		return ConnectConfig{}, fmt.Errorf("invalid JSON: %w", err)
	}

	props := raw
	name, _ := raw["name"].(string)

	// REST-wrapped form: unwrap "config".
	if cfg, ok := raw["config"].(map[string]any); ok {
		props = cfg
	}

	class, _ := props["connector.class"].(string)
	if class == "" {
		return ConnectConfig{}, errors.New("missing required field: connector.class")
	}

	// Prefer the wrapper name; fall back to a name inside config.
	if name == "" {
		name, _ = props["name"].(string)
	}

	return ConnectConfig{Name: name, Class: class, Props: props}, nil
}
