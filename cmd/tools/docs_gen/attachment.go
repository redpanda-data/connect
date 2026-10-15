// Copyright 2026 Redpanda Data, Inc.
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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// connectJSONPath is where a release's connector data goes, under the
// components module. The docs site serves it as
// /connect/components/_attachments/connect-<version>.json, which the Bloblang
// playground and other site tools read.
func connectJSONPath(version string) string {
	return "attachments/connect-" + version + ".json"
}

// renderConnectJSON returns the connector data the docs site publishes for a
// release: the schema that `redpanda-connect list --format json-full` prints,
// with the version and date, and on every component and config object
// whether Redpanda Cloud runs it (cloudSupported) and whether only cgo builds
// have it (requiresCgo). Components the standard binary lacks but Cloud runs
// also get cloudOnly. raw is the docs_gen schema, which has every component.
func renderConnectJSON(raw, cloudRaw []byte, plat platformSet, notInBinary map[string]bool) (string, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return "", err
	}
	var cloudDoc map[string]json.RawMessage
	if err := json.Unmarshal(cloudRaw, &cloudDoc); err != nil {
		return "", err
	}

	for _, key := range componentKeys {
		if _, ok := doc[key]; !ok {
			continue
		}
		comps, err := decodeObjects(doc[key])
		if err != nil {
			return "", fmt.Errorf("%v: %w", key, err)
		}
		for _, c := range comps {
			name, _ := c["name"].(string)
			p := plat.get(key, name)
			c["cloudSupported"] = p.Cloud
			c["requiresCgo"] = p.CgoOnly
			if p.Cloud && notInBinary[componentKey(key, name)] {
				c["cloudOnly"] = true
			}
		}
		if doc[key], err = marshalRaw(comps); err != nil {
			return "", err
		}
	}

	config, err := decodeObjects(doc["config"])
	if err != nil {
		return "", fmt.Errorf("config: %w", err)
	}
	cloudConfig, err := decodeObjects(cloudDoc["config"])
	if err != nil {
		return "", fmt.Errorf("cloud config: %w", err)
	}
	inCloud := map[string]bool{}
	for _, c := range cloudConfig {
		if name, _ := c["name"].(string); name != "" {
			inCloud[name] = true
		}
	}
	for _, c := range config {
		name, _ := c["name"].(string)
		c["cloudSupported"] = inCloud[name]
		c["requiresCgo"] = false
	}
	if doc["config"], err = marshalRaw(config); err != nil {
		return "", err
	}

	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return out.String(), nil
}

// marshalRaw encodes v without escaping <, >, and &, which descriptions use.
func marshalRaw(v any) (json.RawMessage, error) {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(b.Bytes(), "\n"), nil
}

// decodeObjects decodes a JSON array of objects, keeping numbers exactly as
// written. A missing value decodes to no objects.
func decodeObjects(raw json.RawMessage) ([]map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var objs []map[string]any
	if err := dec.Decode(&objs); err != nil {
		return nil, err
	}
	return objs, nil
}
