// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"fmt"
	"sort"
	"strings"
)

// metaKeys are Kafka Connect properties handled outside connector mappers
// (or framework-level) and therefore never reported as "unmapped".
var metaKeys = map[string]bool{
	"connector.class": true,
	"name":            true,
	"tasks.max":       true,
	"transforms":      true,
}

// metaPrefixes are property prefixes handled by converters/SMTs.
var metaPrefixes = []string{"key.converter", "value.converter", "header.converter", "transforms."}

// MapCtx is the per-conversion scratchpad threaded through every mapper.
type MapCtx struct {
	cfg      ConnectConfig
	warnings []Warning
	consumed map[string]bool
}

func newMapCtx(cfg ConnectConfig) *MapCtx {
	return &MapCtx{cfg: cfg, consumed: map[string]bool{}}
}

// Warn records a warning. Callers that want an inline TODO should also attach
// a comment to the relevant yaml.Node.
func (c *MapCtx) Warn(field, msg string) {
	c.warnings = append(c.warnings, Warning{Field: field, Message: msg})
}

// Warnings returns all recorded warnings.
func (c *MapCtx) Warnings() []Warning { return c.warnings }

// String returns the prop value as a string and marks the key consumed.
func (c *MapCtx) String(key string) (string, bool) {
	c.consumed[key] = true
	v, ok := c.cfg.Props[key]
	if !ok {
		return "", false
	}
	return fmt.Sprint(v), true
}

// consume marks a key consumed without reading it.
func (c *MapCtx) consume(key string) { c.consumed[key] = true }

// isMeta reports whether a key is framework/meta and should be ignored by the
// unmapped sweep.
func isMeta(key string) bool {
	if metaKeys[key] {
		return true
	}
	for _, p := range metaPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// Unmapped returns sorted prop keys that were never consumed and are not meta.
func (c *MapCtx) Unmapped() []string {
	var out []string
	for k := range c.cfg.Props {
		if c.consumed[k] || isMeta(k) {
			continue
		}
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
