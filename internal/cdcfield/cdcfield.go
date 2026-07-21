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

// Package cdcfield provides helpers for migrating CDC input config fields to the
// canonical names defined in the CDC Connector Standard (CONTRIBUTING.md §5.3)
// without breaking existing configs.
//
// The migration shape (CONTRIBUTING.md §5.3.2) is: add the canonical-named
// field, keep the old name accepted, and mark the old one Deprecated(). Both the
// canonical and deprecated fields must be declared Optional() with no Default()
// so that ParsedConfig.Contains reports whether the user actually set each one
// (benthos hydrates declared defaults into the parsed config, which would
// otherwise make a defaulted field indistinguishable from an explicitly-set
// one). The resolver helpers below apply the real default in code, prefer the
// canonical name, fall back to the deprecated name, and error when both are set
// to conflicting values.
package cdcfield

import (
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// ResolveInt reads an int field that has been renamed from deprecated to
// canonical. It prefers the canonical name, falls back to the deprecated name,
// errors if both are set to conflicting values, and returns def if neither is
// set. Both fields must be declared Optional() (no Default()).
func ResolveInt(conf *service.ParsedConfig, canonical, deprecated string, def int) (int, error) {
	hasC, hasD := conf.Contains(canonical), conf.Contains(deprecated)
	switch {
	case hasC && hasD:
		c, err := conf.FieldInt(canonical)
		if err != nil {
			return 0, err
		}
		d, err := conf.FieldInt(deprecated)
		if err != nil {
			return 0, err
		}
		if c != d {
			return 0, fmt.Errorf("conflicting values for %q (%d) and its deprecated alias %q (%d): set only %q", canonical, c, deprecated, d, canonical)
		}
		return c, nil
	case hasD:
		return conf.FieldInt(deprecated)
	case hasC:
		return conf.FieldInt(canonical)
	default:
		return def, nil
	}
}

// ResolveString reads a string field that has been renamed from deprecated to
// canonical. It prefers the canonical name, falls back to the deprecated name,
// errors if both are set to conflicting values, and returns def if neither is
// set. Both fields must be declared Optional() (no Default()).
func ResolveString(conf *service.ParsedConfig, canonical, deprecated, def string) (string, error) {
	hasC, hasD := conf.Contains(canonical), conf.Contains(deprecated)
	switch {
	case hasC && hasD:
		c, err := conf.FieldString(canonical)
		if err != nil {
			return "", err
		}
		d, err := conf.FieldString(deprecated)
		if err != nil {
			return "", err
		}
		if c != d {
			return "", fmt.Errorf("conflicting values for %q (%q) and its deprecated alias %q (%q): set only %q", canonical, c, deprecated, d, canonical)
		}
		return c, nil
	case hasD:
		return conf.FieldString(deprecated)
	case hasC:
		return conf.FieldString(canonical)
	default:
		return def, nil
	}
}
