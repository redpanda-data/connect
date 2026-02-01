/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

package icebergx

import (
	"path"
	"strings"
)

// PathSegmentKind identifies the type of path segment.
type PathSegmentKind int

const (
	// PathField represents a named struct field.
	PathField PathSegmentKind = iota
	// PathListElement represents an element within a list.
	PathListElement
	// PathMapEntry represents an entry within a map.
	PathMapEntry
)

// PathSegment represents one element in a schema path.
type PathSegment struct {
	Kind PathSegmentKind
	Name string // only set for PathField
}

func (p PathSegment) String() string {
	switch p.Kind {
	case PathField:
		return p.Name
	case PathListElement:
		return "[*]"
	case PathMapEntry:
		return "{}"
	default:
		return "?"
	}
}

// Path represents a traversal to an element within an iceberg schema
type Path []PathSegment

func (p Path) String() string {
	segments := make([]string, len(p))
	for i, seg := range p {
		segments[i] = seg.String()
	}
	return path.Join(segments...)
}

// ParsePath parses a dot-delimited path string into a Path.
// Special segments:
//   - "[*]" represents a list element
//   - "{}" represents a map entry
//   - All other segments are field names
//
// Examples:
//   - "user.name" -> field "user", field "name"
//   - "items.[*].sku" -> field "items", list element, field "sku"
//   - "data.{}.value" -> field "data", map entry, field "value"
func ParsePath(s string) Path {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ".")
	p := make(Path, len(parts))
	for i, part := range parts {
		switch part {
		case "[*]":
			p[i] = PathSegment{Kind: PathListElement}
		case "{}":
			p[i] = PathSegment{Kind: PathMapEntry}
		default:
			p[i] = PathSegment{Kind: PathField, Name: part}
		}
	}
	return p
}
