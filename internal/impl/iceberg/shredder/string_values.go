// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

package shredder

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/iceberg-go"
)

// DefaultTimestampLayout is the layout used to parse string values destined
// for timestamp columns when no timestamp_format is configured.
const DefaultTimestampLayout = time.RFC3339Nano

// ParseStringValue converts a string value into the Go type expected by the
// column type t, so that sources which stringify numbers and timestamps can be
// written into typed columns. The value is returned unchanged unless it is a
// string and t is an integer, float or timestamp type. Whitespace is not
// trimmed. Layouts without a zone are read as UTC. An empty timestampLayout
// means [DefaultTimestampLayout].
func ParseStringValue(value any, t iceberg.Type, timestampLayout string) (any, error) {
	s, isString := value.(string)
	if !isString {
		return value, nil
	}

	switch t.(type) {
	case iceberg.Int32Type, iceberg.Int64Type:
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as %s: %w", s, t, err)
		}
		return i, nil
	case iceberg.Float32Type, iceberg.Float64Type:
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as %s: %w", s, t, err)
		}
		return f, nil
	case iceberg.TimestampType, iceberg.TimestampTzType:
		if timestampLayout == "" {
			timestampLayout = DefaultTimestampLayout
		}
		ts, err := time.Parse(timestampLayout, s)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as %s with timestamp_format %q: %w", s, t, timestampLayout, err)
		}
		return ts, nil
	default:
		return value, nil
	}
}
