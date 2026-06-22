// Copyright 2026 Redpanda Data, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/redpanda-data/connect/blob/main/licenses/rcl.md

package connectconverter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func newTestCtx(props map[string]any) *MapCtx {
	return newMapCtx(ConnectConfig{Name: "c", Class: "io.example.Foo", Props: props})
}

func TestMapCtxStringConsumes(t *testing.T) {
	c := newTestCtx(map[string]any{"a": "x", "b": "y"})
	v, ok := c.String("a")
	assert.True(t, ok)
	assert.Equal(t, "x", v)
	// "a" consumed, "b" not.
	assert.Equal(t, []string{"b"}, c.Unmapped())
}

func TestMapCtxStringMissing(t *testing.T) {
	c := newTestCtx(map[string]any{})
	_, ok := c.String("missing")
	assert.False(t, ok)
}

func TestMapCtxUnmappedExcludesMeta(t *testing.T) {
	c := newTestCtx(map[string]any{
		"connector.class": "io.example.Foo",
		"name":            "c",
		"tasks.max":       "1",
		"transforms":      "a",
		"key.converter":   "x",
		"value.converter": "y",
		"real.field":      "keep",
	})
	assert.Equal(t, []string{"real.field"}, c.Unmapped())
}

func TestMapCtxWarn(t *testing.T) {
	c := newTestCtx(map[string]any{})
	c.Warn("f", "bad")
	assert.Equal(t, []Warning{{Field: "f", Message: "bad"}}, c.Warnings())
}
