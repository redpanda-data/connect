package service

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ibloblang "github.com/benthosdev/benthos/v4/internal/bloblang"
	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestMessageCopyAirGap(t *testing.T) {
	p := message.NewPart([]byte("hello world"))
	p.MetaSetMut("foo", "bar")
	g1 := NewInternalMessage(p.ShallowCopy())
	g2 := g1.Copy()

	b := p.AsBytes()
	v, _ := p.MetaGetMut("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, err := g1.AsBytes()
	v, _ = g1.MetaGet("foo")
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, err = g2.AsBytes()
	v, _ = g2.MetaGetMut("foo")
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	g2.SetBytes([]byte("and now this"))
	g2.MetaSetMut("foo", "baz")

	b = p.AsBytes()
	v, _ = p.MetaGetMut("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, err = g1.AsBytes()
	v, _ = g1.MetaGetMut("foo")
	require.NoError(t, err)
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, err = g2.AsBytes()
	v, _ = g2.MetaGetMut("foo")
	require.NoError(t, err)
	assert.Equal(t, "and now this", string(b))
	assert.Equal(t, "baz", v)

	g1.SetBytes([]byte("but not this"))
	g1.MetaSetMut("foo", "buz")

	b = p.AsBytes()
	v, _ = p.MetaGetMut("foo")
	assert.Equal(t, "hello world", string(b))
	assert.Equal(t, "bar", v)

	b, err = g1.AsBytes()
	v, _ = g1.MetaGetMut("foo")
	require.NoError(t, err)
	assert.Equal(t, "but not this", string(b))
	assert.Equal(t, "buz", v)

	b, err = g2.AsBytes()
	v, _ = g2.MetaGetMut("foo")
	require.NoError(t, err)
	assert.Equal(t, "and now this", string(b))
	assert.Equal(t, "baz", v)
}

func TestMessageQuery(t *testing.T) {
	p := message.NewPart([]byte(`{"foo":"bar"}`))
	p.MetaSetMut("foo", "bar")
	p.MetaSetMut("bar", "baz")
	g1 := NewInternalMessage(p)

	b, err := g1.AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, `{"foo":"bar"}`, string(b))

	s, err := g1.AsStructured()
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "bar"}, s)

	m, ok := g1.MetaGetMut("foo")
	assert.True(t, ok)
	assert.Equal(t, "bar", m)

	seen := map[string]any{}
	err = g1.MetaWalkMut(func(k string, v any) error {
		seen[k] = v
		return errors.New("stop")
	})
	assert.EqualError(t, err, "stop")
	assert.Len(t, seen, 1)

	seen = map[string]any{}
	err = g1.MetaWalkMut(func(k string, v any) error {
		seen[k] = v
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"foo": "bar",
		"bar": "baz",
	}, seen)
}

func TestMessageQueryValue(t *testing.T) {
	msg := NewMessage(nil)
	msg.SetStructured(map[string]any{
		"content": "hello world",
	})

	tests := map[string]struct {
		mapping string
		exp     any
		err     string
	}{
		"returns string": {
			mapping: `root = json("content")`,
			exp:     "hello world",
		},
		"returns integer": {
			mapping: `root = json("content").length()`,
			exp:     int64(11),
		},
		"returns float": {
			mapping: `root = json("content").length() / 2`,
			exp:     float64(5.5),
		},
		"returns bool": {
			mapping: `root = json("content").length() > 0`,
			exp:     true,
		},
		"returns bytes": {
			mapping: `root = content()`,
			exp:     []byte(`{"content":"hello world"}`),
		},
		"returns nil": {
			mapping: `root = null`,
			exp:     nil,
		},
		"returns null string": {
			mapping: `root = "null"`,
			exp:     "null",
		},
		"returns an array": {
			mapping: `root = [ json("content") ]`,
			exp:     []any{"hello world"},
		},
		"returns an object": {
			mapping: `root.new_content = json("content")`,
			exp:     map[string]any{"new_content": "hello world"},
		},
		"returns an error if the mapping throws": {
			mapping: `root = throw("kaboom")`,
			exp:     nil,
			err:     "failed assignment (line 1): kaboom",
		},
		"returns an error if the root is deleted": {
			mapping: `root = deleted()`,
			exp:     nil,
			err:     "root was deleted",
		},
		"doesn't error out if a field is deleted": {
			mapping: `root.foo = deleted()`,
			exp:     map[string]any{},
			err:     "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			blobl, err := bloblang.Parse(test.mapping)
			require.NoError(t, err)

			res, err := msg.BloblangQueryValue(blobl)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, test.exp, res)
		})
	}
}

func TestMessageMutate(t *testing.T) {
	p := message.NewPart([]byte(`not a json doc`))
	p.MetaSetMut("foo", "bar")
	p.MetaSetMut("bar", "baz")
	g1 := NewInternalMessage(p.ShallowCopy())

	_, err := g1.AsStructured()
	assert.Error(t, err)

	g1.SetStructured(map[string]any{
		"foo": "bar",
	})
	assert.Equal(t, "not a json doc", string(p.AsBytes()))

	s, err := g1.AsStructured()
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"foo": "bar",
	}, s)

	g1.SetBytes([]byte("foo bar baz"))
	assert.Equal(t, "not a json doc", string(p.AsBytes()))

	_, err = g1.AsStructured()
	assert.Error(t, err)

	b, err := g1.AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, "foo bar baz", string(b))

	g1.MetaDelete("foo")

	seen := map[string]any{}
	err = g1.MetaWalkMut(func(k string, v any) error {
		seen[k] = v
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"bar": "baz"}, seen)

	g1.MetaSetMut("foo", "new bar")

	seen = map[string]any{}
	err = g1.MetaWalkMut(func(k string, v any) error {
		seen[k] = v
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "new bar", "bar": "baz"}, seen)
}

func TestNewMessageMutate(t *testing.T) {
	g0 := NewMessage([]byte(`not a json doc`))
	g0.MetaSetMut("foo", "bar")
	g0.MetaSetMut("bar", "baz")

	g1 := g0.Copy()

	_, err := g1.AsStructured()
	assert.Error(t, err)

	g1.SetStructured(map[string]any{
		"foo": "bar",
	})
	g0Bytes, err := g0.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "not a json doc", string(g0Bytes))

	s, err := g1.AsStructuredMut()
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{
		"foo": "bar",
	}, s)

	g1.SetBytes([]byte("foo bar baz"))
	g0Bytes, err = g0.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "not a json doc", string(g0Bytes))

	_, err = g1.AsStructured()
	assert.Error(t, err)

	b, err := g1.AsBytes()
	assert.NoError(t, err)
	assert.Equal(t, "foo bar baz", string(b))

	g1.MetaDelete("foo")

	seen := map[string]any{}
	err = g1.MetaWalkMut(func(k string, v any) error {
		seen[k] = v
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"bar": "baz"}, seen)

	g1.MetaSetMut("foo", "new bar")

	seen = map[string]any{}
	err = g1.MetaWalkMut(func(k string, v any) error {
		seen[k] = v
		return nil
	})
	assert.NoError(t, err)
	assert.Equal(t, map[string]any{"foo": "new bar", "bar": "baz"}, seen)
}

func TestMessageMapping(t *testing.T) {
	part := NewMessage(nil)
	part.SetStructured(map[string]any{
		"content": "hello world",
	})

	blobl, err := bloblang.Parse("root.new_content = this.content.uppercase()")
	require.NoError(t, err)

	res, err := part.BloblangQuery(blobl)
	require.NoError(t, err)

	resI, err := res.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"new_content": "HELLO WORLD",
	}, resI)
}

func TestMessageBatchMapping(t *testing.T) {
	partOne := NewMessage(nil)
	partOne.SetStructured(map[string]any{
		"content": "hello world 1",
	})

	partTwo := NewMessage(nil)
	partTwo.SetStructured(map[string]any{
		"content": "hello world 2",
	})

	blobl, err := bloblang.Parse(`root.new_content = json("content").from_all().join(" - ")`)
	require.NoError(t, err)

	res, err := MessageBatch{partOne, partTwo}.BloblangQuery(0, blobl)
	require.NoError(t, err)

	resI, err := res.AsStructured()
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"new_content": "hello world 1 - hello world 2",
	}, resI)
}

func TestMessageBatchQueryValue(t *testing.T) {
	partOne := NewMessage(nil)
	partOne.SetStructured(map[string]any{
		"content": "hello world 1",
	})

	partTwo := NewMessage(nil)
	partTwo.SetStructured(map[string]any{
		"content": "hello world 2",
	})

	tests := map[string]struct {
		mapping    string
		batchIndex int
		exp        any
		err        string
	}{
		"returns string": {
			mapping: `root = json("content")`,
			exp:     "hello world 1",
		},
		"returns integer": {
			mapping: `root = json("content").length()`,
			exp:     int64(13),
		},
		"returns float": {
			mapping: `root = json("content").length() / 2`,
			exp:     float64(6.5),
		},
		"returns bool": {
			mapping: `root = json("content").length() > 0`,
			exp:     true,
		},
		"returns bytes": {
			mapping: `root = content()`,
			exp:     []byte(`{"content":"hello world 1"}`),
		},
		"returns nil": {
			mapping: `root = null`,
			exp:     nil,
		},
		"returns null string": {
			mapping: `root = "null"`,
			exp:     "null",
		},
		"returns an array": {
			mapping: `root = [ json("content") ]`,
			exp:     []any{"hello world 1"},
		},
		"returns an object": {
			mapping: `root.new_content = json("content")`,
			exp:     map[string]any{"new_content": "hello world 1"},
		},
		"supports batch-wide queries": {
			mapping: `root.new_content = json("content").from_all().join(" - ")`,
			exp:     map[string]any{"new_content": "hello world 1 - hello world 2"},
		},
		"handles the specified message index correctly": {
			mapping:    `root = json("content")`,
			batchIndex: 1,
			exp:        "hello world 2",
		},
		"returns an error if the mapping throws": {
			mapping: `root = throw("kaboom")`,
			exp:     nil,
			err:     "failed assignment (line 1): kaboom",
		},
		"returns an error if the root is deleted": {
			mapping: `root = deleted()`,
			exp:     nil,
			err:     "root was deleted",
		},
		"doesn't error out if a field is deleted": {
			mapping: `root.foo = deleted()`,
			exp:     map[string]any{},
			err:     "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			blobl, err := bloblang.Parse(test.mapping)
			require.NoError(t, err)

			res, err := MessageBatch{partOne, partTwo}.BloblangQueryValue(test.batchIndex, blobl)
			if test.err != "" {
				require.ErrorContains(t, err, test.err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, test.exp, res)
		})
	}
}

func BenchmarkMessageMappingNew(b *testing.B) {
	part := NewMessage(nil)
	part.SetStructured(map[string]any{
		"content": "hello world",
	})

	blobl, err := bloblang.Parse("root.new_content = this.content.uppercase()")
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := part.BloblangQuery(blobl)
		require.NoError(b, err)

		resI, err := res.AsStructured()
		require.NoError(b, err)
		assert.Equal(b, map[string]any{
			"new_content": "HELLO WORLD",
		}, resI)
	}
}

func BenchmarkMessageMappingOld(b *testing.B) {
	part := message.NewPart(nil)
	part.SetStructured(map[string]any{
		"content": "hello world",
	})

	msg := message.Batch{part}

	blobl, err := ibloblang.GlobalEnvironment().NewMapping("root.new_content = this.content.uppercase()")
	require.NoError(b, err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := blobl.MapPart(0, msg)
		require.NoError(b, err)

		resI, err := res.AsStructuredMut()
		require.NoError(b, err)
		assert.Equal(b, map[string]any{
			"new_content": "HELLO WORLD",
		}, resI)
	}
}
