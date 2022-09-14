package pure

import (
	"context"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/public/bloblang"
	"github.com/benthosdev/benthos/v4/public/service"
)

func TestMappingCreateCrossfire(t *testing.T) {
	tCtx := context.Background()

	inMsg := service.NewMessage(nil)
	inMsg.SetStructuredMut(map[string]any{
		"foo": map[string]any{
			"bar": map[string]any{
				"baz": "original value",
				"qux": "dont change",
			},
		},
	})
	inMsg.MetaSet("foo", "orig1")
	inMsg.MetaSet("bar", "orig2")

	inMsg2 := service.NewMessage([]byte(`{}`))

	exec, err := bloblang.Parse(`
foo = json("foo").from(0)
foo.bar_new = "this is swapped now"
foo.bar.baz = "and this changed"
meta foo = meta("foo").from(0)
meta bar = meta("bar").from(0)
meta baz = "new meta"
`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, service.MessageBatch{inMsg, inMsg2})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 2)

	msgBytes, err := inMsg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`, string(msgBytes))
	v, _ := inMsg.MetaGet("foo")
	assert.Equal(t, "orig1", v)
	v, _ = inMsg.MetaGet("bar")
	assert.Equal(t, "orig2", v)
	v, _ = inMsg.MetaGet("baz")
	assert.Equal(t, "", v)

	msgBytes, err = inMsg2.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{}`, string(msgBytes))
	v, _ = inMsg2.MetaGet("foo")
	assert.Equal(t, "", v)
	v, _ = inMsg2.MetaGet("bar")
	assert.Equal(t, "", v)
	v, _ = inMsg2.MetaGet("baz")
	assert.Equal(t, "", v)

	msgBytes, err = outBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = outBatches[0][0].MetaGet("foo")
	assert.Equal(t, "orig1", v)
	v, _ = outBatches[0][0].MetaGet("bar")
	assert.Equal(t, "orig2", v)
	v, _ = outBatches[0][0].MetaGet("baz")
	assert.Equal(t, "new meta", v)

	msgBytes, err = outBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = outBatches[0][1].MetaGet("foo")
	assert.Equal(t, "orig1", v)
	v, _ = outBatches[0][1].MetaGet("bar")
	assert.Equal(t, "orig2", v)
	v, _ = outBatches[0][1].MetaGet("baz")
	assert.Equal(t, "new meta", v)
}

func TestMappingCreateCustomObject(t *testing.T) {
	tCtx := context.Background()

	part := service.NewMessage(nil)

	gObj := gabs.New()
	_, _ = gObj.ArrayOfSize(3, "foos")

	gObjEle := gabs.New()
	_, _ = gObjEle.Set("FROM NEW OBJECT", "foo")

	_, _ = gObj.S("foos").SetIndex(gObjEle.Data(), 0)
	_, _ = gObj.S("foos").SetIndex(5, 1)

	part.SetStructuredMut(gObj.Data())

	exec, err := bloblang.Parse(`root.foos = this.foos`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, service.MessageBatch{part})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	resPartBytes, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)

	assert.Equal(t, `{"foos":[{"foo":"FROM NEW OBJECT"},5,null]}`, string(resPartBytes))
}

func TestMappingCreateFiltering(t *testing.T) {
	tCtx := context.Background()

	inBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":{"delete":true}}`)),
		service.NewMessage([]byte(`{"foo":{"dont":"delete me"}}`)),
		service.NewMessage([]byte(`{"bar":{"delete":true}}`)),
		service.NewMessage([]byte(`{"bar":{"dont":"delete me"}}`)),
	}

	exec, err := bloblang.Parse(`
root = match {
  (foo | bar).delete.or(false) => deleted(),
}
`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, inBatch)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 2)

	assert.NoError(t, outBatches[0][0].GetError())
	assert.NoError(t, outBatches[0][1].GetError())

	msgBytes, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"foo":{"dont":"delete me"}}`, string(msgBytes))

	msgBytes, err = outBatches[0][1].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `{"bar":{"dont":"delete me"}}`, string(msgBytes))
}

func TestMappingCreateFilterAll(t *testing.T) {
	tCtx := context.Background()

	inBatch := service.MessageBatch{
		service.NewMessage([]byte(`{"foo":{"delete":true}}`)),
		service.NewMessage([]byte(`{"foo":{"dont":"delete me"}}`)),
		service.NewMessage([]byte(`{"bar":{"delete":true}}`)),
		service.NewMessage([]byte(`{"bar":{"dont":"delete me"}}`)),
	}

	exec, err := bloblang.Parse(`root = deleted()`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, inBatch)
	assert.NoError(t, err)
	assert.Empty(t, outBatches)
}

func TestMappingCreateJSONError(t *testing.T) {
	tCtx := context.Background()

	msg := service.MessageBatch{
		service.NewMessage([]byte(`this is not valid json`)),
	}

	exec, err := bloblang.Parse(`foo = json().bar`)
	require.NoError(t, err)

	proc := newMapping(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, msg)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	msgBytes, err := outBatches[0][0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, `this is not valid json`, string(msgBytes))

	err = outBatches[0][0].GetError()
	require.Error(t, err)
	assert.Equal(t, `failed assignment (line 1): invalid character 'h' in literal true (expecting 'r')`, err.Error())
}
