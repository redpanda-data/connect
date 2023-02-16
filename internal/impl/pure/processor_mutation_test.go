package pure

import (
	"context"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/message"
	"github.com/benthosdev/benthos/v4/public/bloblang"
)

func TestMutationCreateCrossfire(t *testing.T) {
	tCtx := context.Background()

	inMsg := message.NewPart(nil)
	inMsg.SetStructuredMut(map[string]any{
		"foo": map[string]any{
			"bar": map[string]any{
				"baz": "original value",
				"qux": "dont change",
			},
		},
	})
	inMsg.MetaSetMut("foo", "orig1")
	inMsg.MetaSetMut("bar", "orig2")

	inMsg2 := message.NewPart([]byte(`{}`))

	exec, err := bloblang.Parse(`
a = batch_index()
foo = json("foo").from(0)
foo.bar_new = "this is swapped now"
foo.bar.baz = "and this changed"
meta foo = meta("foo").from(0)
meta bar = meta("bar").from(0)
meta baz = "new meta"
`)
	require.NoError(t, err)

	proc := newMutation(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, nil, message.Batch{inMsg, inMsg2})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 2)

	msgBytes := inMsg.AsBytes()
	assert.Equal(t, `{"a":0,"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ := inMsg.MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = inMsg.MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	v, _ = inMsg.MetaGetMut("baz")
	assert.Equal(t, "new meta", v)

	msgBytes = inMsg2.AsBytes()
	assert.Equal(t, `{"a":1,"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = inMsg2.MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = inMsg2.MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	v, _ = inMsg2.MetaGetMut("baz")
	assert.Equal(t, "new meta", v)

	msgBytes = outBatches[0][0].AsBytes()
	assert.Equal(t, `{"a":0,"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = outBatches[0][0].MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = outBatches[0][0].MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	v, _ = outBatches[0][0].MetaGetMut("baz")
	assert.Equal(t, "new meta", v)

	msgBytes = outBatches[0][1].AsBytes()
	assert.Equal(t, `{"a":1,"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(msgBytes))
	v, _ = outBatches[0][1].MetaGetMut("foo")
	assert.Equal(t, "orig1", v)
	v, _ = outBatches[0][1].MetaGetMut("bar")
	assert.Equal(t, "orig2", v)
	v, _ = outBatches[0][1].MetaGetMut("baz")
	assert.Equal(t, "new meta", v)
}

func TestMutationCreateCustomObject(t *testing.T) {
	tCtx := context.Background()

	part := message.NewPart(nil)

	gObj := gabs.New()
	_, _ = gObj.ArrayOfSize(3, "foos")

	gObjEle := gabs.New()
	_, _ = gObjEle.Set("FROM NEW OBJECT", "foo")

	_, _ = gObj.S("foos").SetIndex(gObjEle.Data(), 0)
	_, _ = gObj.S("foos").SetIndex(5, 1)

	part.SetStructuredMut(gObj.Data())

	exec, err := bloblang.Parse(`root.foos = this.foos`)
	require.NoError(t, err)

	proc := newMutation(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, nil, message.Batch{part})
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	resPartBytes := outBatches[0][0].AsBytes()
	assert.Equal(t, `{"foos":[{"foo":"FROM NEW OBJECT"},5,null]}`, string(resPartBytes))
}

func TestMutationCreateFiltering(t *testing.T) {
	tCtx := context.Background()

	inBatch := message.Batch{
		message.NewPart([]byte(`{"foo":{"delete":true}}`)),
		message.NewPart([]byte(`{"foo":{"dont":"delete me"}}`)),
		message.NewPart([]byte(`{"bar":{"delete":true}}`)),
		message.NewPart([]byte(`{"bar":{"dont":"delete me"}}`)),
	}

	exec, err := bloblang.Parse(`
root = match {
  (foo | bar).delete.or(false) => deleted(),
}
`)
	require.NoError(t, err)

	proc := newMutation(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, nil, inBatch)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 2)

	assert.NoError(t, outBatches[0][0].ErrorGet())
	assert.NoError(t, outBatches[0][1].ErrorGet())

	msgBytes := outBatches[0][0].AsBytes()
	assert.Equal(t, `{"foo":{"dont":"delete me"}}`, string(msgBytes))

	msgBytes = outBatches[0][1].AsBytes()
	assert.Equal(t, `{"bar":{"dont":"delete me"}}`, string(msgBytes))
}

func TestMutationCreateFilterAll(t *testing.T) {
	tCtx := context.Background()

	inBatch := message.Batch{
		message.NewPart([]byte(`{"foo":{"delete":true}}`)),
		message.NewPart([]byte(`{"foo":{"dont":"delete me"}}`)),
		message.NewPart([]byte(`{"bar":{"delete":true}}`)),
		message.NewPart([]byte(`{"bar":{"dont":"delete me"}}`)),
	}

	exec, err := bloblang.Parse(`root = deleted()`)
	require.NoError(t, err)

	proc := newMutation(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, nil, inBatch)
	assert.NoError(t, err)
	assert.Empty(t, outBatches)
}

func TestMutationCreateJSONError(t *testing.T) {
	tCtx := context.Background()

	msg := message.Batch{
		message.NewPart([]byte(`this is not valid json`)),
	}

	exec, err := bloblang.Parse(`foo = json().bar`)
	require.NoError(t, err)

	proc := newMutation(exec, nil)

	outBatches, err := proc.ProcessBatch(tCtx, nil, msg)
	require.NoError(t, err)
	require.Len(t, outBatches, 1)
	require.Len(t, outBatches[0], 1)

	msgBytes := outBatches[0][0].AsBytes()
	assert.Equal(t, `this is not valid json`, string(msgBytes))

	err = outBatches[0][0].ErrorGet()
	require.Error(t, err)
	assert.Equal(t, `failed assignment (line 1): invalid character 'h' in literal true (expecting 'r')`, err.Error())
}
