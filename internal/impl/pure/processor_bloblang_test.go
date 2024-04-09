package pure_test

import (
	"context"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"

	_ "github.com/benthosdev/benthos/v4/internal/impl/pure"
)

func TestBloblangCrossfire(t *testing.T) {
	msg := message.QuickBatch(nil)

	part := message.NewPart([]byte(`{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`))
	part.MetaSetMut("foo", "orig1")
	part.MetaSetMut("bar", "orig2")
	msg = append(msg, part, message.NewPart([]byte(`{}`)))
	if err := msg.Iter(func(i int, p *message.Part) error {
		_, err := p.AsStructuredMut()
		return err
	}); err != nil {
		t.Fatal(err)
	}

	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `
	foo = json("foo").from(0)
	foo.bar_new = "this is swapped now"
	foo.bar.baz = "and this changed"
	meta foo = meta("foo").from(0)
	meta bar = meta("bar").from(0)
	meta baz = "new meta"
`
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessBatch(context.Background(), msg.ShallowCopy())
	require.NoError(t, res)
	require.Len(t, outMsgs, 1)

	inputPartOne := msg.Get(0)
	inputPartTwo := msg.Get(1)
	resPartOne := outMsgs[0].Get(0)
	resPartTwo := outMsgs[0].Get(1)

	assert.Equal(t, `{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`, string(inputPartOne.AsBytes()))
	assert.Equal(t, "orig1", inputPartOne.MetaGetStr("foo"))
	assert.Equal(t, "orig2", inputPartOne.MetaGetStr("bar"))
	assert.Equal(t, "", inputPartOne.MetaGetStr("baz"))

	assert.Equal(t, `{}`, string(inputPartTwo.AsBytes()))
	assert.Equal(t, "", inputPartTwo.MetaGetStr("foo"))
	assert.Equal(t, "", inputPartTwo.MetaGetStr("bar"))
	assert.Equal(t, "", inputPartTwo.MetaGetStr("baz"))

	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(resPartOne.AsBytes()))
	assert.Equal(t, "orig1", resPartOne.MetaGetStr("foo"))
	assert.Equal(t, "orig2", resPartOne.MetaGetStr("bar"))
	assert.Equal(t, "new meta", resPartOne.MetaGetStr("baz"))

	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(resPartTwo.AsBytes()))
	assert.Equal(t, "orig1", resPartTwo.MetaGetStr("foo"))
	assert.Equal(t, "orig2", resPartTwo.MetaGetStr("bar"))
	assert.Equal(t, "new meta", resPartTwo.MetaGetStr("baz"))
}

type testKeyType int

const testFooKey testKeyType = iota

func TestBloblangContext(t *testing.T) {
	msg := message.QuickBatch(nil)

	part := message.NewPart([]byte(`{"foo":{"bar":{"baz":"original value"}}}`))
	part.MetaSetMut("foo", "orig1")
	part.MetaSetMut("bar", "orig2")

	key, val := testFooKey, "bar"
	msg = append(msg, message.WithContext(context.WithValue(context.Background(), key, val), part))

	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `result = foo.bar.baz.uppercase()`
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `{"result":"ORIGINAL VALUE"}`, string(resPart.AsBytes()))
	assert.Equal(t, "orig1", resPart.MetaGetStr("foo"))
	assert.Equal(t, "orig2", resPart.MetaGetStr("bar"))
	assert.Equal(t, val, message.GetContext(resPart).Value(key))
}

func TestBloblangCustomObject(t *testing.T) {
	msg := message.QuickBatch(nil)

	part := message.NewPart(nil)

	gObj := gabs.New()
	_, _ = gObj.ArrayOfSize(3, "foos")

	gObjEle := gabs.New()
	_, _ = gObjEle.Set("FROM NEW OBJECT", "foo")

	_, _ = gObj.S("foos").SetIndex(gObjEle.Data(), 0)
	_, _ = gObj.S("foos").SetIndex(5, 1)

	part.SetStructured(gObj.Data())
	msg = append(msg, part)

	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `root.foos = this.foos`
	proc, err := mock.NewManager().NewProcessor(conf)
	require.NoError(t, err)

	outMsgs, res := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `{"foos":[{"foo":"FROM NEW OBJECT"},5,null]}`, string(resPart.AsBytes()))
}

func TestBloblangFiltering(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"delete":true}}`),
		[]byte(`{"foo":{"dont":"delete me"}}`),
		[]byte(`{"bar":{"delete":true}}`),
		[]byte(`{"bar":{"dont":"delete me"}}`),
	})

	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `
	root = match {
		(foo | bar).delete.or(false) => deleted(),
	}
	`
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, res)
	require.Len(t, outMsgs, 1)
	require.Equal(t, 2, outMsgs[0].Len())
	assert.NoError(t, outMsgs[0].Get(0).ErrorGet())
	assert.NoError(t, outMsgs[0].Get(1).ErrorGet())
	assert.Equal(t, `{"foo":{"dont":"delete me"}}`, string(outMsgs[0].Get(0).AsBytes()))
	assert.Equal(t, `{"bar":{"dont":"delete me"}}`, string(outMsgs[0].Get(1).AsBytes()))
}

func TestBloblangFilterAll(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"delete":true}}`),
		[]byte(`{"foo":{"dont":"delete me"}}`),
		[]byte(`{"bar":{"delete":true}}`),
		[]byte(`{"bar":{"dont":"delete me"}}`),
	})

	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `root = deleted()`
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessBatch(context.Background(), msg)
	assert.Empty(t, outMsgs)
	assert.NoError(t, res)
}

func TestBloblangJSONError(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte(`this is not valid json`),
	})

	conf := processor.NewConfig()
	conf.Type = "bloblang"
	conf.Plugin = `
	foo = json().bar
`
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessBatch(context.Background(), msg)
	require.NoError(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `this is not valid json`, string(resPart.AsBytes()))
	require.Error(t, resPart.ErrorGet())
	assert.Equal(t, `failed assignment (line 2): invalid character 'h' in literal true (expecting 'r')`, resPart.ErrorGet().Error())
}
