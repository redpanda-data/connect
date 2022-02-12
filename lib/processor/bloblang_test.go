package processor

import (
	"context"
	"testing"

	imessage "github.com/Jeffail/benthos/v3/internal/message"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/manager/mock"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloblangCrossfire(t *testing.T) {
	msg := message.QuickBatch(nil)

	part := message.NewPart([]byte(`{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`))
	part.MetaSet("foo", "orig1")
	part.MetaSet("bar", "orig2")
	msg.Append(part)

	msg.Append(message.NewPart([]byte(`{}`)))
	if err := msg.Iter(func(i int, p *message.Part) error {
		_, err := p.JSON()
		return err
	}); err != nil {
		t.Fatal(err)
	}

	conf := NewConfig()
	conf.Bloblang = `
	foo = json("foo").from(0)
	foo.bar_new = "this is swapped now"
	foo.bar.baz = "and this changed"
	meta foo = meta("foo").from(0)
	meta bar = meta("bar").from(0)
	meta baz = "new meta"
`
	proc, err := NewBloblang(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)

	inputPartOne := msg.Get(0)
	inputPartTwo := msg.Get(1)
	resPartOne := outMsgs[0].Get(0)
	resPartTwo := outMsgs[0].Get(1)

	assert.Equal(t, `{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`, string(inputPartOne.Get()))
	assert.Equal(t, "orig1", inputPartOne.MetaGet("foo"))
	assert.Equal(t, "orig2", inputPartOne.MetaGet("bar"))
	assert.Equal(t, "", inputPartOne.MetaGet("baz"))

	assert.Equal(t, `{}`, string(inputPartTwo.Get()))
	assert.Equal(t, "", inputPartTwo.MetaGet("foo"))
	assert.Equal(t, "", inputPartTwo.MetaGet("bar"))
	assert.Equal(t, "", inputPartTwo.MetaGet("baz"))

	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(resPartOne.Get()))
	assert.Equal(t, "orig1", resPartOne.MetaGet("foo"))
	assert.Equal(t, "orig2", resPartOne.MetaGet("bar"))
	assert.Equal(t, "new meta", resPartOne.MetaGet("baz"))

	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(resPartTwo.Get()))
	assert.Equal(t, "orig1", resPartTwo.MetaGet("foo"))
	assert.Equal(t, "orig2", resPartTwo.MetaGet("bar"))
	assert.Equal(t, "new meta", resPartTwo.MetaGet("baz"))
}

type testKeyType int

const testFooKey testKeyType = iota

func TestBloblangContext(t *testing.T) {
	msg := message.QuickBatch(nil)

	part := message.NewPart([]byte(`{"foo":{"bar":{"baz":"original value"}}}`))
	part.MetaSet("foo", "orig1")
	part.MetaSet("bar", "orig2")

	key, val := testFooKey, "bar"
	msg.Append(message.WithContext(context.WithValue(context.Background(), key, val), part))

	conf := NewConfig()
	conf.Bloblang = `result = foo.bar.baz.uppercase()`
	proc, err := NewBloblang(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `{"result":"ORIGINAL VALUE"}`, string(resPart.Get()))
	assert.Equal(t, "orig1", resPart.MetaGet("foo"))
	assert.Equal(t, "orig2", resPart.MetaGet("bar"))
	assert.Equal(t, val, message.GetContext(resPart).Value(key))
}

func TestBloblangCustomObject(t *testing.T) {
	msg := message.QuickBatch(nil)

	part := message.NewPart(nil)

	gObj := gabs.New()
	gObj.ArrayOfSize(3, "foos")

	gObjEle := gabs.New()
	gObjEle.Set("FROM NEW OBJECT", "foo")

	gObj.S("foos").SetIndex(gObjEle.Data(), 0)
	gObj.S("foos").SetIndex(5, 1)

	require.NoError(t, part.SetJSON(gObj.Data()))
	msg.Append(part)

	conf := NewConfig()
	conf.Bloblang = `root.foos = this.foos`
	proc, err := NewBloblang(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	require.NoError(t, err)

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `{"foos":[{"foo":"FROM NEW OBJECT"},5,null]}`, string(resPart.Get()))
}

func TestBloblangFiltering(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"delete":true}}`),
		[]byte(`{"foo":{"dont":"delete me"}}`),
		[]byte(`{"bar":{"delete":true}}`),
		[]byte(`{"bar":{"dont":"delete me"}}`),
	})

	conf := NewConfig()
	conf.Bloblang = `
	root = match {
		(foo | bar).delete.or(false) => deleted(),
	}
	`
	proc, err := NewBloblang(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)
	require.Equal(t, 2, outMsgs[0].Len())
	assert.Equal(t, ``, outMsgs[0].Get(0).MetaGet(FailFlagKey))
	assert.Equal(t, ``, outMsgs[0].Get(1).MetaGet(FailFlagKey))
	assert.Equal(t, `{"foo":{"dont":"delete me"}}`, string(outMsgs[0].Get(0).Get()))
	assert.Equal(t, `{"bar":{"dont":"delete me"}}`, string(outMsgs[0].Get(1).Get()))
}

func TestBloblangFilterAll(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte(`{"foo":{"delete":true}}`),
		[]byte(`{"foo":{"dont":"delete me"}}`),
		[]byte(`{"bar":{"delete":true}}`),
		[]byte(`{"bar":{"dont":"delete me"}}`),
	})

	conf := NewConfig()
	conf.Bloblang = `root = deleted()`
	proc, err := NewBloblang(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	assert.Empty(t, outMsgs)
	assert.Equal(t, nil, res)
}

func TestBloblangJSONError(t *testing.T) {
	msg := message.QuickBatch([][]byte{
		[]byte(`this is not valid json`),
	})

	conf := NewConfig()
	conf.Bloblang = `
	foo = json().bar
`
	proc, err := NewBloblang(conf, mock.NewManager(), log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `this is not valid json`, string(resPart.Get()))
	assert.Equal(t, `failed assignment (line 2): invalid character 'h' in literal true (expecting 'r')`, resPart.MetaGet(imessage.FailFlagKey))
}
