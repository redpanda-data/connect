package processor

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloblangCrossfire(t *testing.T) {
	msg := message.New(nil)

	part := message.NewPart([]byte(`{"foo":{"bar":{"baz":"original value","qux":"dont change"}}}`))
	part.Metadata().Set("foo", "orig1")
	part.Metadata().Set("bar", "orig2")
	msg.Append(part)

	msg.Append(message.NewPart([]byte(`{}`)))
	if err := msg.Iter(func(i int, p types.Part) error {
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
	proc, err := NewBloblang(conf, nil, log.Noop(), metrics.Noop())
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
	assert.Equal(t, "orig1", inputPartOne.Metadata().Get("foo"))
	assert.Equal(t, "orig2", inputPartOne.Metadata().Get("bar"))
	assert.Equal(t, "", inputPartOne.Metadata().Get("baz"))

	assert.Equal(t, `{}`, string(inputPartTwo.Get()))
	assert.Equal(t, "", inputPartTwo.Metadata().Get("foo"))
	assert.Equal(t, "", inputPartTwo.Metadata().Get("bar"))
	assert.Equal(t, "", inputPartTwo.Metadata().Get("baz"))

	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(resPartOne.Get()))
	assert.Equal(t, "orig1", resPartOne.Metadata().Get("foo"))
	assert.Equal(t, "orig2", resPartOne.Metadata().Get("bar"))
	assert.Equal(t, "new meta", resPartOne.Metadata().Get("baz"))

	assert.Equal(t, `{"foo":{"bar":{"baz":"and this changed","qux":"dont change"},"bar_new":"this is swapped now"}}`, string(resPartTwo.Get()))
	assert.Equal(t, "orig1", resPartTwo.Metadata().Get("foo"))
	assert.Equal(t, "orig2", resPartTwo.Metadata().Get("bar"))
	assert.Equal(t, "new meta", resPartTwo.Metadata().Get("baz"))
}

func TestBloblangFiltering(t *testing.T) {
	msg := message.New([][]byte{
		[]byte(`{"foo":{"delete":true}}`),
		[]byte(`{"foo":{"dont":"delete me"}}`),
		[]byte(`{"bar":{"delete":true}}`),
		[]byte(`{"bar":{"dont":"delete me"}}`),
	})

	conf := NewConfig()
	conf.Bloblang = `
	root = match {
		(foo | bar).delete == true => deleted(),
	}
	`
	proc, err := NewBloblang(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)
	require.Equal(t, 2, outMsgs[0].Len())
	assert.Equal(t, ``, outMsgs[0].Get(0).Metadata().Get(FailFlagKey))
	assert.Equal(t, ``, outMsgs[0].Get(1).Metadata().Get(FailFlagKey))
	assert.Equal(t, `{"foo":{"dont":"delete me"}}`, string(outMsgs[0].Get(0).Get()))
	assert.Equal(t, `{"bar":{"dont":"delete me"}}`, string(outMsgs[0].Get(1).Get()))
}

func TestBloblangJSONError(t *testing.T) {
	msg := message.New([][]byte{
		[]byte(`this is not valid json`),
	})

	conf := NewConfig()
	conf.Bloblang = `
	foo = json().bar
`
	proc, err := NewBloblang(conf, nil, log.Noop(), metrics.Noop())
	if err != nil {
		t.Fatal(err)
	}

	outMsgs, res := proc.ProcessMessage(msg)
	require.Nil(t, res)
	require.Len(t, outMsgs, 1)

	resPart := outMsgs[0].Get(0)

	assert.Equal(t, `this is not valid json`, string(resPart.Get()))
	assert.Equal(t, `failed to execute mapping assignment at line 1: invalid character 'h' in literal true (expecting 'r')`, resPart.Metadata().Get(types.FailFlagKey))
}
