package message

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageSerialization(t *testing.T) {
	input := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("12345"),
	}

	m2, err := FromBytes(SerializeBytes(input))
	if err != nil {
		t.Error(err)
		return
	}

	if !reflect.DeepEqual(input, GetAllBytes(m2)) {
		t.Errorf("Messages not equal: %v != %v", input, m2)
	}
}

func TestNew(t *testing.T) {
	m := QuickBatch(nil)
	if act := len(m); act > 0 {
		t.Errorf("New returned more than zero message parts: %v", act)
	}
}

func TestIter(t *testing.T) {
	parts := [][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
		[]byte(`baz`),
	}
	m := QuickBatch(parts)
	iters := 0
	_ = m.Iter(func(index int, b *Part) error {
		if exp, act := string(parts[index]), string(b.AsBytes()); exp != act {
			t.Errorf("Unexpected part: %v != %v", act, exp)
		}
		iters++
		return nil
	})
	if exp, act := 3, iters; exp != act {
		t.Errorf("Wrong count of iterations: %v != %v", act, exp)
	}
}

func TestMessageInvalidBytesFormat(t *testing.T) {
	cases := [][]byte{
		[]byte(``),
		[]byte(`this is invalid`),
		{0x00, 0x00},
		{0x00, 0x00, 0x00, 0x05},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00},
	}

	for _, c := range cases {
		if _, err := FromBytes(c); err == nil {
			t.Errorf("Received nil error from invalid byte sequence: %s", c)
		}
	}
}

func TestMessageIncompleteJSON(t *testing.T) {
	tests := []struct {
		message string
		err     string
	}{
		{message: "{}"},
		{
			message: "{} not foo",
			err:     "invalid character 'o' in literal null (expecting 'u')",
		},
		{
			message: "{} {}",
			err:     "message contains multiple valid documents",
		},
		{message: `["foo"]  `},
		{message: `   ["foo"]  `},
		{message: `   ["foo"]
		
		`},
		{
			message: `   ["foo"] 
		
		
		
		{}`,
			err: "message contains multiple valid documents",
		},
	}

	for _, test := range tests {
		msg := QuickBatch([][]byte{[]byte(test.message)})
		_, err := msg.Get(0).AsStructuredMut()
		if test.err == "" {
			assert.NoError(t, err)
		} else {
			assert.EqualError(t, err, test.err)
		}
	}
}

func TestMessageJSONGet(t *testing.T) {
	msg := QuickBatch(
		[][]byte{[]byte(`{"foo":{"bar":"baz"}}`)},
	)

	_, err := msg.Get(1).AsStructuredMut()
	require.Error(t, err)

	jObj, err := msg.Get(0).AsStructuredMut()
	require.NoError(t, err)

	exp := map[string]any{
		"foo": map[string]any{
			"bar": "baz",
		},
	}
	assert.Equal(t, exp, jObj)

	msg.Get(0).SetBytes([]byte(`{"foo":{"bar":"baz2"}}`))

	jObj, err = msg.Get(0).AsStructuredMut()
	require.NoError(t, err)

	exp = map[string]any{
		"foo": map[string]any{
			"bar": "baz2",
		},
	}
	assert.Equal(t, exp, jObj)
}

func TestMessageJSONSet(t *testing.T) {
	msg := QuickBatch([][]byte{[]byte(`hello world`)})

	msg.Get(1).SetStructured(nil)

	p1Obj := map[string]any{
		"foo": map[string]any{
			"bar": "baz",
		},
	}
	p1Str := `{"foo":{"bar":"baz"}}`

	p2Obj := map[string]any{
		"baz": map[string]any{
			"bar": "foo",
		},
	}
	p2Str := `{"baz":{"bar":"foo"}}`

	msg.Get(0).SetStructured(p1Obj)
	if exp, act := p1Str, string(msg.Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}

	msg.Get(0).SetStructured(p2Obj)
	if exp, act := p2Str, string(msg.Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}

	msg.Get(0).SetStructured(p1Obj)
	if exp, act := p1Str, string(msg.Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
}

func TestMessageMetadata(t *testing.T) {
	m := QuickBatch([][]byte{
		[]byte("foo"),
		[]byte("bar"),
	})

	m.Get(0).MetaSetMut("foo", "bar")
	if exp, act := "bar", m.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Get(0).MetaSetMut("foo", "bar2")
	if exp, act := "bar2", m.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m.Get(0).MetaSetMut("bar", "baz")
	m.Get(0).MetaSetMut("baz", "qux")

	exp := map[string]string{
		"foo": "bar2",
		"bar": "baz",
		"baz": "qux",
	}
	act := map[string]string{}
	require.NoError(t, m.Get(0).MetaIterStr(func(k, v string) error {
		act[k] = v
		return nil
	}))
	if !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestMessageCopy(t *testing.T) {
	m := QuickBatch([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
	})
	m.Get(0).MetaSetMut("foo", "bar")

	m2 := m.ShallowCopy()
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m2.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m2.Get(0).MetaSetMut("foo", "bar2")
	m2.Get(0).SetBytes([]byte(`baz`))
	if exp, act := [][]byte{[]byte(`baz`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar2", m2.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestMessageErrors(t *testing.T) {
	p1 := NewPart([]byte("foo"))
	assert.NoError(t, p1.ErrorGet())

	p2 := p1.WithContext(context.Background())
	assert.NoError(t, p2.ErrorGet())

	p3 := p2.ShallowCopy()
	assert.NoError(t, p3.ErrorGet())

	p1.ErrorSet(errors.New("err1"))
	assert.EqualError(t, p1.ErrorGet(), "err1")
	assert.EqualError(t, p2.ErrorGet(), "err1")
	assert.NoError(t, p3.ErrorGet())

	p2.ErrorSet(errors.New("err2"))
	assert.EqualError(t, p1.ErrorGet(), "err2")
	assert.EqualError(t, p2.ErrorGet(), "err2")
	assert.NoError(t, p3.ErrorGet())

	p3.ErrorSet(errors.New("err3"))
	assert.EqualError(t, p1.ErrorGet(), "err2")
	assert.EqualError(t, p2.ErrorGet(), "err2")
	assert.EqualError(t, p3.ErrorGet(), "err3")
}

func TestMessageDeepCopy(t *testing.T) {
	m := QuickBatch([][]byte{
		[]byte(`foo`),
		[]byte(`bar`),
	})
	m.Get(0).MetaSetMut("foo", "bar")

	m2 := m.DeepCopy()
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m2.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}

	m2.Get(0).MetaSetMut("foo", "bar2")
	m2.Get(0).SetBytes([]byte(`baz`))
	if exp, act := [][]byte{[]byte(`baz`), []byte(`bar`)}, GetAllBytes(m2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar2", m2.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	if exp, act := [][]byte{[]byte(`foo`), []byte(`bar`)}, GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
	if exp, act := "bar", m.Get(0).MetaGetStr("foo"); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func TestMessageJSONSetGet(t *testing.T) {
	msg := QuickBatch([][]byte{[]byte(`hello world`)})

	p1Obj := map[string]any{
		"foo": map[string]any{
			"bar": "baz",
		},
	}
	p1Str := `{"foo":{"bar":"baz"}}`

	p2Obj := map[string]any{
		"baz": map[string]any{
			"bar": "foo",
		},
	}
	p2Str := `{"baz":{"bar":"foo"}}`

	var err error
	var jObj any

	msg.Get(0).SetStructured(p1Obj)
	if exp, act := p1Str, string(msg.Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.Get(0).AsStructuredMut(); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}

	msg.Get(0).SetStructured(p2Obj)
	if exp, act := p2Str, string(msg.Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.Get(0).AsStructuredMut(); err != nil {
		t.Fatal(err)
	}
	if exp, act := p2Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}

	msg.Get(0).SetStructured(p1Obj)
	if exp, act := p1Str, string(msg.Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong json blob: %v != %v", act, exp)
	}
	if jObj, err = msg.Get(0).AsStructuredMut(); err != nil {
		t.Fatal(err)
	}
	if exp, act := p1Obj, jObj; !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong json obj: %v != %v", act, exp)
	}
}

func TestMessageSplitJSON(t *testing.T) {
	msg1 := QuickBatch([][]byte{
		[]byte("Foo plain text"),
		[]byte(`nothing here`),
	})

	msg1.Get(1).SetStructured(map[string]any{"foo": "bar"})
	msg2 := msg1.ShallowCopy()

	if exp, act := GetAllBytes(msg1), GetAllBytes(msg2); !reflect.DeepEqual(exp, act) {
		t.Errorf("Parts unmatched from shallow copy: %s != %s", act, exp)
	}

	msg2.Get(0).SetBytes([]byte("Bar different text"))

	if exp, act := "Foo plain text", string(msg1.Get(0).AsBytes()); exp != act {
		t.Errorf("Original content was changed from shallow copy: %v != %v", act, exp)
	}

	msg1.Get(1).SetStructured(map[string]any{"foo": "baz"})
	jCont, err := msg1.Get(1).AsStructuredMut()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"foo": "baz"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.Get(1).AsBytes()); exp != act {
		t.Errorf("Unexpected original content: %v != %v", act, exp)
	}

	jCont, err = msg2.Get(1).AsStructuredMut()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"foo": "bar"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"bar"}`, string(msg2.Get(1).AsBytes()); exp != act {
		t.Errorf("Unexpected shallow content: %v != %v", act, exp)
	}

	msg2.Get(1).SetStructured(map[string]any{"foo": "baz2"})
	jCont, err = msg2.Get(1).AsStructuredMut()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"foo": "baz2"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz2"}`, string(msg2.Get(1).AsBytes()); exp != act {
		t.Errorf("Unexpected shallow copy content: %v != %v", act, exp)
	}

	jCont, err = msg1.Get(1).AsStructuredMut()
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := map[string]any{"foo": "baz"}, jCont; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected original json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg1.Get(1).AsBytes()); exp != act {
		t.Errorf("Unexpected original content: %v != %v", act, exp)
	}
}

func TestMessageCrossContaminateJSON(t *testing.T) {
	msg1 := QuickBatch([][]byte{
		[]byte(`{"foo":"bar"}`),
	})

	jCont1, err := msg1.Get(0).AsStructuredMut()
	require.NoError(t, err)

	msg2 := msg1.ShallowCopy()
	jCont2, err := msg2.Get(0).AsStructuredMut()
	require.NoError(t, err)

	_, err = gabs.Wrap(jCont2).Set("baz", "foo")
	require.NoError(t, err)

	if exp, act := map[string]any{"foo": "bar"}, jCont1; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"bar"}`, string(msg1.Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected raw content: %v != %v", exp, act)
	}

	if exp, act := map[string]any{"foo": "baz"}, jCont2; !reflect.DeepEqual(exp, act) {
		t.Errorf("Unexpected json content: %v != %v", exp, act)
	}
	if exp, act := `{"foo":"baz"}`, string(msg2.Get(0).AsBytes()); exp != act {
		t.Errorf("Unexpected raw content: %v != %v", exp, act)
	}
}

func BenchmarkJSONGet(b *testing.B) {
	sample1 := []byte(`{
	"foo":{
		"bar":"baz",
		"this":{
			"will":{
				"be":{
					"very":{
						"nested":true
					}
				},
				"dont_forget":"me"
			},
			"dont_forget":"me"
		},
		"dont_forget":"me"
	},
	"numbers": [0,1,2,3,4,5,6,7]
}`)
	sample2 := []byte(`{
	"foo2":{
		"bar":"baz2",
		"this":{
			"will":{
				"be":{
					"very":{
						"nested":false
					}
				},
				"dont_forget":"me too"
			},
			"dont_forget":"me too"
		},
		"dont_forget":"me too"
	},
	"numbers": [0,1,2,3,4,5,6,7]
}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := QuickBatch([][]byte{sample1})

		jObj, err := msg.Get(0).AsStructuredMut()
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]any); !ok {
			b.Error("Couldn't cast to map")
		}

		jObj, err = msg.Get(0).AsStructuredMut()
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]any); !ok {
			b.Error("Couldn't cast to map")
		}

		msg.Get(0).SetBytes(sample2)

		jObj, err = msg.Get(0).AsStructuredMut()
		if err != nil {
			b.Error(err)
		}
		if _, ok := jObj.(map[string]any); !ok {
			b.Error("Couldn't cast to map")
		}
	}
}
