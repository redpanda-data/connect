package xml_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/benthosdev/benthos/v4/internal/component/processor"
	"github.com/benthosdev/benthos/v4/internal/manager/mock"
	"github.com/benthosdev/benthos/v4/internal/message"
)

func TestXMLCases(t *testing.T) {
	type testCase struct {
		name   string
		input  string
		output string
	}
	tests := []testCase{
		{
			name: "basic 1",
			input: `<root>
  <next>foo1</next>
</root>`,
			output: `{"root":{"next":"foo1"}}`,
		},
		{
			name: "contains escapes 1",
			input: `<root>
  <next>foo&amp;bar</next>
</root>`,
			output: `{"root":{"next":"foo&bar"}}`,
		},
		{
			name: "contains HTML escapes",
			input: `<root>
  <next>foo&lt;&ndash;&circ;&amp;bar</next>
</root>`,
			output: `{"root":{"next":"foo<&ndash;&circ;&bar"}}`,
		},
		{
			name: "basic 2",
			input: `<root>
  <next>foo1</next>
  <inner>
  	<thing>10</thing>
  </inner>
</root>`,
			output: `{"root":{"inner":{"thing":"10"},"next":"foo1"}}`,
		},
		{
			name: "with array 1",
			input: `<root>
  <next>foo1</next>
  <next>foo2</next>
  <next>foo3</next>
</root>`,
			output: `{"root":{"next":["foo1","foo2","foo3"]}}`,
		},
		{
			name: "with attributes 1",
			input: `<root isRooted="true">
  <next withinRoot="yes">foo1</next>
  <inner>
  	<thing someAttr="is boring" someAttr2="is also boring">10</thing>
  </inner>
</root>`,
			output: `{"root":{"-isRooted":"true","inner":{"thing":{"#text":"10","-someAttr":"is boring","-someAttr2":"is also boring"}},"next":{"#text":"foo1","-withinRoot":"yes"}}}`,
		},
		{
			name: "array with attributes 1",
			input: `<root>
  <title>This is a title</title>
  <description tone="boring">This is a description</description>
  <elements id="1">foo1</elements>
  <elements id="2">foo2</elements>
  <elements>foo3</elements>
</root>`,
			output: `{"root":{"description":{"#text":"This is a description","-tone":"boring"},"elements":[{"#text":"foo1","-id":"1"},{"#text":"foo2","-id":"2"},"foo3"],"title":"This is a title"}}`,
		},
		{
			name: "contains non utf-8 encoding",
			input: `<?xml version="1.0" encoding="ISO-8859-1"?>
<a><b>Hello world!</b></a>`,
			output: `{"a":{"b":"Hello world!"}}`,
		},
		{
			name:   "with numbers and bools without casting",
			input:  `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`,
			output: `{"root":{"bool":"True","number":{"#text":"123","-id":"99"},"title":"This is a title"}}`,
		},
	}

	conf := processor.NewConfig()
	conf.Type = "xml"
	conf.XML.Operator = "to_json"
	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		t.Run(test.name, func(tt *testing.T) {
			msgsOut, res := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte(test.input)}))
			if res != nil {
				tt.Fatal(res)
			}
			if len(msgsOut) != 1 {
				tt.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
			}
			if exp, act := test.output, string(msgsOut[0].Get(0).AsBytes()); exp != act {
				tt.Errorf("Wrong result: %v != %v", act, exp)
			}
			assert.NoError(t, msgsOut[0].Get(0).ErrorGet())
		})
	}
}

func TestXMLWithCast(t *testing.T) {
	conf := processor.NewConfig()
	conf.Type = "xml"
	conf.XML.Cast = true
	conf.XML.Operator = "to_json"

	testString := `<root><title>This is a title</title><number id="99">123</number><bool>True</bool></root>`

	proc, err := mock.NewManager().NewProcessor(conf)
	if err != nil {
		t.Fatal(err)
	}

	msgsOut, res := proc.ProcessBatch(context.Background(), message.QuickBatch([][]byte{[]byte(testString)}))
	if res != nil {
		t.Fatal(res.Error())
	}
	if len(msgsOut) != 1 {
		t.Fatalf("Wrong count of result messages: %v != 1", len(msgsOut))
	}
	if exp, act := `{"root":{"bool":true,"number":{"#text":123,"-id":99},"title":"This is a title"}}`, string(msgsOut[0].Get(0).AsBytes()); exp != act {
		t.Errorf("Wrong result: %v != %v", act, exp)
	}
	assert.NoError(t, msgsOut[0].Get(0).ErrorGet())
}
