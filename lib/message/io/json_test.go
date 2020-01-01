package io

import (
	"testing"

	"github.com/Jeffail/benthos/v3/lib/message"
)

//------------------------------------------------------------------------------

func TestJSONSerialisation(t *testing.T) {
	msg := message.New([][]byte{
		[]byte("foo"),
		[]byte(`{"bar":"baz"}`),
		[]byte("qux"),
	})

	msg.Get(1).Metadata().Set("meta1", "val1")
	msg.Get(1).Metadata().Set("meta2", "val2")
	msg.Get(2).Metadata().Set("meta3", "val3")

	bytes, err := MessageToJSON(msg)
	if err != nil {
		t.Fatal(err)
	}

	exp := `[{"metadata":{},"value":"foo"},{"metadata":{"meta1":"val1","meta2":"val2"},"value":"{\"bar\":\"baz\"}"},{"metadata":{"meta3":"val3"},"value":"qux"}]`

	if act := string(bytes); exp != act {
		t.Errorf("Wrong serialisation result: %v != %v", act, exp)
	}

	msg = message.New(nil)
	bytes, err = MessageToJSON(msg)
	if err != nil {
		t.Fatal(err)
	}
	exp = `[]`
	if act := string(bytes); exp != act {
		t.Errorf("Wrong serialisation result: %v != %v", act, exp)
	}

	msg.Append(message.NewPart(nil))
	bytes, err = MessageToJSON(msg)
	if err != nil {
		t.Fatal(err)
	}
	exp = `[{"metadata":{},"value":""}]`
	if act := string(bytes); exp != act {
		t.Errorf("Wrong serialisation result: %v != %v", act, exp)
	}
}

func TestJSONBadDeserialisation(t *testing.T) {
	input := `not #%@%$# valuid "" json`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `{"foo":"bar}`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `[[]]`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `[{"value":5}]`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}

	input = `[{"value":"5","metadata":[]}]`
	if _, err := MessageFromJSON([]byte(input)); err == nil {
		t.Error("Expected error")
	}
}

func TestJSONDeserialisation(t *testing.T) {
	input := `[]`
	msg, err := MessageFromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 0, msg.Len(); exp != act {
		t.Errorf("Wrong count of message parts: %v != %v", act, exp)
	}

	input = `[{"metadata":{},"value":""}]`
	msg, err = MessageFromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 1, msg.Len(); exp != act {
		t.Errorf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := 0, len(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong size of message part: %v != %v", act, exp)
	}

	input = `[{"metadata":{"meta1":"value1"},"value":"foo"},{"metadata":{"meta2":"value2","meta3":"value3"},"value":"bar"}]`
	msg, err = MessageFromJSON([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if exp, act := 2, msg.Len(); exp != act {
		t.Errorf("Wrong count of message parts: %v != %v", act, exp)
	}
	if exp, act := "foo", string(msg.Get(0).Get()); exp != act {
		t.Errorf("Wrong message part value: %v != %v", act, exp)
	}
	if exp, act := "bar", string(msg.Get(1).Get()); exp != act {
		t.Errorf("Wrong message part value: %v != %v", act, exp)
	}
	if exp, act := "value1", msg.Get(0).Metadata().Get("meta1"); exp != act {
		t.Errorf("Wrong message part metadata value: %v != %v", act, exp)
	}
	if exp, act := "value2", msg.Get(1).Metadata().Get("meta2"); exp != act {
		t.Errorf("Wrong message part metadata value: %v != %v", act, exp)
	}
	if exp, act := "value3", msg.Get(1).Metadata().Get("meta3"); exp != act {
		t.Errorf("Wrong message part metadata value: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
