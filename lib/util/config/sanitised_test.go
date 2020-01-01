package config

import (
	"encoding/json"
	"testing"

	yaml "gopkg.in/yaml.v3"
)

func TestSanitisedJSON(t *testing.T) {
	exp := `{"type":"foo","a":"a","z":"z"}`
	obj := Sanitised{
		"a":    "a",
		"type": "foo",
		"z":    "z",
	}
	actBytes, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}

	act := string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	exp = `{"type":5,"a":"hello","b":2.3}`
	obj = Sanitised{
		"a":    "hello",
		"b":    2.3,
		"type": 5,
	}
	if actBytes, err = json.Marshal(obj); err != nil {
		t.Fatal(err)
	}

	act = string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}

	exp = `{"type":"foo"}`
	obj = Sanitised{
		"type": "foo",
	}
	if actBytes, err = json.Marshal(obj); err != nil {
		t.Fatal(err)
	}

	act = string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}

func TestSanitisedYAML(t *testing.T) {
	exp := `type: foo
a: a
z: z
`

	obj := Sanitised{
		"a":    "a",
		"type": "foo",
		"z":    "z",
	}
	actBytes, err := yaml.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}

	act := string(actBytes)
	if act != exp {
		t.Errorf("Unexpected result: %v != %v", act, exp)
	}
}
