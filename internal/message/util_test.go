package message

import (
	"encoding/json"
	"reflect"
	"testing"
)

//------------------------------------------------------------------------------

func TestGetAllBytes(t *testing.T) {
	rawBytes := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	}
	m := QuickBatch(rawBytes)
	if exp, act := rawBytes, GetAllBytes(m); !reflect.DeepEqual(exp, act) {
		t.Errorf("Wrong result: %s != %s", act, exp)
	}
}

func TestCloneGeneric(t *testing.T) {
	var original any
	var cloned any

	err := json.Unmarshal([]byte(`{
		"root":{
			"first":{
				"value1": 1,
				"value2": 1.2,
				"value3": false,
				"value4": "hello world"
			},
			"second": [
				1,
				1.2,
				false,
				"hello world"
			]
		}
	}`), &original)
	if err != nil {
		t.Fatal(err)
	}

	cloned = cloneGeneric(original)
	if exp, act := original, cloned; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong cloned contents: %v != %v", act, exp)
	}

	target := cloned.(map[string]any)
	target = target["root"].(map[string]any)
	target = target["first"].(map[string]any)
	target["value1"] = 2

	target = original.(map[string]any)
	target = target["root"].(map[string]any)
	target = target["first"].(map[string]any)
	if exp, act := float64(1), target["value1"].(float64); exp != act {
		t.Errorf("Original value was mutated: %v != %v", act, exp)
	}
}

func TestCloneGenericYAML(t *testing.T) {
	var original any = map[any]any{
		"root": map[any]any{
			"first": map[any]any{
				"value1": 1,
				"value2": 1.2,
				"value3": false,
				"value4": "hello world",
			},
			"second": []any{
				1, 1.2, false, "hello world",
			},
		},
	}

	cloned := cloneGeneric(original)
	if exp, act := original, cloned; !reflect.DeepEqual(exp, act) {
		t.Fatalf("Wrong cloned contents: %v != %v", act, exp)
	}

	target := cloned.(map[any]any)
	target = target["root"].(map[any]any)
	target = target["first"].(map[any]any)
	target["value1"] = 2

	target = original.(map[any]any)
	target = target["root"].(map[any]any)
	target = target["first"].(map[any]any)
	if exp, act := 1, target["value1"].(int); exp != act {
		t.Errorf("Original value was mutated: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------

var benchResult float64

func BenchmarkCloneGeneric(b *testing.B) {
	var generic, cloned any
	err := json.Unmarshal([]byte(`{
		"root":{
			"first":{
				"value1": 1,
				"value2": 1.2,
				"value3": false,
				"value4": "hello world"
			},
			"second": [
				1,
				1.2,
				false,
				"hello world"
			]
		}
	}`), &generic)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cloned = cloneGeneric(generic)
	}
	b.StopTimer()

	target := cloned.(map[string]any)
	target = target["root"].(map[string]any)
	target = target["first"].(map[string]any)
	benchResult = target["value1"].(float64)
	if exp, act := float64(1), benchResult; exp != act {
		b.Errorf("Wrong result: %v != %v", act, exp)
	}
}

func BenchmarkCloneJSON(b *testing.B) {
	var generic, cloned any
	err := json.Unmarshal([]byte(`{
		"root":{
			"first":{
				"value1": 1,
				"value2": 1.2,
				"value3": false,
				"value4": "hello world"
			},
			"second": [
				1,
				1.2,
				false,
				"hello world"
			]
		}
	}`), &generic)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var interBytes []byte
		if interBytes, err = json.Marshal(generic); err != nil {
			b.Fatal(err)
		}
		if err = json.Unmarshal(interBytes, &cloned); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	target := cloned.(map[string]any)
	target = target["root"].(map[string]any)
	target = target["first"].(map[string]any)
	benchResult = target["value1"].(float64)
	if exp, act := float64(1), benchResult; exp != act {
		b.Errorf("Wrong result: %v != %v", act, exp)
	}
}

//------------------------------------------------------------------------------
