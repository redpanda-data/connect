package config

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/input"
	"github.com/Jeffail/benthos/v3/lib/output"
	"github.com/Jeffail/benthos/v3/lib/processor"
	"github.com/Jeffail/gabs/v2"
	"github.com/stretchr/testify/assert"
	yaml "gopkg.in/yaml.v3"
)

func TestComponentExamples(t *testing.T) {
	for typeName, ctor := range input.Constructors {
		for _, example := range ctor.Examples {
			s := New()
			dec := yaml.NewDecoder(bytes.NewReader([]byte(example.Config)))
			dec.KnownFields(true)
			assert.NoError(t, dec.Decode(&s), "%v:%v:%v", "input", typeName, example.Title)

			type confAlias Type
			sAliased := confAlias(New())
			dec = yaml.NewDecoder(bytes.NewReader([]byte(example.Config)))
			dec.KnownFields(true)
			assert.NoError(t, dec.Decode(&sAliased), "%v:%v:%v", "input", typeName, example.Title)
		}
	}
	for typeName, ctor := range processor.Constructors {
		for _, example := range ctor.Examples {
			s := New()
			dec := yaml.NewDecoder(bytes.NewReader([]byte(example.Config)))
			dec.KnownFields(true)
			assert.NoError(t, dec.Decode(&s), "%v:%v:%v", "processor", typeName, example.Title)

			type confAlias Type
			sAliased := confAlias(New())
			dec = yaml.NewDecoder(bytes.NewReader([]byte(example.Config)))
			dec.KnownFields(true)
			assert.NoError(t, dec.Decode(&sAliased), "%v:%v:%v", "processor", typeName, example.Title)
		}
	}
	for typeName, ctor := range output.Constructors {
		for _, example := range ctor.Examples {
			s := New()
			dec := yaml.NewDecoder(bytes.NewReader([]byte(example.Config)))
			dec.KnownFields(true)
			assert.NoError(t, dec.Decode(&s), "%v:%v:%v", "output", typeName, example.Title)

			type confAlias Type
			sAliased := confAlias(New())
			dec = yaml.NewDecoder(bytes.NewReader([]byte(example.Config)))
			dec.KnownFields(true)
			assert.NoError(t, dec.Decode(&sAliased), "%v:%v:%v", "output", typeName, example.Title)
		}
	}
}

func CheckTagsOfType(v reflect.Type, checkedTypes map[string]struct{}, t *testing.T) {
	tPath := v.PkgPath() + "." + v.Name()
	if _, exists := checkedTypes[tPath]; len(v.PkgPath()) > 0 && exists {
		return
	}
	checkedTypes[tPath] = struct{}{}

	switch v.Kind() {
	case reflect.Slice:
		CheckTagsOfType(v.Elem(), checkedTypes, t)
	case reflect.Map:
		CheckTagsOfType(v.Elem(), checkedTypes, t)
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			field := v.Field(i)
			jTag := field.Tag.Get("json")
			yTag := field.Tag.Get("yaml")

			if len(yTag) == 0 {
				t.Errorf("Empty field tag in type %v", tPath)
			}

			if strings.ToLower(yTag) != yTag {
				t.Errorf("Non-lower case field tag in type %v: %v", tPath, yTag)
			}

			if jTag != yTag {
				t.Errorf("Mismatched config tags in type %v: json(%v) != yaml(%v)", tPath, jTag, yTag)
			}

			CheckTagsOfType(field.Type, checkedTypes, t)
		}
	}
}

func TestConfigTags(t *testing.T) {
	v := reflect.TypeOf(New())

	checkedTypes := map[string]struct{}{}
	CheckTagsOfType(v, checkedTypes, t)
}

func TestExampleGen(t *testing.T) {
	conf := New()
	AddExamples(&conf, "files", "memory", "jmespath", "file")

	jBytes, err := json.Marshal(conf)
	if err != nil {
		t.Fatal(err)
	}

	gObj, err := gabs.ParseJSON(jBytes)
	if err != nil {
		t.Fatal(err)
	}

	if exp, act := `"files"`, gObj.Path("input.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `"memory"`, gObj.Path("buffer.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `["jmespath","filter_parts"]`, gObj.Path("pipeline.processors.*.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `["text","jmespath"]`, gObj.Path("pipeline.processors.*.filter_parts.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}

	if exp, act := `"file"`, gObj.Path("output.type").String(); exp != act {
		t.Errorf("Unexpected conf value: %v != %v", act, exp)
	}
}
