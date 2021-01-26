package condition

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message"
	"github.com/Jeffail/benthos/v3/lib/metrics"
)

func TestJSONSchemaExternalSchemaCheck(t *testing.T) {
	schema := `{
		"$id": "https://example.com/person.schema.json",
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Person",
		"type": "object",
		"properties": {
		  "firstName": {
			"type": "string",
			"description": "The person's first name."
		  },
		  "lastName": {
			"type": "string",
			"description": "The person's last name."
		  },
		  "age": {
			"description": "Age in years which must be equal to or greater than zero.",
			"type": "integer",
			"minimum": 0
		  }
		}
	}`

	tmpSchemaFile, err := ioutil.TempFile("", "benthos_jsonschema_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpSchemaFile.Name())

	// write schema definition to tmpfile
	if _, err := tmpSchemaFile.Write([]byte(schema)); err != nil {
		t.Fatal(err)
	}

	testLog := log.Noop()
	testMet := metrics.Noop()

	type fields struct {
		schemaPath string
		part       int
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "schema match",
			fields: fields{
				schemaPath: fmt.Sprintf("file://%s", tmpSchemaFile.Name()),
				part:       0,
			},
			arg: [][]byte{
				[]byte(`{"firstName":"John","lastName":"Doe","age":21}`),
			},
			want: true,
		},
		{
			name: "schema no match",
			fields: fields{
				schemaPath: fmt.Sprintf("file://%s", tmpSchemaFile.Name()),
				part:       0,
			},
			arg: [][]byte{
				[]byte(`{"firstName":"John","lastName":"Doe","age":-20}`),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "jsonschema"
			conf.JSONSchema.SchemaPath = tt.fields.schemaPath
			conf.JSONSchema.Part = tt.fields.part

			c, err := NewJSONSchema(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("JSONSchema.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONSchemaInlineSchemaCheck(t *testing.T) {
	schemaDef := `{
		"$id": "https://example.com/person.schema.json",
		"$schema": "http://json-schema.org/draft-07/schema#",
		"title": "Person",
		"type": "object",
		"properties": {
		  "firstName": {
			"type": "string",
			"description": "The person's first name."
		  },
		  "lastName": {
			"type": "string",
			"description": "The person's last name."
		  },
		  "age": {
			"description": "Age in years which must be equal to or greater than zero.",
			"type": "integer",
			"minimum": 0
		  }
		}
	}`

	testLog := log.Noop()
	testMet := metrics.Noop()

	type fields struct {
		schema string
		part   int
	}
	tests := []struct {
		name   string
		fields fields
		arg    [][]byte
		want   bool
	}{
		{
			name: "schema match",
			fields: fields{
				schema: schemaDef,
				part:   0,
			},
			arg: [][]byte{
				[]byte(`{"firstName":"John","lastName":"Doe","age":21}`),
			},
			want: true,
		},
		{
			name: "schema no match",
			fields: fields{
				schema: schemaDef,
				part:   0,
			},
			arg: [][]byte{
				[]byte(`{"firstName":"John","lastName":"Doe","age":-20}`),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := NewConfig()
			conf.Type = "jsonschema"
			conf.JSONSchema.Schema = tt.fields.schema
			conf.JSONSchema.Part = tt.fields.part

			c, err := NewJSONSchema(conf, nil, testLog, testMet)
			if err != nil {
				t.Error(err)
				return
			}
			if got := c.Check(message.New(tt.arg)); got != tt.want {
				t.Errorf("JSONSchema.Check() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONSchemaPathNotExist(t *testing.T) {
	testLog := log.Noop()
	testMet := metrics.Noop()

	conf := NewConfig()
	conf.Type = "jsonschema"
	conf.JSONSchema.SchemaPath = "file://path_does_not_exist"

	_, err := NewJSONSchema(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from loading non existant schema file")
	}
}

func TestJSONSchemaInvalidSchema(t *testing.T) {
	schema := `{
		"$schema": "http://json-schema.org/draft-07/schema#",
		"type": "any"
	}`

	tmpSchemaFile, err := ioutil.TempFile("", "benthos_jsonschema_invalid_schema_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpSchemaFile.Name())

	// write schema definition to tmpfile
	if _, err := tmpSchemaFile.Write([]byte(schema)); err != nil {
		t.Fatal(err)
	}

	testLog := log.Noop()
	testMet := metrics.Noop()

	conf := NewConfig()
	conf.Type = "jsonschema"
	conf.JSONSchema.SchemaPath = fmt.Sprintf("file://%s", tmpSchemaFile.Name())

	_, err = NewJSONSchema(conf, nil, testLog, testMet)
	if err == nil {
		t.Error("expected error from loading bad schema")
	}
}
