package template

import (
	"bytes"
	"text/template"

	"github.com/benthosdev/benthos/v4/internal/docs"

	_ "embed"
)

//go:embed docs.md
var testDocs string

type testContext struct {
	Fields []docs.FieldSpecCtx
}

// DocsMarkdown returns a markdown document for the templates documentation.
func DocsMarkdown() ([]byte, error) {
	testDocsTemplate := docs.FieldsTemplate(false) + testDocs

	var buf bytes.Buffer
	err := template.Must(template.New("tests").Parse(testDocsTemplate)).Execute(&buf, testContext{
		Fields: docs.FieldObject("", "").WithChildren(ConfigSpec()).FlattenChildrenForDocs(),
	})

	return buf.Bytes(), err
}

// ConfigSpec returns a configuration spec for a template.
func ConfigSpec() docs.FieldSpec {
	return docs.FieldObject("tests", "A list of one or more unit tests to execute.").Array().WithChildren(
		docs.FieldString("name", "The name of the test, this should be unique and give a rough indication of what behaviour is being tested."),
		docs.FieldString(
			"environment", "An optional map of environment variables to set for the duration of the test.",
		).Map().Optional(),
		docs.FieldString(
			"target_processors",
			`
A [JSON Pointer][json-pointer] that identifies the specific processors which should be executed by the test. The target can either be a single processor or an array of processors. Alternatively a resource label can be used to identify a processor.

It is also possible to target processors in a separate file by prefixing the target with a path relative to the test file followed by a # symbol.
`,
			"foo_processor",
			"/pipeline/processors/0",
			"target.yaml#/pipeline/processors",
			"target.yaml#/pipeline/processors",
		).HasDefault("/pipeline/processors"),
		docs.FieldString(
			"target_mapping",
			"A file path relative to the test definition path of a Bloblang file to execute as an alternative to testing processors with the `target_processors` field. This allows you to define unit tests for Bloblang mappings directly.",
		).HasDefault(""),
		docs.FieldAnything(
			"mocks",
			"An optional map of processors to mock. Keys should contain either a label or a JSON pointer of a processor that should be mocked. Values should contain a processor definition, which will replace the mocked processor. Most of the time you'll want to use a [`mapping` processor][processors.mapping] here, and use it to create a result that emulates the target processor.",
			map[string]any{
				"get_foobar_api": map[string]any{
					"mapping": "root = content().string() + \" this is some mock content\"",
				},
			},
			map[string]any{
				"/pipeline/processors/1": map[string]any{
					"mapping": "root = content().string() + \" this is some mock content\"",
				},
			},
		).Map().Optional(),
		docs.FieldObject(
			"input_batch", "Define a batch of messages to feed into your test, specify either an `input_batch` or a series of `input_batches`.",
		).Array().Optional().WithChildren(
			docs.FieldString("content", "The raw content of the input message.").HasDefault(""),
			docs.FieldAnything(`json_content`, "Sets the raw content of the message to a JSON document matching the structure of the value.", map[string]any{
				"foo": "foo value",
				"bar": []any{"element1", 10},
			},
			).Optional(),
			docs.FieldString(
				`file_content`,
				"Sets the raw content of the message by reading a file. The path of the file should be relative to the path of the test file.",
				"./foo/bar.txt",
			).Optional(),
			docs.FieldString("metadata", "A map of metadata key/values to add to the input message.").Map().Optional(),
		),
		docs.FieldObject(
			"input_batches", "Define a series of batches of messages to feed into your test, specify either an `input_batch` or a series of `input_batches`.",
		).ArrayOfArrays().Optional().WithChildren(
			docs.FieldString("content", "The raw content of the input message.").HasDefault(""),
			docs.FieldAnything(`json_content`, "Sets the raw content of the message to a JSON document matching the structure of the value.", map[string]any{
				"foo": "foo value",
				"bar": []any{"element1", 10},
			},
			).Optional(),
			docs.FieldString(
				`file_content`,
				"Sets the raw content of the message by reading a file. The path of the file should be relative to the path of the test file.",
				"./foo/bar.txt",
			).Optional(),
			docs.FieldString("metadata", "A map of metadata key/values to add to the input message.").Map().Optional(),
		),
		docs.FieldObject(
			"output_batches", "List of output batches.",
		).ArrayOfArrays().Optional().WithChildren(
			docs.FieldString("content", "The raw content of the input message.").HasDefault(""),
			docs.FieldAnything("metadata", "A map of metadata key/values to add to the input message.").Map().Optional(),
			docs.FieldString(
				`bloblang`,
				"Executes a Bloblang mapping on the output message, if the result is anything other than a boolean equalling `true` the test fails.",
				"this.age > 10 && @foo.length() > 0",
			).Optional(),
			docs.FieldString(`content_equals`, "Checks the full raw contents of a message against a value.").Optional(),
			docs.FieldString(`content_matches`, "Checks whether the full raw contents of a message matches a regular expression (re2).", "^foo [a-z]+ bar$").Optional(),
			docs.FieldAnything(
				`metadata_equals`,
				"Checks a map of metadata keys to values against the metadata stored in the message. If there is a value mismatch between a key of the condition versus the message metadata this condition will fail.",
				map[string]any{
					"example_key": "example metadata value",
				},
			).Map().Optional(),
			docs.FieldString(
				`file_equals`,
				"Checks that the contents of a message matches the contents of a file. The path of the file should be relative to the path of the test file.",
				"./foo/bar.txt",
			).Optional(),
			docs.FieldString(
				`file_json_equals`,
				"Checks that both the message and the file contents are valid JSON documents, and that they are structurally equivalent. Will ignore formatting and ordering differences. The path of the file should be relative to the path of the test file.",
				"./foo/bar.json",
			).Optional(),
			docs.FieldAnything(
				`json_equals`,
				"Checks that both the message and the condition are valid JSON documents, and that they are structurally equivalent. Will ignore formatting and ordering differences.",
				map[string]any{"key": "value"},
			).Optional(),
			docs.FieldAnything(
				`json_contains`,
				"Checks that both the message and the condition are valid JSON documents, and that the message is a superset of the condition.",
				map[string]any{"key": "value"},
			).Optional(),
			docs.FieldString(
				`file_json_contains`,
				"Checks that both the message and the file contents are valid JSON documents, and that the message is a superset of the condition. Will ignore formatting and ordering differences. The path of the file should be relative to the path of the test file.",
				"./foo/bar.json",
			).Optional(),
		),
	)
}
