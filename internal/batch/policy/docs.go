package policy

import "github.com/benthosdev/benthos/v4/internal/docs"

// FieldSpec returns a spec for a common batching field.
func FieldSpec() docs.FieldSpec {
	return docs.FieldSpec{
		Name: "batching",
		Type: docs.FieldTypeObject,
		Description: `
Allows you to configure a [batching policy](/docs/configuration/batching).`,
		Examples: []any{
			map[string]any{
				"count":     0,
				"byte_size": 5000,
				"period":    "1s",
			},
			map[string]any{
				"count":  10,
				"period": "1s",
			},
			map[string]any{
				"count":  0,
				"period": "1m",
				"check":  `this.contains("END BATCH")`,
			},
		},
		Children: docs.FieldSpecs{
			docs.FieldInt(
				"count",
				"A number of messages at which the batch should be flushed. If `0` disables count based batching.",
			).HasDefault(0),
			docs.FieldInt(
				"byte_size",
				"An amount of bytes at which the batch should be flushed. If `0` disables size based batching.",
			).HasDefault(0),
			docs.FieldString(
				"period",
				"A period in which an incomplete batch should be flushed regardless of its size.",
				"1s", "1m", "500ms",
			).HasDefault(""),
			docs.FieldBloblang(
				"check",
				"A [Bloblang query](/docs/guides/bloblang/about/) that should return a boolean value indicating whether a message should end a batch.",
				`this.type == "end_of_transaction"`,
			).HasDefault(""),
			docs.FieldProcessor(
				"processors",
				"A list of [processors](/docs/components/processors/about) to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op.",
				[]map[string]any{
					{
						"archive": map[string]any{
							"format": "concatenate",
						},
					},
				},
				[]map[string]any{
					{
						"archive": map[string]any{
							"format": "lines",
						},
					},
				},
				[]map[string]any{
					{
						"archive": map[string]any{
							"format": "json_array",
						},
					},
				},
			).Array().Advanced().Optional(),
		},
	}
}
