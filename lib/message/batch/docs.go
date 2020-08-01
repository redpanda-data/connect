package batch

import "github.com/Jeffail/benthos/v3/internal/docs"

// FieldSpec returns a spec for a common batching field.
func FieldSpec() docs.FieldSpec {
	return docs.FieldSpec{
		Name: "batching",
		Description: `
Allows you to configure a [batching policy](/docs/configuration/batching).`,
		Examples: []interface{}{
			map[string]interface{}{
				"byte_size": 5000,
				"period":    "1s",
			},
			map[string]interface{}{
				"count":  10,
				"period": "1s",
			},
			map[string]interface{}{
				"period": "1m",
				"condition": map[string]interface{}{
					"text": map[string]interface{}{
						"operator": "contains",
						"arg":      "END BATCH",
					},
				},
			},
		},
		Children: docs.FieldSpecs{
			docs.FieldCommon("count", "A number of messages at which the batch should be flushed. If `0` disables count based batching."),
			docs.FieldCommon("byte_size", "An amount of bytes at which the batch should be flushed. If `0` disables size based batching."),
			docs.FieldCommon("period", "A period in which an incomplete batch should be flushed regardless of its size.", "1s", "1m", "500ms"),
			docs.FieldAdvanced("condition", "A [condition](/docs/components/conditions/about) to test against each message entering the batch, if this condition resolves to `true` then the batch is flushed."),
			docs.FieldAdvanced(
				"processors", "A list of [processors](/docs/components/processors/about) to apply to a batch as it is flushed. This allows you to aggregate and archive the batch however you see fit. Please note that all resulting messages are flushed as a single batch, therefore splitting the batch into smaller batches using these processors is a no-op.",
				[]map[string]interface{}{
					{
						"archive": map[string]interface{}{
							"format": "lines",
						},
					},
				},
				[]map[string]interface{}{
					{
						"archive": map[string]interface{}{
							"format": "json_array",
						},
					},
				},
				[]map[string]interface{}{
					{
						"merge_json": struct{}{},
					},
				},
			),
		},
	}
}
