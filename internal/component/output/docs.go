package output

import "github.com/benthosdev/benthos/v4/internal/docs"

var docsAsync = `
This output benefits from sending multiple messages in flight in parallel for
improved performance. You can tune the max number of in flight messages (or
message batches) with the field ` + "`max_in_flight`" + `.`

var docsBatches = `
This output benefits from sending messages as a batch for improved performance.
Batches can be formed at both the input and output level. You can find out more
[in this doc](/docs/configuration/batching).`

// Description appends standard feature descriptions to an output description
// based on various features of the output.
func Description(async, batches bool, content string) string {
	if !async && !batches {
		return content
	}
	content += "\n\n## Performance"
	if async {
		content += "\n" + docsAsync
	}
	if batches {
		content += "\n" + docsBatches
	}
	return content
}

// InjectTracingSpanMappingDocs returns a field spec describing an inject
// tracing span mapping.
var InjectTracingSpanMappingDocs = docs.FieldBloblang(
	"inject_tracing_map",
	"EXPERIMENTAL: A [Bloblang mapping](/docs/guides/bloblang/about) used to inject an object containing tracing propagation information into outbound messages. The specification of the injected fields will match the format used by the service wide tracer.",
	`meta = meta().merge(this)`,
	`root.meta.span = this`,
).AtVersion("3.45.0").Advanced()
