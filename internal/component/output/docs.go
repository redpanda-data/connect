package output

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
