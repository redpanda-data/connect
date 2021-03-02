package mongodb

import "github.com/Jeffail/benthos/v3/internal/docs"

func writeConcernDocs() docs.FieldSpecs {
	return docs.FieldSpecs{
		docs.FieldCommon("w", "W requests acknowledgement that write operations propagate to the specified number of mongodb instances."),
		docs.FieldCommon("j", "J requests acknowledgement from MongoDB that write operations are written to the journal."),
		docs.FieldCommon("w_timeout", "The write concern timeout."),
	}
}

func mapExamples() []interface{} {
	examples := []interface{}{"root.a = this.foo\nroot.b = this.bar"}
	return examples
}
