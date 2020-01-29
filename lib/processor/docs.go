package processor

import "github.com/Jeffail/benthos/v3/lib/x/docs"

//------------------------------------------------------------------------------

// DocsUsesBatches returns a documentation paragraph regarding processors that
// benefit from input level batching.
var DocsUsesBatches = `
The functionality of this processor depends on being applied across messages
that are batched. You can find out more about batching [in this doc](/docs/configuration/batching).`

var partsFieldSpec = docs.FieldAdvanced(
	"parts",
	`An optional array of message indexes of a batch that the processor should apply to.
If left empty all messages are processed. This field is only applicable when
batching messages [at the input level](/docs/configuration/batching).

Indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1.`,
)

//------------------------------------------------------------------------------
