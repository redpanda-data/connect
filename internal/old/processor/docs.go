package processor

// DocsUsesBatches returns a documentation paragraph regarding processors that
// benefit from input level batching.
var DocsUsesBatches = `
The functionality of this processor depends on being applied across messages
that are batched. You can find out more about batching [in this doc](/docs/configuration/batching).`
