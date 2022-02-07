package message

// FailFlagKey is a metadata key used for flagging processor errors in Benthos.
// If a message part has any non-empty value for this metadata key then it will
// be interpretted as having failed a processor step somewhere in the pipeline.
//
// TODO: V4 stop hiding this as a metadata field
var FailFlagKey = "benthos_processing_failed"
