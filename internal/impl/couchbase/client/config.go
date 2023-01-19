package client

// Transcoder represents the transcoder that will be used by Couchbase.
type Transcoder string

const (
	// TranscoderRaw raw operation.
	TranscoderRaw Transcoder = "raw"
	// TranscoderRawJSON rawjson transcoder.
	TranscoderRawJSON Transcoder = "rawjson"
	// TranscoderRawString rawstring transcoder.
	TranscoderRawString Transcoder = "rawstring"
	// TranscoderJSON JSON transcoder.
	TranscoderJSON Transcoder = "json"
	// TranscoderLegacy Legacy transcoder.
	TranscoderLegacy Transcoder = "legacy"
)

// Operation represents the operation that will be performed by Couchbase.
type Operation string

const (
	// OperationGet Get operation.
	OperationGet Operation = "get"
	// OperationInsert Insert operation.
	OperationInsert Operation = "insert"
	// OperationRemove Delete operation.
	OperationRemove Operation = "remove"
	// OperationReplace Replace operation.
	OperationReplace Operation = "replace"
	// OperationUpsert Upsert operation.
	OperationUpsert Operation = "upsert"
)
