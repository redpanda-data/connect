package main

import (
	"bytes"

	"github.com/redpanda-data/redpanda/src/transform-sdk/go/transform"
)

func main() {
	transform.OnRecordWritten(makeUppercase)
}

func makeUppercase(e transform.WriteEvent, w transform.RecordWriter) error {
	return w.Write(transform.Record{
		Key:   e.Record().Key,
		Value: bytes.ToUpper(e.Record().Value),
	})
}
