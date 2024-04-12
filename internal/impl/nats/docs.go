package nats

import (
	ispan "github.com/benthosdev/benthos/v4/internal/component/input/span"
	ospan "github.com/benthosdev/benthos/v4/internal/component/output/span"
	"github.com/benthosdev/benthos/v4/public/service"
)

const (
	kvFieldBucket = "bucket"
)

const (
	tracingVersion = "4.23.0"
)

func connectionNameDescription() string {
	return `### Connection Name

When monitoring and managing a production NATS system, it is often useful to
know which connection a message was send/received from. This can be achieved by
setting the connection name option when creating a NATS connection.

Benthos will automatically set the connection name based off the label of the given
NATS component, so that monitoring tools between NATS and benthos can stay in sync.
`
}

func inputTracingDocs() *service.ConfigField {
	return ispan.ExtractTracingSpanMappingDocs().Version(tracingVersion)
}
func outputTracingDocs() *service.ConfigField {
	return ospan.InjectTracingSpanMappingDocs().Version(tracingVersion)
}
func kvDocs(extraFields ...*service.ConfigField) []*service.ConfigField {
	// TODO: Use `slices.Concat()` after switching to Go 1.22
	fields := append(
		connectionHeadFields(),
		[]*service.ConfigField{
			service.NewStringField(kvFieldBucket).
				Description("The name of the KV bucket.").Example("my_kv_bucket"),
		}...,
	)
	fields = append(fields, extraFields...)
	fields = append(fields, connectionTailFields()...)

	return fields
}
