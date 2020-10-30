package output

import (
	"github.com/Jeffail/benthos/v3/internal/docs"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/output/writer"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

func init() {
	Constructors[TypeGRPCClient] = TypeSpec{
		constructor: NewGRPCClient,
		Summary: `
Sends a Unary message to a GRPC Server.`,
		Description: `
	Address is where the GRPC Client should dial to. If HTTPS is not set in the scheme,
	then Insecure dial is used.

	Method is the name of the gRPC Method that will be called, in the format package.Service/Method.

	FileDescriptorPath is the file descriptor path in the file system. File descriptors can be
	generated from .proto files with the following command:
    // protoc \
    // --descriptor_set_out=file.pb \
	// file.proto`,
		Async: true,
		FieldSpecs: docs.FieldSpecs{
			docs.FieldCommon("address", "The address of the server"),
			docs.FieldCommon("method", "The method name that will be called").SupportsInterpolation(false),
			docs.FieldAdvanced("descriptor_file_path", "Protobuf descriptor file path.").SupportsInterpolation(false),
		},
		Categories: []Category{
			CategoryNetwork,
		},
	}
}

//------------------------------------------------------------------------------

// NewGRPCClient creates a new GRPC Client output type.
func NewGRPCClient(conf Config, mgr types.Manager, log log.Modular, stats metrics.Type) (Type, error) {
	g, err := writer.NewGRPCClient(conf.GRPCClient, log, stats)
	if err != nil {
		return nil, err
	}
	return NewWriter(
		TypeGRPCClient, g, log, stats,
	)
}

//------------------------------------------------------------------------------
