package writer

import (
	"context"
	"errors"
	"io/ioutil"
	"net/url"
	"strings"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

//------------------------------------------------------------------------------

// GRPCClientConfig contains configuration fields for the GRPC output type.
type GRPCClientConfig struct {
	Method             string `json:"method" yaml:"method"`
	Address            string `json:"address" yaml:"address"`
	DescriptorFilePath string `json:"descriptor_file_path" yaml:"descriptor_file_path"`
}

// NewGRPCClientConfig creates a new Config with default values.
func NewGRPCClientConfig() GRPCClientConfig {
	return GRPCClientConfig{}
}

//------------------------------------------------------------------------------

// GRPCClient is a benthos writer.Type implementation that writes messages to an
// GRPC Method.
type GRPCClient struct {
	conf GRPCClientConfig

	conn             *grpc.ClientConn
	inputDescriptor  protoreflect.MessageDescriptor
	outputDescriptor protoreflect.MessageDescriptor

	log   log.Modular
	stats metrics.Type
}

// NewGRPCClient creates a new GRPC Client writer.Type.
func NewGRPCClient(
	conf GRPCClientConfig,
	log log.Modular,
	stats metrics.Type,
) (*GRPCClient, error) {
	if conf.Address == "" {
		return nil, errors.New("address must not be empty")
	}
	// get MessageDescriptor from protoDescriptor fileset.

	b, err := ioutil.ReadFile(conf.DescriptorFilePath)
	if err != nil {
		return nil, err
	}

	fds := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(b, fds); err != nil {
		return nil, err
	}
	ff, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, err
	}
	descriptor, err := ff.FindDescriptorByName(protoreflect.FullName(strings.Replace(conf.Method, "/", ".", -1)))
	if err != nil {
		return nil, err
	}
	mdp := protodesc.ToMethodDescriptorProto(descriptor.(protoreflect.MethodDescriptor))
	inputType, outputType := mdp.GetInputType(), mdp.GetOutputType()
	// remove leading dot
	if len(inputType) >= 1 {
		inputType = inputType[1:]
	}
	if len(outputType) >= 1 {
		outputType = outputType[1:]
	}

	inputDescriptor, err := ff.FindDescriptorByName(protoreflect.FullName(inputType))
	if err != nil {
		return nil, err
	}

	outputDescriptor, err := ff.FindDescriptorByName(protoreflect.FullName(outputType))
	if err != nil {
		return nil, err
	}

	g := GRPCClient{
		conf:             conf,
		inputDescriptor:  inputDescriptor.(protoreflect.MessageDescriptor),
		outputDescriptor: outputDescriptor.(protoreflect.MessageDescriptor),
		log:              log,
		stats:            stats,
	}
	return &g, nil
}

//------------------------------------------------------------------------------

// Connect creates a new GRPCClient
func (a *GRPCClient) Connect() error {
	return a.ConnectWithContext(context.Background())
}

// ConnectWithContext creates a new GRPCClient client and ensures that the connection has started.
func (a *GRPCClient) ConnectWithContext(ctx context.Context) error {
	if a.conn != nil {
		return nil
	}

	u, err := url.Parse(a.conf.Address)
	if err != nil {
		return err
	}

	opts := []grpc.DialOption{}
	if u.Scheme == "http" {
		opts = append(opts, grpc.WithInsecure())
	}

	conn, err := grpc.DialContext(ctx, u.Host, opts...)
	if err != nil {
		return err
	}
	a.conn = conn

	a.log.Infof("Sending messages to GRPC Address: %s, Method: %s\n", a.conf.Address, a.conf.Method)
	return nil
}

// Write attempts to write message contents to a target GRPC connection.
func (a *GRPCClient) Write(msg types.Message) error {
	return a.WriteWithContext(context.Background(), msg)
}

// WriteWithContext attempts to write message contents to a target GRPC connection.
func (a *GRPCClient) WriteWithContext(ctx context.Context, msg types.Message) error {
	if a.conn == nil {
		return types.ErrNotConnected
	}

	return msg.Iter(func(i int, p types.Part) error {
		data := p.Get() // MUST be a JSON object.
		inMsg := dynamicpb.NewMessage(a.inputDescriptor)
		if err := protojson.Unmarshal(data, inMsg); err != nil {
			return err
		}
		outMsg := dynamicpb.NewMessage(a.outputDescriptor)
		// TODO: set as response.
		return a.conn.Invoke(ctx, a.conf.Method, inMsg, outMsg)
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (a *GRPCClient) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (a *GRPCClient) WaitForClose(time.Duration) error {
	return a.conn.Close()
}

//------------------------------------------------------------------------------
