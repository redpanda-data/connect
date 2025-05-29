"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings
from ....redpanda.runtime.v1alpha1 import output_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2
GRPC_GENERATED_VERSION = '1.71.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False
try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True
if _version_not_supported:
    raise RuntimeError(f'The grpc package installed is at version {GRPC_VERSION},' + f' but the generated code in redpanda/runtime/v1alpha1/output_pb2_grpc.py depends on' + f' grpcio>={GRPC_GENERATED_VERSION}.' + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}' + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.')

class BatchOutputServiceStub(object):
    """BatchOutput is an interface implemented by Benthos outputs that require
    Benthos to batch messages before dispatch in order to improve throughput.
    Each call to WriteBatch should block until either all messages in the batch
    have been successfully or unsuccessfully sent, or the RPC deadline is reached.

    Multiple write calls can be performed in parallel, and the constructor of an
    output must provide a MaxInFlight parameter indicating the maximum number of
    parallel batched write calls the output supports.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Init = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchOutputService/Init', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputInitRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputInitResponse.FromString, _registered_method=True)
        self.Connect = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchOutputService/Connect', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputConnectRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputConnectResponse.FromString, _registered_method=True)
        self.Send = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchOutputService/Send', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputSendRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputSendResponse.FromString, _registered_method=True)
        self.Close = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchOutputService/Close', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputCloseRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputCloseResponse.FromString, _registered_method=True)

class BatchOutputServiceServicer(object):
    """BatchOutput is an interface implemented by Benthos outputs that require
    Benthos to batch messages before dispatch in order to improve throughput.
    Each call to WriteBatch should block until either all messages in the batch
    have been successfully or unsuccessfully sent, or the RPC deadline is reached.

    Multiple write calls can be performed in parallel, and the constructor of an
    output must provide a MaxInFlight parameter indicating the maximum number of
    parallel batched write calls the output supports.
    """

    def Init(self, request, context):
        """Init is the first method called for a batch output and it passes the user's
        configuration to the output.

        The schema for the output configuration is specified in the `plugin.yaml`
        file provided to Redpanda Connect.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Connect(self, request, context):
        """Establish a connection to the downstream service. Connect will always be
        called first when a writer is instantiated, and will be continuously
        called with back off until a nil error is returned.

        Once Connect returns a nil error the write method will be called until
        either Error.NotConnected is returned, or the writer is closed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Send(self, request, context):
        """Write a batch of messages to a sink, or return an error if delivery is
        not possible.

        If this method returns Error.NotConnected then write will not be called
        again until Connect has returned a nil error.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Close(self, request, context):
        """Close the component, blocks until either the underlying resources are
        cleaned up or the RPC deadline is reached.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_BatchOutputServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {'Init': grpc.unary_unary_rpc_method_handler(servicer.Init, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputInitRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputInitResponse.SerializeToString), 'Connect': grpc.unary_unary_rpc_method_handler(servicer.Connect, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputConnectRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputConnectResponse.SerializeToString), 'Send': grpc.unary_unary_rpc_method_handler(servicer.Send, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputSendRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputSendResponse.SerializeToString), 'Close': grpc.unary_unary_rpc_method_handler(servicer.Close, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputCloseRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputCloseResponse.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('redpanda.runtime.v1alpha1.BatchOutputService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('redpanda.runtime.v1alpha1.BatchOutputService', rpc_method_handlers)

class BatchOutputService(object):
    """BatchOutput is an interface implemented by Benthos outputs that require
    Benthos to batch messages before dispatch in order to improve throughput.
    Each call to WriteBatch should block until either all messages in the batch
    have been successfully or unsuccessfully sent, or the RPC deadline is reached.

    Multiple write calls can be performed in parallel, and the constructor of an
    output must provide a MaxInFlight parameter indicating the maximum number of
    parallel batched write calls the output supports.
    """

    @staticmethod
    def Init(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchOutputService/Init', redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputInitRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputInitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Connect(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchOutputService/Connect', redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputConnectRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputConnectResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Send(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchOutputService/Send', redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputSendRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputSendResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Close(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchOutputService/Close', redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputCloseRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2.BatchOutputCloseResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)