"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings
from ....redpanda.runtime.v1alpha1 import input_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2
GRPC_GENERATED_VERSION = '1.71.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False
try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True
if _version_not_supported:
    raise RuntimeError(f'The grpc package installed is at version {GRPC_VERSION},' + f' but the generated code in redpanda/runtime/v1alpha1/input_pb2_grpc.py depends on' + f' grpcio>={GRPC_GENERATED_VERSION}.' + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}' + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.')

class BatchInputServiceStub(object):
    """BatchInput is an interface implemented by Benthos inputs that produce
    messages in batches, where there is a desire to process and send the batch as
    a logical group rather than as individual messages.

    Calls to ReadBatch should block until either a message batch is ready to
    process, the connection is lost, or the RPC deadline is reached.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Init = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchInputService/Init', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputInitRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputInitResponse.FromString, _registered_method=True)
        self.Connect = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchInputService/Connect', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputConnectRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputConnectResponse.FromString, _registered_method=True)
        self.ReadBatch = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchInputService/ReadBatch', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputReadRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputReadResponse.FromString, _registered_method=True)
        self.Ack = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchInputService/Ack', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputAckRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputAckResponse.FromString, _registered_method=True)
        self.Close = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchInputService/Close', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputCloseRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputCloseResponse.FromString, _registered_method=True)

class BatchInputServiceServicer(object):
    """BatchInput is an interface implemented by Benthos inputs that produce
    messages in batches, where there is a desire to process and send the batch as
    a logical group rather than as individual messages.

    Calls to ReadBatch should block until either a message batch is ready to
    process, the connection is lost, or the RPC deadline is reached.
    """

    def Init(self, request, context):
        """Init is the first method called for a batch input and it passes the user's
        configuration to the input.

        The schema for the input configuration is specified in the `plugin.yaml`
        file provided to Redpanda Connect.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Connect(self, request, context):
        """Establish a connection to the upstream service. Connect will always be
        called first when a reader is instantiated, and will be continuously
        called with back off until a nil error is returned.

        Once Connect returns a nil error the Read method will be called until
        either ErrNotConnected is returned, or the reader is closed.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReadBatch(self, request, context):
        """Read a message batch from a source, along with a function to be called
        once the entire batch can be either acked (successfully sent or
        intentionally filtered) or nacked (failed to be processed or dispatched
        to the output).

        The Ack will be called for every message batch at least once, but
        there are no guarantees as to when this will occur. If your input
        implementation doesn't have a specific mechanism for dealing with a nack
        then you can instruct the Connect framework to auto_replay_nacks in the
        InitResponse to get automatic retries.

        If this method returns Error.NotConnected then ReadBatch will not be called
        again until Connect has returned a nil error. If Error.EndOfInput is
        returned then Read will no longer be called and the pipeline will
        gracefully terminate.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Ack(self, request, context):
        """Acknowledge a message batch. This function ensures that the source of the
        message receives either an acknowledgement (error is missing) or an error
        that can either be propagated upstream as a nack, or trigger a reattempt at
        delivering the same message.

        If your input implementation doesn't have a specific mechanism for dealing
        with a nack then you can wrap your input implementation with AutoRetryNacks
        to get automatic retries, and noop this function.
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

def add_BatchInputServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {'Init': grpc.unary_unary_rpc_method_handler(servicer.Init, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputInitRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputInitResponse.SerializeToString), 'Connect': grpc.unary_unary_rpc_method_handler(servicer.Connect, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputConnectRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputConnectResponse.SerializeToString), 'ReadBatch': grpc.unary_unary_rpc_method_handler(servicer.ReadBatch, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputReadRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputReadResponse.SerializeToString), 'Ack': grpc.unary_unary_rpc_method_handler(servicer.Ack, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputAckRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputAckResponse.SerializeToString), 'Close': grpc.unary_unary_rpc_method_handler(servicer.Close, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputCloseRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputCloseResponse.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('redpanda.runtime.v1alpha1.BatchInputService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('redpanda.runtime.v1alpha1.BatchInputService', rpc_method_handlers)

class BatchInputService(object):
    """BatchInput is an interface implemented by Benthos inputs that produce
    messages in batches, where there is a desire to process and send the batch as
    a logical group rather than as individual messages.

    Calls to ReadBatch should block until either a message batch is ready to
    process, the connection is lost, or the RPC deadline is reached.
    """

    @staticmethod
    def Init(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchInputService/Init', redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputInitRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputInitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Connect(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchInputService/Connect', redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputConnectRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputConnectResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ReadBatch(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchInputService/ReadBatch', redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputReadRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputReadResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Ack(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchInputService/Ack', redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputAckRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputAckResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Close(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchInputService/Close', redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputCloseRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_input__pb2.BatchInputCloseResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)