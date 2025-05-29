"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings
from ....redpanda.runtime.v1alpha1 import processor_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2
GRPC_GENERATED_VERSION = '1.71.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False
try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True
if _version_not_supported:
    raise RuntimeError(f'The grpc package installed is at version {GRPC_VERSION},' + f' but the generated code in redpanda/runtime/v1alpha1/processor_pb2_grpc.py depends on' + f' grpcio>={GRPC_GENERATED_VERSION}.' + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}' + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.')

class BatchProcessorServiceStub(object):
    """BatchProcessor is a Benthos processor implementation that works against
    batches of messages, which allows windowed processing.

    Message batches must be created by upstream components (inputs, buffers, etc)
    otherwise this processor will simply receive batches containing single
    messages.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Init = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchProcessorService/Init', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorInitRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorInitResponse.FromString, _registered_method=True)
        self.ProcessBatch = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchProcessorService/ProcessBatch', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorProcessBatchRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorProcessBatchResponse.FromString, _registered_method=True)
        self.Close = channel.unary_unary('/redpanda.runtime.v1alpha1.BatchProcessorService/Close', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorCloseRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorCloseResponse.FromString, _registered_method=True)

class BatchProcessorServiceServicer(object):
    """BatchProcessor is a Benthos processor implementation that works against
    batches of messages, which allows windowed processing.

    Message batches must be created by upstream components (inputs, buffers, etc)
    otherwise this processor will simply receive batches containing single
    messages.
    """

    def Init(self, request, context):
        """Init is the first method called for a batch processor and it passes the
        user's configuration to the input.

        The schema for the processor configuration is specified in the
        `plugin.yaml` file provided to Redpanda Connect.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ProcessBatch(self, request, context):
        """Process a batch of messages into one or more resulting batches, or return
        an error if the entire batch could not be processed. If zero messages are
        returned and the error is nil then all messages are filtered.

        The provided MessageBatch should NOT be modified, in order to return a
        mutated batch a copy of the slice should be created instead.

        When an error is returned all of the input messages will continue down
        the pipeline but will be marked with the error with *message.SetError,
        and metrics and logs will be emitted.

        In order to add errors to individual messages of the batch for downstream
        handling use message.SetError(err) and return it in the resulting batch
        with a nil error.

        The Message types returned MUST be derived from the provided messages,
        and CANNOT be custom instantiations of Message. In order to copy the
        provided messages use the Copy method.
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

def add_BatchProcessorServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {'Init': grpc.unary_unary_rpc_method_handler(servicer.Init, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorInitRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorInitResponse.SerializeToString), 'ProcessBatch': grpc.unary_unary_rpc_method_handler(servicer.ProcessBatch, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorProcessBatchRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorProcessBatchResponse.SerializeToString), 'Close': grpc.unary_unary_rpc_method_handler(servicer.Close, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorCloseRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorCloseResponse.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('redpanda.runtime.v1alpha1.BatchProcessorService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('redpanda.runtime.v1alpha1.BatchProcessorService', rpc_method_handlers)

class BatchProcessorService(object):
    """BatchProcessor is a Benthos processor implementation that works against
    batches of messages, which allows windowed processing.

    Message batches must be created by upstream components (inputs, buffers, etc)
    otherwise this processor will simply receive batches containing single
    messages.
    """

    @staticmethod
    def Init(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchProcessorService/Init', redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorInitRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorInitResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ProcessBatch(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchProcessorService/ProcessBatch', redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorProcessBatchRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorProcessBatchResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Close(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.BatchProcessorService/Close', redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorCloseRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_processor__pb2.BatchProcessorCloseResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)