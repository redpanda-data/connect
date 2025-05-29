"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings
from ....redpanda.runtime.v1alpha1 import agent_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2
GRPC_GENERATED_VERSION = '1.71.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False
try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True
if _version_not_supported:
    raise RuntimeError(f'The grpc package installed is at version {GRPC_VERSION},' + f' but the generated code in redpanda/runtime/v1alpha1/agent_pb2_grpc.py depends on' + f' grpcio>={GRPC_GENERATED_VERSION}.' + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}' + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.')

class AgentRuntimeStub(object):
    """`AgentRuntime` is the service that provides the ability to invoke an agent.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.InvokeAgent = channel.unary_unary('/redpanda.runtime.v1alpha1.AgentRuntime/InvokeAgent', request_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2.InvokeAgentRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2.InvokeAgentResponse.FromString, _registered_method=True)

class AgentRuntimeServicer(object):
    """`AgentRuntime` is the service that provides the ability to invoke an agent.
    """

    def InvokeAgent(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_AgentRuntimeServicer_to_server(servicer, server):
    rpc_method_handlers = {'InvokeAgent': grpc.unary_unary_rpc_method_handler(servicer.InvokeAgent, request_deserializer=redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2.InvokeAgentRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2.InvokeAgentResponse.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('redpanda.runtime.v1alpha1.AgentRuntime', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('redpanda.runtime.v1alpha1.AgentRuntime', rpc_method_handlers)

class AgentRuntime(object):
    """`AgentRuntime` is the service that provides the ability to invoke an agent.
    """

    @staticmethod
    def InvokeAgent(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v1alpha1.AgentRuntime/InvokeAgent', redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2.InvokeAgentRequest.SerializeToString, redpanda_dot_runtime_dot_v1alpha1_dot_agent__pb2.InvokeAgentResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)