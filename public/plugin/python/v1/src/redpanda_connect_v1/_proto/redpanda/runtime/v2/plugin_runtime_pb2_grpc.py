"""Client and server classes corresponding to protobuf-defined services."""
import grpc
import warnings
from ....redpanda.runtime.v2 import plugin_runtime_pb2 as redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2
GRPC_GENERATED_VERSION = '1.80.0'
GRPC_VERSION = grpc.__version__
_version_not_supported = False
try:
    from grpc._utilities import first_version_is_lower
    _version_not_supported = first_version_is_lower(GRPC_VERSION, GRPC_GENERATED_VERSION)
except ImportError:
    _version_not_supported = True
if _version_not_supported:
    raise RuntimeError(f'The grpc package installed is at version {GRPC_VERSION},' + ' but the generated code in redpanda/runtime/v2/plugin_runtime_pb2_grpc.py depends on' + f' grpcio>={GRPC_GENERATED_VERSION}.' + f' Please upgrade your grpc module to grpcio>={GRPC_GENERATED_VERSION}' + f' or downgrade your generated code using grpcio-tools<={GRPC_VERSION}.')

class PluginRuntimeStub(object):
    """PluginRuntime is the wire protocol between a Redpanda Connect host and a
    plugin subprocess. One plugin binary registers multiple components (any
    mix of inputs / processors / outputs); the host multiplexes instances of
    those components onto the single gRPC connection.

    Lifecycle:
    1. Plugin subprocess connects back to the host's pre-created socket.
    2. Host calls Handshake to negotiate protocol version + capabilities.
    3. Host calls ListComponents to enumerate registered components and
    their config specs.
    4. For every component reference in the user's pipeline YAML, host
    calls OpenInstance and receives an instance_id used on every
    subsequent lifecycle call.
    5. ProcessBatch (processors), ReadBatch+Ack (inputs), SendBatch
    (outputs) all carry instance_id.
    6. CloseInstance shuts a single instance down; subprocess exits when
    the host's gRPC connection is closed.

    PoC scope: only the Handshake / ListComponents / OpenInstance /
    CloseInstance / ProcessBatch surface is implemented. Input + Output
    lifecycle methods are listed here for design completeness but ship in
    follow-up phases.
    """

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Handshake = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/Handshake', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.HandshakeRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.HandshakeResponse.FromString, _registered_method=True)
        self.ListComponents = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/ListComponents', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.ListComponentsRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.ListComponentsResponse.FromString, _registered_method=True)
        self.OpenInstance = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/OpenInstance', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.OpenInstanceRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.OpenInstanceResponse.FromString, _registered_method=True)
        self.CloseInstance = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/CloseInstance', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.CloseInstanceRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.CloseInstanceResponse.FromString, _registered_method=True)
        self.ProcessBatch = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/ProcessBatch', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceProcessRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceProcessResponse.FromString, _registered_method=True)
        self.Connect = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/Connect', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceConnectRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceConnectResponse.FromString, _registered_method=True)
        self.ReadBatch = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/ReadBatch', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceReadRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceReadResponse.FromString, _registered_method=True)
        self.Ack = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/Ack', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceAckRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceAckResponse.FromString, _registered_method=True)
        self.SendBatch = channel.unary_unary('/redpanda.runtime.v2.PluginRuntime/SendBatch', request_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceSendRequest.SerializeToString, response_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceSendResponse.FromString, _registered_method=True)

class PluginRuntimeServicer(object):
    """PluginRuntime is the wire protocol between a Redpanda Connect host and a
    plugin subprocess. One plugin binary registers multiple components (any
    mix of inputs / processors / outputs); the host multiplexes instances of
    those components onto the single gRPC connection.

    Lifecycle:
    1. Plugin subprocess connects back to the host's pre-created socket.
    2. Host calls Handshake to negotiate protocol version + capabilities.
    3. Host calls ListComponents to enumerate registered components and
    their config specs.
    4. For every component reference in the user's pipeline YAML, host
    calls OpenInstance and receives an instance_id used on every
    subsequent lifecycle call.
    5. ProcessBatch (processors), ReadBatch+Ack (inputs), SendBatch
    (outputs) all carry instance_id.
    6. CloseInstance shuts a single instance down; subprocess exits when
    the host's gRPC connection is closed.

    PoC scope: only the Handshake / ListComponents / OpenInstance /
    CloseInstance / ProcessBatch surface is implemented. Input + Output
    lifecycle methods are listed here for design completeness but ship in
    follow-up phases.
    """

    def Handshake(self, request, context):
        """Handshake is the first call the host makes. The plugin advertises its
        SDK metadata and selects a protocol version compatible with the host's
        supported set.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ListComponents(self, request, context):
        """ListComponents returns every component the plugin's Environment has
        registered, including the serialised ConfigSpec for each one.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def OpenInstance(self, request, context):
        """OpenInstance creates a new component instance bound to a parsed user
        config. Returns an instance_id used on every subsequent lifecycle call
        targeting that instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CloseInstance(self, request, context):
        """CloseInstance shuts down a previously-opened instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ProcessBatch(self, request, context):
        """ProcessBatch dispatches a message batch to the processor instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Connect(self, request, context):
        """----------------------------------------------------------------------
        Input + Output lifecycle methods (declared for design completeness;
        not implemented in the PoC).
        ----------------------------------------------------------------------

        Connect establishes the upstream/downstream connection for an input
        or output instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def ReadBatch(self, request, context):
        """ReadBatch reads the next batch from an input instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def Ack(self, request, context):
        """Ack acknowledges or nacks a previously-read batch on an input
        instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def SendBatch(self, request, context):
        """SendBatch dispatches a message batch to an output instance.
        """
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

def add_PluginRuntimeServicer_to_server(servicer, server):
    rpc_method_handlers = {'Handshake': grpc.unary_unary_rpc_method_handler(servicer.Handshake, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.HandshakeRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.HandshakeResponse.SerializeToString), 'ListComponents': grpc.unary_unary_rpc_method_handler(servicer.ListComponents, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.ListComponentsRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.ListComponentsResponse.SerializeToString), 'OpenInstance': grpc.unary_unary_rpc_method_handler(servicer.OpenInstance, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.OpenInstanceRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.OpenInstanceResponse.SerializeToString), 'CloseInstance': grpc.unary_unary_rpc_method_handler(servicer.CloseInstance, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.CloseInstanceRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.CloseInstanceResponse.SerializeToString), 'ProcessBatch': grpc.unary_unary_rpc_method_handler(servicer.ProcessBatch, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceProcessRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceProcessResponse.SerializeToString), 'Connect': grpc.unary_unary_rpc_method_handler(servicer.Connect, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceConnectRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceConnectResponse.SerializeToString), 'ReadBatch': grpc.unary_unary_rpc_method_handler(servicer.ReadBatch, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceReadRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceReadResponse.SerializeToString), 'Ack': grpc.unary_unary_rpc_method_handler(servicer.Ack, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceAckRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceAckResponse.SerializeToString), 'SendBatch': grpc.unary_unary_rpc_method_handler(servicer.SendBatch, request_deserializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceSendRequest.FromString, response_serializer=redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceSendResponse.SerializeToString)}
    generic_handler = grpc.method_handlers_generic_handler('redpanda.runtime.v2.PluginRuntime', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))
    server.add_registered_method_handlers('redpanda.runtime.v2.PluginRuntime', rpc_method_handlers)

class PluginRuntime(object):
    """PluginRuntime is the wire protocol between a Redpanda Connect host and a
    plugin subprocess. One plugin binary registers multiple components (any
    mix of inputs / processors / outputs); the host multiplexes instances of
    those components onto the single gRPC connection.

    Lifecycle:
    1. Plugin subprocess connects back to the host's pre-created socket.
    2. Host calls Handshake to negotiate protocol version + capabilities.
    3. Host calls ListComponents to enumerate registered components and
    their config specs.
    4. For every component reference in the user's pipeline YAML, host
    calls OpenInstance and receives an instance_id used on every
    subsequent lifecycle call.
    5. ProcessBatch (processors), ReadBatch+Ack (inputs), SendBatch
    (outputs) all carry instance_id.
    6. CloseInstance shuts a single instance down; subprocess exits when
    the host's gRPC connection is closed.

    PoC scope: only the Handshake / ListComponents / OpenInstance /
    CloseInstance / ProcessBatch surface is implemented. Input + Output
    lifecycle methods are listed here for design completeness but ship in
    follow-up phases.
    """

    @staticmethod
    def Handshake(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/Handshake', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.HandshakeRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.HandshakeResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ListComponents(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/ListComponents', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.ListComponentsRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.ListComponentsResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def OpenInstance(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/OpenInstance', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.OpenInstanceRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.OpenInstanceResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def CloseInstance(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/CloseInstance', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.CloseInstanceRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.CloseInstanceResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ProcessBatch(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/ProcessBatch', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceProcessRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceProcessResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Connect(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/Connect', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceConnectRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceConnectResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def ReadBatch(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/ReadBatch', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceReadRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceReadResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def Ack(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/Ack', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceAckRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceAckResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)

    @staticmethod
    def SendBatch(request, target, options=(), channel_credentials=None, call_credentials=None, insecure=False, compression=None, wait_for_ready=None, timeout=None, metadata=None):
        return grpc.experimental.unary_unary(request, target, '/redpanda.runtime.v2.PluginRuntime/SendBatch', redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceSendRequest.SerializeToString, redpanda_dot_runtime_dot_v2_dot_plugin__runtime__pb2.InstanceSendResponse.FromString, options, channel_credentials, insecure, call_credentials, compression, wait_for_ready, timeout, metadata, _registered_method=True)