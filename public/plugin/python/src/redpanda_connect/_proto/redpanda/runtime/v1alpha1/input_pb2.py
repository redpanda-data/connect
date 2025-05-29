"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'redpanda/runtime/v1alpha1/input.proto')
_sym_db = _symbol_database.Default()
from ....redpanda.runtime.v1alpha1 import message_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_message__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%redpanda/runtime/v1alpha1/input.proto\x12\x19redpanda.runtime.v1alpha1\x1a\'redpanda/runtime/v1alpha1/message.proto"I\n\x15BatchInputInitRequest\x120\n\x06config\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Value"d\n\x16BatchInputInitResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error\x12\x19\n\x11auto_replay_nacks\x18\x02 \x01(\x08"\x1a\n\x18BatchInputConnectRequest"L\n\x19BatchInputConnectResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"\x17\n\x15BatchInputReadRequest"\x93\x01\n\x16BatchInputReadResponse\x12\x10\n\x08batch_id\x18\x01 \x01(\x04\x126\n\x05batch\x18\x02 \x01(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch\x12/\n\x05error\x18\x03 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"Y\n\x14BatchInputAckRequest\x12\x10\n\x08batch_id\x18\x01 \x01(\x04\x12/\n\x05error\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"H\n\x15BatchInputAckResponse\x12/\n\x05error\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"\x18\n\x16BatchInputCloseRequest"J\n\x17BatchInputCloseResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error2\xc2\x04\n\x11BatchInputService\x12k\n\x04Init\x120.redpanda.runtime.v1alpha1.BatchInputInitRequest\x1a1.redpanda.runtime.v1alpha1.BatchInputInitResponse\x12t\n\x07Connect\x123.redpanda.runtime.v1alpha1.BatchInputConnectRequest\x1a4.redpanda.runtime.v1alpha1.BatchInputConnectResponse\x12p\n\tReadBatch\x120.redpanda.runtime.v1alpha1.BatchInputReadRequest\x1a1.redpanda.runtime.v1alpha1.BatchInputReadResponse\x12h\n\x03Ack\x12/.redpanda.runtime.v1alpha1.BatchInputAckRequest\x1a0.redpanda.runtime.v1alpha1.BatchInputAckResponse\x12n\n\x05Close\x121.redpanda.runtime.v1alpha1.BatchInputCloseRequest\x1a2.redpanda.runtime.v1alpha1.BatchInputCloseResponseBBZ@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepbb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'redpanda.runtime.v1alpha1.input_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb'
    _globals['_BATCHINPUTINITREQUEST']._serialized_start = 109
    _globals['_BATCHINPUTINITREQUEST']._serialized_end = 182
    _globals['_BATCHINPUTINITRESPONSE']._serialized_start = 184
    _globals['_BATCHINPUTINITRESPONSE']._serialized_end = 284
    _globals['_BATCHINPUTCONNECTREQUEST']._serialized_start = 286
    _globals['_BATCHINPUTCONNECTREQUEST']._serialized_end = 312
    _globals['_BATCHINPUTCONNECTRESPONSE']._serialized_start = 314
    _globals['_BATCHINPUTCONNECTRESPONSE']._serialized_end = 390
    _globals['_BATCHINPUTREADREQUEST']._serialized_start = 392
    _globals['_BATCHINPUTREADREQUEST']._serialized_end = 415
    _globals['_BATCHINPUTREADRESPONSE']._serialized_start = 418
    _globals['_BATCHINPUTREADRESPONSE']._serialized_end = 565
    _globals['_BATCHINPUTACKREQUEST']._serialized_start = 567
    _globals['_BATCHINPUTACKREQUEST']._serialized_end = 656
    _globals['_BATCHINPUTACKRESPONSE']._serialized_start = 658
    _globals['_BATCHINPUTACKRESPONSE']._serialized_end = 730
    _globals['_BATCHINPUTCLOSEREQUEST']._serialized_start = 732
    _globals['_BATCHINPUTCLOSEREQUEST']._serialized_end = 756
    _globals['_BATCHINPUTCLOSERESPONSE']._serialized_start = 758
    _globals['_BATCHINPUTCLOSERESPONSE']._serialized_end = 832
    _globals['_BATCHINPUTSERVICE']._serialized_start = 835
    _globals['_BATCHINPUTSERVICE']._serialized_end = 1413