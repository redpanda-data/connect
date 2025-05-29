"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'redpanda/runtime/v1alpha1/output.proto')
_sym_db = _symbol_database.Default()
from ....redpanda.runtime.v1alpha1 import message_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_message__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n&redpanda/runtime/v1alpha1/output.proto\x12\x19redpanda.runtime.v1alpha1\x1a\'redpanda/runtime/v1alpha1/message.proto"N\n\x0bBatchPolicy\x12\x11\n\tbyte_size\x18\x01 \x01(\x03\x12\r\n\x05count\x18\x02 \x01(\x03\x12\r\n\x05check\x18\x03 \x01(\t\x12\x0e\n\x06period\x18\x04 \x01(\t"J\n\x16BatchOutputInitRequest\x120\n\x06config\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Value"\x9f\x01\n\x17BatchOutputInitResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error\x12\x15\n\rmax_in_flight\x18\x02 \x01(\x05\x12<\n\x0cbatch_policy\x18\x03 \x01(\x0b2&.redpanda.runtime.v1alpha1.BatchPolicy"\x1b\n\x19BatchOutputConnectRequest"M\n\x1aBatchOutputConnectResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"P\n\x16BatchOutputSendRequest\x126\n\x05batch\x18\x01 \x01(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch"J\n\x17BatchOutputSendResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"\x19\n\x17BatchOutputCloseRequest"K\n\x18BatchOutputCloseResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error2\xe4\x03\n\x12BatchOutputService\x12o\n\x04Init\x121.redpanda.runtime.v1alpha1.BatchOutputInitRequest\x1a2.redpanda.runtime.v1alpha1.BatchOutputInitResponse"\x00\x12x\n\x07Connect\x124.redpanda.runtime.v1alpha1.BatchOutputConnectRequest\x1a5.redpanda.runtime.v1alpha1.BatchOutputConnectResponse"\x00\x12o\n\x04Send\x121.redpanda.runtime.v1alpha1.BatchOutputSendRequest\x1a2.redpanda.runtime.v1alpha1.BatchOutputSendResponse"\x00\x12r\n\x05Close\x122.redpanda.runtime.v1alpha1.BatchOutputCloseRequest\x1a3.redpanda.runtime.v1alpha1.BatchOutputCloseResponse"\x00BBZ@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepbb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'redpanda.runtime.v1alpha1.output_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb'
    _globals['_BATCHPOLICY']._serialized_start = 110
    _globals['_BATCHPOLICY']._serialized_end = 188
    _globals['_BATCHOUTPUTINITREQUEST']._serialized_start = 190
    _globals['_BATCHOUTPUTINITREQUEST']._serialized_end = 264
    _globals['_BATCHOUTPUTINITRESPONSE']._serialized_start = 267
    _globals['_BATCHOUTPUTINITRESPONSE']._serialized_end = 426
    _globals['_BATCHOUTPUTCONNECTREQUEST']._serialized_start = 428
    _globals['_BATCHOUTPUTCONNECTREQUEST']._serialized_end = 455
    _globals['_BATCHOUTPUTCONNECTRESPONSE']._serialized_start = 457
    _globals['_BATCHOUTPUTCONNECTRESPONSE']._serialized_end = 534
    _globals['_BATCHOUTPUTSENDREQUEST']._serialized_start = 536
    _globals['_BATCHOUTPUTSENDREQUEST']._serialized_end = 616
    _globals['_BATCHOUTPUTSENDRESPONSE']._serialized_start = 618
    _globals['_BATCHOUTPUTSENDRESPONSE']._serialized_end = 692
    _globals['_BATCHOUTPUTCLOSEREQUEST']._serialized_start = 694
    _globals['_BATCHOUTPUTCLOSEREQUEST']._serialized_end = 719
    _globals['_BATCHOUTPUTCLOSERESPONSE']._serialized_start = 721
    _globals['_BATCHOUTPUTCLOSERESPONSE']._serialized_end = 796
    _globals['_BATCHOUTPUTSERVICE']._serialized_start = 799
    _globals['_BATCHOUTPUTSERVICE']._serialized_end = 1283