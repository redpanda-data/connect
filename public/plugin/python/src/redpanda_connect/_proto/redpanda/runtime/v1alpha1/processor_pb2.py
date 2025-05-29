"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'redpanda/runtime/v1alpha1/processor.proto')
_sym_db = _symbol_database.Default()
from ....redpanda.runtime.v1alpha1 import message_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_message__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n)redpanda/runtime/v1alpha1/processor.proto\x12\x19redpanda.runtime.v1alpha1\x1a\'redpanda/runtime/v1alpha1/message.proto"M\n\x19BatchProcessorInitRequest\x120\n\x06config\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Value"M\n\x1aBatchProcessorInitResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"[\n!BatchProcessorProcessBatchRequest\x126\n\x05batch\x18\x01 \x01(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch"\x8f\x01\n"BatchProcessorProcessBatchResponse\x128\n\x07batches\x18\x01 \x03(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch\x12/\n\x05error\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"\x1c\n\x1aBatchProcessorCloseRequest"N\n\x1bBatchProcessorCloseResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error2\x98\x03\n\x15BatchProcessorService\x12u\n\x04Init\x124.redpanda.runtime.v1alpha1.BatchProcessorInitRequest\x1a5.redpanda.runtime.v1alpha1.BatchProcessorInitResponse"\x00\x12\x8d\x01\n\x0cProcessBatch\x12<.redpanda.runtime.v1alpha1.BatchProcessorProcessBatchRequest\x1a=.redpanda.runtime.v1alpha1.BatchProcessorProcessBatchResponse"\x00\x12x\n\x05Close\x125.redpanda.runtime.v1alpha1.BatchProcessorCloseRequest\x1a6.redpanda.runtime.v1alpha1.BatchProcessorCloseResponse"\x00BBZ@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepbb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'redpanda.runtime.v1alpha1.processor_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb'
    _globals['_BATCHPROCESSORINITREQUEST']._serialized_start = 113
    _globals['_BATCHPROCESSORINITREQUEST']._serialized_end = 190
    _globals['_BATCHPROCESSORINITRESPONSE']._serialized_start = 192
    _globals['_BATCHPROCESSORINITRESPONSE']._serialized_end = 269
    _globals['_BATCHPROCESSORPROCESSBATCHREQUEST']._serialized_start = 271
    _globals['_BATCHPROCESSORPROCESSBATCHREQUEST']._serialized_end = 362
    _globals['_BATCHPROCESSORPROCESSBATCHRESPONSE']._serialized_start = 365
    _globals['_BATCHPROCESSORPROCESSBATCHRESPONSE']._serialized_end = 508
    _globals['_BATCHPROCESSORCLOSEREQUEST']._serialized_start = 510
    _globals['_BATCHPROCESSORCLOSEREQUEST']._serialized_end = 538
    _globals['_BATCHPROCESSORCLOSERESPONSE']._serialized_start = 540
    _globals['_BATCHPROCESSORCLOSERESPONSE']._serialized_end = 618
    _globals['_BATCHPROCESSORSERVICE']._serialized_start = 621
    _globals['_BATCHPROCESSORSERVICE']._serialized_end = 1029