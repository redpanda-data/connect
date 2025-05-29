"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'redpanda/runtime/v1alpha1/message.proto')
_sym_db = _symbol_database.Default()
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from google.protobuf import duration_pb2 as google_dot_protobuf_dot_duration__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\'redpanda/runtime/v1alpha1/message.proto\x12\x19redpanda.runtime.v1alpha1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\x1egoogle/protobuf/duration.proto"\xa2\x01\n\x0bStructValue\x12B\n\x06fields\x18\x01 \x03(\x0b22.redpanda.runtime.v1alpha1.StructValue.FieldsEntry\x1aO\n\x0bFieldsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12/\n\x05value\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Value:\x028\x01"=\n\tListValue\x120\n\x06values\x18\x01 \x03(\x0b2 .redpanda.runtime.v1alpha1.Value"\xf4\x02\n\x05Value\x12:\n\nnull_value\x18\x01 \x01(\x0e2$.redpanda.runtime.v1alpha1.NullValueH\x00\x12\x16\n\x0cstring_value\x18\x02 \x01(\tH\x00\x12\x17\n\rinteger_value\x18\x03 \x01(\x03H\x00\x12\x16\n\x0cdouble_value\x18\x04 \x01(\x01H\x00\x12\x14\n\nbool_value\x18\x05 \x01(\x08H\x00\x125\n\x0ftimestamp_value\x18\x06 \x01(\x0b2\x1a.google.protobuf.TimestampH\x00\x12\x15\n\x0bbytes_value\x18\x07 \x01(\x0cH\x00\x12>\n\x0cstruct_value\x18\x08 \x01(\x0b2&.redpanda.runtime.v1alpha1.StructValueH\x00\x12:\n\nlist_value\x18\t \x01(\x0b2$.redpanda.runtime.v1alpha1.ListValueH\x00B\x06\n\x04kind"\xfb\x01\n\x05Error\x12\x0f\n\x07message\x18\x01 \x01(\t\x12,\n\x07backoff\x18\x02 \x01(\x0b2\x19.google.protobuf.DurationH\x00\x12F\n\rnot_connected\x18\x03 \x01(\x0b2-.redpanda.runtime.v1alpha1.Error.NotConnectedH\x00\x12C\n\x0cend_of_input\x18\x04 \x01(\x0b2+.redpanda.runtime.v1alpha1.Error.EndOfInputH\x00\x1a\x0e\n\x0cNotConnected\x1a\x0c\n\nEndOfInputB\x08\n\x06detail"\xc8\x01\n\x07Message\x12\x0f\n\x05bytes\x18\x01 \x01(\x0cH\x00\x126\n\nstructured\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.ValueH\x00\x128\n\x08metadata\x18\x03 \x01(\x0b2&.redpanda.runtime.v1alpha1.StructValue\x12/\n\x05error\x18\x04 \x01(\x0b2 .redpanda.runtime.v1alpha1.ErrorB\t\n\x07payload"D\n\x0cMessageBatch\x124\n\x08messages\x18\x01 \x03(\x0b2".redpanda.runtime.v1alpha1.Message*\x1b\n\tNullValue\x12\x0e\n\nNULL_VALUE\x10\x00BBZ@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepbb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'redpanda.runtime.v1alpha1.message_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z@github.com/redpanda-data/connect/v4/internal/rpcplugin/runtimepb'
    _globals['_STRUCTVALUE_FIELDSENTRY']._loaded_options = None
    _globals['_STRUCTVALUE_FIELDSENTRY']._serialized_options = b'8\x01'
    _globals['_NULLVALUE']._serialized_start = 1265
    _globals['_NULLVALUE']._serialized_end = 1292
    _globals['_STRUCTVALUE']._serialized_start = 136
    _globals['_STRUCTVALUE']._serialized_end = 298
    _globals['_STRUCTVALUE_FIELDSENTRY']._serialized_start = 219
    _globals['_STRUCTVALUE_FIELDSENTRY']._serialized_end = 298
    _globals['_LISTVALUE']._serialized_start = 300
    _globals['_LISTVALUE']._serialized_end = 361
    _globals['_VALUE']._serialized_start = 364
    _globals['_VALUE']._serialized_end = 736
    _globals['_ERROR']._serialized_start = 739
    _globals['_ERROR']._serialized_end = 990
    _globals['_ERROR_NOTCONNECTED']._serialized_start = 952
    _globals['_ERROR_NOTCONNECTED']._serialized_end = 966
    _globals['_ERROR_ENDOFINPUT']._serialized_start = 968
    _globals['_ERROR_ENDOFINPUT']._serialized_end = 980
    _globals['_MESSAGE']._serialized_start = 993
    _globals['_MESSAGE']._serialized_end = 1193
    _globals['_MESSAGEBATCH']._serialized_start = 1195
    _globals['_MESSAGEBATCH']._serialized_end = 1263