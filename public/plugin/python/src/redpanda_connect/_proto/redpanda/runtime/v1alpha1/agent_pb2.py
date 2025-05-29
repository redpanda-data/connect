"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 5, 29, 0, '', 'redpanda/runtime/v1alpha1/agent.proto')
_sym_db = _symbol_database.Default()
from google.protobuf import timestamp_pb2 as google_dot_protobuf_dot_timestamp__pb2
from ....redpanda.runtime.v1alpha1 import message_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_message__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%redpanda/runtime/v1alpha1/agent.proto\x12\x19redpanda.runtime.v1alpha1\x1a\x1fgoogle/protobuf/timestamp.proto\x1a\'redpanda/runtime/v1alpha1/message.proto"F\n\x0cTraceContext\x12\x10\n\x08trace_id\x18\x01 \x01(\t\x12\x0f\n\x07span_id\x18\x02 \x01(\t\x12\x13\n\x0btrace_flags\x18\x04 \x01(\t"7\n\x05Trace\x12.\n\x05spans\x18\x01 \x03(\x0b2\x1f.redpanda.runtime.v1alpha1.Span"\xd3\x02\n\x04Span\x12\x0f\n\x07span_id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12.\n\nstart_time\x18\x03 \x01(\x0b2\x1a.google.protobuf.Timestamp\x12,\n\x08end_time\x18\x04 \x01(\x0b2\x1a.google.protobuf.Timestamp\x12C\n\nattributes\x18\x05 \x03(\x0b2/.redpanda.runtime.v1alpha1.Span.AttributesEntry\x124\n\x0bchild_spans\x18\x06 \x03(\x0b2\x1f.redpanda.runtime.v1alpha1.Span\x1aS\n\x0fAttributesEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12/\n\x05value\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Value:\x028\x01"\x89\x01\n\x12InvokeAgentRequest\x123\n\x07message\x18\x01 \x01(\x0b2".redpanda.runtime.v1alpha1.Message\x12>\n\rtrace_context\x18\x02 \x01(\x0b2\'.redpanda.runtime.v1alpha1.TraceContext"{\n\x13InvokeAgentResponse\x123\n\x07message\x18\x01 \x01(\x0b2".redpanda.runtime.v1alpha1.Message\x12/\n\x05trace\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Trace2|\n\x0cAgentRuntime\x12l\n\x0bInvokeAgent\x12-.redpanda.runtime.v1alpha1.InvokeAgentRequest\x1a..redpanda.runtime.v1alpha1.InvokeAgentResponseB>Z<github.com/redpanda-data/connect/v4/internal/agent/runtimepbb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'redpanda.runtime.v1alpha1.agent_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'Z<github.com/redpanda-data/connect/v4/internal/agent/runtimepb'
    _globals['_SPAN_ATTRIBUTESENTRY']._loaded_options = None
    _globals['_SPAN_ATTRIBUTESENTRY']._serialized_options = b'8\x01'
    _globals['_TRACECONTEXT']._serialized_start = 142
    _globals['_TRACECONTEXT']._serialized_end = 212
    _globals['_TRACE']._serialized_start = 214
    _globals['_TRACE']._serialized_end = 269
    _globals['_SPAN']._serialized_start = 272
    _globals['_SPAN']._serialized_end = 611
    _globals['_SPAN_ATTRIBUTESENTRY']._serialized_start = 528
    _globals['_SPAN_ATTRIBUTESENTRY']._serialized_end = 611
    _globals['_INVOKEAGENTREQUEST']._serialized_start = 614
    _globals['_INVOKEAGENTREQUEST']._serialized_end = 751
    _globals['_INVOKEAGENTRESPONSE']._serialized_start = 753
    _globals['_INVOKEAGENTRESPONSE']._serialized_end = 876
    _globals['_AGENTRUNTIME']._serialized_start = 878
    _globals['_AGENTRUNTIME']._serialized_end = 1002