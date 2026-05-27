"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import runtime_version as _runtime_version
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_runtime_version.ValidateProtobufRuntimeVersion(_runtime_version.Domain.PUBLIC, 6, 31, 1, '', 'redpanda/runtime/v2/plugin_runtime.proto')
_sym_db = _symbol_database.Default()
from ....redpanda.runtime.v1alpha1 import message_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_message__pb2
from ....redpanda.runtime.v1alpha1 import output_pb2 as redpanda_dot_runtime_dot_v1alpha1_dot_output__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n(redpanda/runtime/v2/plugin_runtime.proto\x12\x13redpanda.runtime.v2\x1a\'redpanda/runtime/v1alpha1/message.proto\x1a&redpanda/runtime/v1alpha1/output.proto"M\n\x10HandshakeRequest\x12#\n\x1bsupported_protocol_versions\x18\x01 \x03(\r\x12\x14\n\x0chost_version\x18\x02 \x01(\t"\xb6\x01\n\x11HandshakeResponse\x12!\n\x19selected_protocol_version\x18\x01 \x01(\r\x12\x10\n\x08sdk_name\x18\x02 \x01(\t\x12\x13\n\x0bsdk_version\x18\x03 \x01(\t\x12\x10\n\x08language\x18\x04 \x01(\t\x12\x14\n\x0ccapabilities\x18\x05 \x03(\t\x12/\n\x05error\x18\x06 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"\x17\n\x15ListComponentsRequest"V\n\x16ListComponentsResponse\x12<\n\ncomponents\x18\x01 \x03(\x0b2(.redpanda.runtime.v2.ComponentDescriptor"\x84\x01\n\x13ComponentDescriptor\x12\x0c\n\x04name\x18\x01 \x01(\t\x120\n\x04kind\x18\x02 \x01(\x0e2".redpanda.runtime.v2.ComponentKind\x12-\n\x04spec\x18\x03 \x01(\x0b2\x1f.redpanda.runtime.v2.ConfigSpec"\x89\x01\n\nConfigSpec\x12\x0f\n\x07summary\x18\x01 \x01(\t\x12\x13\n\x0bdescription\x18\x02 \x01(\t\x12\x0f\n\x07version\x18\x03 \x01(\t\x12\x12\n\ncategories\x18\x04 \x03(\t\x120\n\x06fields\x18\x05 \x03(\x0b2 .redpanda.runtime.v2.ConfigField"\x8e\x02\n\x0bConfigField\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x13\n\x0bdescription\x18\x02 \x01(\t\x12,\n\x04type\x18\x03 \x01(\x0e2\x1e.redpanda.runtime.v2.FieldType\x12,\n\x04kind\x18\x04 \x01(\x0e2\x1e.redpanda.runtime.v2.FieldKind\x12\x14\n\x0cdefault_json\x18\x05 \x01(\t\x12\x10\n\x08optional\x18\x06 \x01(\x08\x12\x10\n\x08advanced\x18\x07 \x01(\x08\x12\x12\n\ndeprecated\x18\x08 \x01(\x08\x122\n\x08children\x18\t \x03(\x0b2 .redpanda.runtime.v2.ConfigField"_\n\x13OpenInstanceRequest\x12\x16\n\x0ecomponent_name\x18\x01 \x01(\t\x120\n\x06config\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Value"\xcc\x01\n\x14OpenInstanceResponse\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04\x12\x15\n\rmax_in_flight\x18\x02 \x01(\x05\x12<\n\x0cbatch_policy\x18\x03 \x01(\x0b2&.redpanda.runtime.v1alpha1.BatchPolicy\x12\x19\n\x11auto_replay_nacks\x18\x04 \x01(\x08\x12/\n\x05error\x18\x05 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"+\n\x14CloseInstanceRequest\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04"H\n\x15CloseInstanceResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"-\n\x16InstanceConnectRequest\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04"J\n\x17InstanceConnectResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"*\n\x13InstanceReadRequest\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04"\x91\x01\n\x14InstanceReadResponse\x12\x10\n\x08batch_id\x18\x01 \x01(\x04\x126\n\x05batch\x18\x02 \x01(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch\x12/\n\x05error\x18\x03 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"l\n\x12InstanceAckRequest\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04\x12\x10\n\x08batch_id\x18\x02 \x01(\x04\x12/\n\x05error\x18\x03 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"F\n\x13InstanceAckResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"e\n\x16InstanceProcessRequest\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04\x126\n\x05batch\x18\x02 \x01(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch"\x84\x01\n\x17InstanceProcessResponse\x128\n\x07batches\x18\x01 \x03(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch\x12/\n\x05error\x18\x02 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error"b\n\x13InstanceSendRequest\x12\x13\n\x0binstance_id\x18\x01 \x01(\x04\x126\n\x05batch\x18\x02 \x01(\x0b2\'.redpanda.runtime.v1alpha1.MessageBatch"G\n\x14InstanceSendResponse\x12/\n\x05error\x18\x01 \x01(\x0b2 .redpanda.runtime.v1alpha1.Error*\x82\x01\n\rComponentKind\x12\x1e\n\x1aCOMPONENT_KIND_UNSPECIFIED\x10\x00\x12\x18\n\x14COMPONENT_KIND_INPUT\x10\x01\x12\x1c\n\x18COMPONENT_KIND_PROCESSOR\x10\x02\x12\x19\n\x15COMPONENT_KIND_OUTPUT\x10\x03*\x94\x01\n\tFieldType\x12\x1a\n\x16FIELD_TYPE_UNSPECIFIED\x10\x00\x12\x15\n\x11FIELD_TYPE_STRING\x10\x01\x12\x12\n\x0eFIELD_TYPE_INT\x10\x02\x12\x14\n\x10FIELD_TYPE_FLOAT\x10\x03\x12\x13\n\x0fFIELD_TYPE_BOOL\x10\x04\x12\x15\n\x11FIELD_TYPE_OBJECT\x10\x05*g\n\tFieldKind\x12\x1a\n\x16FIELD_KIND_UNSPECIFIED\x10\x00\x12\x15\n\x11FIELD_KIND_SCALAR\x10\x01\x12\x13\n\x0fFIELD_KIND_LIST\x10\x02\x12\x12\n\x0eFIELD_KIND_MAP\x10\x032\x92\x07\n\rPluginRuntime\x12Z\n\tHandshake\x12%.redpanda.runtime.v2.HandshakeRequest\x1a&.redpanda.runtime.v2.HandshakeResponse\x12i\n\x0eListComponents\x12*.redpanda.runtime.v2.ListComponentsRequest\x1a+.redpanda.runtime.v2.ListComponentsResponse\x12c\n\x0cOpenInstance\x12(.redpanda.runtime.v2.OpenInstanceRequest\x1a).redpanda.runtime.v2.OpenInstanceResponse\x12f\n\rCloseInstance\x12).redpanda.runtime.v2.CloseInstanceRequest\x1a*.redpanda.runtime.v2.CloseInstanceResponse\x12i\n\x0cProcessBatch\x12+.redpanda.runtime.v2.InstanceProcessRequest\x1a,.redpanda.runtime.v2.InstanceProcessResponse\x12d\n\x07Connect\x12+.redpanda.runtime.v2.InstanceConnectRequest\x1a,.redpanda.runtime.v2.InstanceConnectResponse\x12`\n\tReadBatch\x12(.redpanda.runtime.v2.InstanceReadRequest\x1a).redpanda.runtime.v2.InstanceReadResponse\x12X\n\x03Ack\x12\'.redpanda.runtime.v2.InstanceAckRequest\x1a(.redpanda.runtime.v2.InstanceAckResponse\x12`\n\tSendBatch\x12(.redpanda.runtime.v2.InstanceSendRequest\x1a).redpanda.runtime.v2.InstanceSendResponseBEZCgithub.com/redpanda-data/connect/v4/internal/rpcplugin/v2/runtimepbb\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'redpanda.runtime.v2.plugin_runtime_pb2', _globals)
if not _descriptor._USE_C_DESCRIPTORS:
    _globals['DESCRIPTOR']._loaded_options = None
    _globals['DESCRIPTOR']._serialized_options = b'ZCgithub.com/redpanda-data/connect/v4/internal/rpcplugin/v2/runtimepb'
    _globals['_COMPONENTKIND']._serialized_start = 2403
    _globals['_COMPONENTKIND']._serialized_end = 2533
    _globals['_FIELDTYPE']._serialized_start = 2536
    _globals['_FIELDTYPE']._serialized_end = 2684
    _globals['_FIELDKIND']._serialized_start = 2686
    _globals['_FIELDKIND']._serialized_end = 2789
    _globals['_HANDSHAKEREQUEST']._serialized_start = 146
    _globals['_HANDSHAKEREQUEST']._serialized_end = 223
    _globals['_HANDSHAKERESPONSE']._serialized_start = 226
    _globals['_HANDSHAKERESPONSE']._serialized_end = 408
    _globals['_LISTCOMPONENTSREQUEST']._serialized_start = 410
    _globals['_LISTCOMPONENTSREQUEST']._serialized_end = 433
    _globals['_LISTCOMPONENTSRESPONSE']._serialized_start = 435
    _globals['_LISTCOMPONENTSRESPONSE']._serialized_end = 521
    _globals['_COMPONENTDESCRIPTOR']._serialized_start = 524
    _globals['_COMPONENTDESCRIPTOR']._serialized_end = 656
    _globals['_CONFIGSPEC']._serialized_start = 659
    _globals['_CONFIGSPEC']._serialized_end = 796
    _globals['_CONFIGFIELD']._serialized_start = 799
    _globals['_CONFIGFIELD']._serialized_end = 1069
    _globals['_OPENINSTANCEREQUEST']._serialized_start = 1071
    _globals['_OPENINSTANCEREQUEST']._serialized_end = 1166
    _globals['_OPENINSTANCERESPONSE']._serialized_start = 1169
    _globals['_OPENINSTANCERESPONSE']._serialized_end = 1373
    _globals['_CLOSEINSTANCEREQUEST']._serialized_start = 1375
    _globals['_CLOSEINSTANCEREQUEST']._serialized_end = 1418
    _globals['_CLOSEINSTANCERESPONSE']._serialized_start = 1420
    _globals['_CLOSEINSTANCERESPONSE']._serialized_end = 1492
    _globals['_INSTANCECONNECTREQUEST']._serialized_start = 1494
    _globals['_INSTANCECONNECTREQUEST']._serialized_end = 1539
    _globals['_INSTANCECONNECTRESPONSE']._serialized_start = 1541
    _globals['_INSTANCECONNECTRESPONSE']._serialized_end = 1615
    _globals['_INSTANCEREADREQUEST']._serialized_start = 1617
    _globals['_INSTANCEREADREQUEST']._serialized_end = 1659
    _globals['_INSTANCEREADRESPONSE']._serialized_start = 1662
    _globals['_INSTANCEREADRESPONSE']._serialized_end = 1807
    _globals['_INSTANCEACKREQUEST']._serialized_start = 1809
    _globals['_INSTANCEACKREQUEST']._serialized_end = 1917
    _globals['_INSTANCEACKRESPONSE']._serialized_start = 1919
    _globals['_INSTANCEACKRESPONSE']._serialized_end = 1989
    _globals['_INSTANCEPROCESSREQUEST']._serialized_start = 1991
    _globals['_INSTANCEPROCESSREQUEST']._serialized_end = 2092
    _globals['_INSTANCEPROCESSRESPONSE']._serialized_start = 2095
    _globals['_INSTANCEPROCESSRESPONSE']._serialized_end = 2227
    _globals['_INSTANCESENDREQUEST']._serialized_start = 2229
    _globals['_INSTANCESENDREQUEST']._serialized_end = 2327
    _globals['_INSTANCESENDRESPONSE']._serialized_start = 2329
    _globals['_INSTANCESENDRESPONSE']._serialized_end = 2400
    _globals['_PLUGINRUNTIME']._serialized_start = 2792
    _globals['_PLUGINRUNTIME']._serialized_end = 3706