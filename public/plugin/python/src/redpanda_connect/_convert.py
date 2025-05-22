"""
Convert between protobuf and Python types for Redpanda Connect.
"""

from google.protobuf import duration_pb2, timestamp_pb2

from redpanda.runtime.v1alpha1 import message_pb2

from .core import Message, Value
from .errors import BackoffError, BaseError, EndOfInputError, NotConnectedError


def proto_to_value(proto: message_pb2.Value) -> Value:
    kind = proto.WhichOneof("kind")
    if kind == "bool_value":
        return proto.bool_value
    elif kind == "integer_value":
        return proto.integer_value
    elif kind == "double_value":
        return proto.double_value
    elif kind == "string_value":
        return proto.string_value
    elif kind == "bytes_value":
        return proto.bytes_value
    elif kind == "timestamp_value":
        return proto.timestamp_value.ToDatetime()
    elif kind == "struct_value":
        return {k: proto_to_value(v) for k, v in proto.struct_value.fields.items()}
    elif kind == "list_value":
        return [proto_to_value(v) for v in proto.list_value.values]
    elif kind == "null_value":
        return None
    else:
        raise ValueError(f"Unknown proto value kind: {kind}")


def value_to_proto(value: Value) -> message_pb2.Value:
    if isinstance(value, bool):
        return message_pb2.Value(bool_value=value)
    elif isinstance(value, int):
        return message_pb2.Value(integer_value=value)
    elif isinstance(value, float):
        return message_pb2.Value(double_value=value)
    elif isinstance(value, str):
        return message_pb2.Value(string_value=value)
    elif isinstance(value, bytes):
        return message_pb2.Value(bytes_value=value)
    elif isinstance(value, dict):
        struct_value = message_pb2.StructValue()
        for k, v in value.items():
            struct_value.fields[k].CopyFrom(value_to_proto(v))
        return message_pb2.Value(struct_value=struct_value)
    elif isinstance(value, list):
        list_value = message_pb2.ListValue()
        for v in value:
            list_value.values.append(value_to_proto(v))
        return message_pb2.Value(list_value=list_value)
    elif value is None:
        return message_pb2.Value(null_value=message_pb2.NullValue.NULL_VALUE)
    else:
        timestamp_value = timestamp_pb2.Timestamp()
        timestamp_value.FromDatetime(value)
        return message_pb2.Value(timestamp_value=timestamp_value)
    raise ValueError(f"Unsupported value type: {type(value)}")  # pyright: ignore[reportUnreachable]


def message_to_proto(message: Message) -> message_pb2.Message:
    proto = message_pb2.Message()
    if isinstance(message.payload, bytes):
        proto.bytes = message.payload
    elif isinstance(message.payload, str):
        proto.bytes = message.payload.encode()
    else:
        proto.structured.CopyFrom(value_to_proto(message.payload))
    for k, v in message.metadata.items():
        proto.metadata.fields[k].CopyFrom(value_to_proto(v))
    if message.error:
        proto.error.CopyFrom(error_to_proto(message.error))
    return proto


def proto_to_error(proto: message_pb2.Error) -> BaseError | None:
    if not proto.message:
        return None
    detail = proto.WhichOneof("detail")
    if detail == "not_connected":
        return NotConnectedError()
    elif detail == "backoff":
        duration = proto.backoff.ToTimedelta()
        return BackoffError(proto.message, duration)
    elif detail == "end_of_input":
        return EndOfInputError()
    else:
        return BaseError(proto.message)


def error_to_proto(error: BaseError) -> message_pb2.Error:
    if isinstance(error, NotConnectedError):
        return message_pb2.Error(
            message=error.message, not_connected=message_pb2.Error.NotConnected()
        )
    if isinstance(error, BackoffError):
        duration = duration_pb2.Duration()
        duration.FromTimedelta(error.duration)
        return message_pb2.Error(message=error.message, backoff=duration)
    if isinstance(error, EndOfInputError):
        return message_pb2.Error(message=error.message, end_of_input=message_pb2.Error.EndOfInput())
    return message_pb2.Error(message=error.message)


def proto_to_message(proto: message_pb2.Message) -> Message:
    if proto.WhichOneof("payload") == "bytes":
        payload = proto.bytes
    else:
        payload = proto_to_value(proto.structured)
    metadata = {k: proto_to_value(v) for k, v in proto.metadata.fields.items()}
    error = None
    if proto.error:
        error = proto_to_error(proto.error)
    return Message(payload=payload, metadata=metadata, error=error)


def proto_to_batch(proto: message_pb2.MessageBatch) -> list[Message]:
    return [proto_to_message(m) for m in proto.messages]


def batch_to_proto(batch: list[Message]) -> message_pb2.MessageBatch:
    proto = message_pb2.MessageBatch()
    for m in batch:
        proto.messages.append(message_to_proto(m))
    return proto
