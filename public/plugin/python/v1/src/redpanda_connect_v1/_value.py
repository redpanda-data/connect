# Copyright 2026 Redpanda Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Conversion helpers between the wire-form proto types
(redpanda.runtime.v1alpha1 Value/Message/Error) and the plugin SDK's
Python representations."""

from __future__ import annotations

import datetime as _dt
from typing import Any

from google.protobuf import timestamp_pb2

from ._message import Message
from ._proto.redpanda.runtime.v1alpha1 import message_pb2

# ----------------------------------------------------------------------
# Value <-> Python any
# ----------------------------------------------------------------------


def proto_to_value(proto: message_pb2.Value) -> Any:
    """Recursively convert a wire-form Value into a Python value."""
    kind = proto.WhichOneof("kind")
    if kind is None or kind == "null_value":
        return None
    if kind == "bool_value":
        return proto.bool_value
    if kind == "integer_value":
        return proto.integer_value
    if kind == "double_value":
        return proto.double_value
    if kind == "string_value":
        return proto.string_value
    if kind == "bytes_value":
        return proto.bytes_value
    if kind == "timestamp_value":
        return proto.timestamp_value.ToDatetime()
    if kind == "struct_value":
        return {k: proto_to_value(v) for k, v in proto.struct_value.fields.items()}
    if kind == "list_value":
        return [proto_to_value(v) for v in proto.list_value.values]
    raise ValueError(f"unknown proto Value kind: {kind}")


def value_to_proto(value: Any) -> message_pb2.Value:
    """Recursively convert a Python value into a wire-form Value."""
    if value is None:
        return message_pb2.Value(null_value=message_pb2.NullValue.NULL_VALUE)
    # bool must come before int — bool is an int subclass in Python.
    if isinstance(value, bool):
        return message_pb2.Value(bool_value=value)
    if isinstance(value, int):
        return message_pb2.Value(integer_value=value)
    if isinstance(value, float):
        return message_pb2.Value(double_value=value)
    if isinstance(value, str):
        return message_pb2.Value(string_value=value)
    if isinstance(value, bytes):
        return message_pb2.Value(bytes_value=value)
    if isinstance(value, _dt.datetime):
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(value)
        return message_pb2.Value(timestamp_value=ts)
    if isinstance(value, dict):
        struct = message_pb2.StructValue()
        for k, v in value.items():
            struct.fields[k].CopyFrom(value_to_proto(v))
        return message_pb2.Value(struct_value=struct)
    if isinstance(value, list):
        lst = message_pb2.ListValue()
        for v in value:
            lst.values.append(value_to_proto(v))
        return message_pb2.Value(list_value=lst)
    raise TypeError(f"unsupported Python type for Value: {type(value).__name__}")


# ----------------------------------------------------------------------
# Message <-> proto
# ----------------------------------------------------------------------


def proto_to_message(proto: message_pb2.Message) -> Message:
    payload: Any
    if proto.WhichOneof("payload") == "bytes":
        payload = proto.bytes
    else:
        payload = proto_to_value(proto.structured)
    metadata = {k: proto_to_value(v) for k, v in proto.metadata.fields.items()}
    error = proto.error.message if proto.HasField("error") and proto.error.message else None
    return Message(payload=payload, metadata=metadata, error=error)


def message_to_proto(msg: Message) -> message_pb2.Message:
    proto = message_pb2.Message()
    payload = msg.payload
    if isinstance(payload, bytes):
        proto.bytes = payload
    else:
        proto.structured.CopyFrom(value_to_proto(payload))
    for k, v in msg.metadata.items():
        proto.metadata.fields[k].CopyFrom(value_to_proto(v))
    if msg.error is not None:
        proto.error.message = msg.error
    return proto


def proto_to_batch(proto: message_pb2.MessageBatch) -> list[Message]:
    return [proto_to_message(m) for m in proto.messages]


def batch_to_proto(batch: list[Message]) -> message_pb2.MessageBatch:
    proto = message_pb2.MessageBatch()
    for m in batch:
        proto.messages.append(message_to_proto(m))
    return proto


def error_to_proto(exc: BaseException) -> message_pb2.Error:
    return message_pb2.Error(message=str(exc))
