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

"""Configuration-spec mirror builder for the Python v1 plugin SDK.

The shape mirrors the Go SDK at
``public/plugin/go/rpcn/v1``: plugin authors compose a ``ConfigSpec``
out of typed ``ConfigField`` builders. At Serve() time the spec is
serialised into a ``ConfigSpec`` proto that the host reconstructs into
a real benthos ``service.ConfigSpec`` for linting + parsing.
"""

from __future__ import annotations

import enum
import json
from typing import Any

from ._proto.redpanda.runtime.v2 import plugin_runtime_pb2 as _pb


class _FieldType(enum.IntEnum):
    UNSPECIFIED = 0
    STRING = 1
    INT = 2
    FLOAT = 3
    BOOL = 4
    OBJECT = 5


class _FieldKind(enum.IntEnum):
    UNSPECIFIED = 0
    SCALAR = 1
    LIST = 2
    MAP = 3


class ConfigField:
    """Mirror of ``rpcn.ConfigField`` from the Go SDK."""

    __slots__ = (
        "_name",
        "_description",
        "_type",
        "_kind",
        "_default",
        "_has_default",
        "_optional",
        "_advanced",
        "_deprecated",
        "_children",
    )

    def __init__(
        self,
        name: str,
        field_type: _FieldType,
        field_kind: _FieldKind = _FieldKind.SCALAR,
        children: list["ConfigField"] | None = None,
    ) -> None:
        self._name = name
        self._description = ""
        self._type = field_type
        self._kind = field_kind
        self._default: Any = None
        self._has_default = False
        self._optional = False
        self._advanced = False
        self._deprecated = False
        self._children: list[ConfigField] = list(children) if children else []

    def description(self, text: str) -> "ConfigField":
        self._description = text
        return self

    def default(self, value: Any) -> "ConfigField":
        self._default = value
        self._has_default = True
        return self

    def optional(self) -> "ConfigField":
        self._optional = True
        return self

    def advanced(self) -> "ConfigField":
        self._advanced = True
        return self

    def deprecated(self) -> "ConfigField":
        self._deprecated = True
        return self

    def to_proto(self) -> _pb.ConfigField:
        pb = _pb.ConfigField(
            name=self._name,
            description=self._description,
            type=int(self._type),
            kind=int(self._kind),
            optional=self._optional,
            advanced=self._advanced,
            deprecated=self._deprecated,
            children=[c.to_proto() for c in self._children],
        )
        if self._has_default:
            pb.default_json = json.dumps(self._default)
        return pb


# Field constructors mirroring rpcn.NewXxxField in Go.


def StringField(name: str) -> ConfigField:
    return ConfigField(name, _FieldType.STRING, _FieldKind.SCALAR)


def IntField(name: str) -> ConfigField:
    return ConfigField(name, _FieldType.INT, _FieldKind.SCALAR)


def FloatField(name: str) -> ConfigField:
    return ConfigField(name, _FieldType.FLOAT, _FieldKind.SCALAR)


def BoolField(name: str) -> ConfigField:
    return ConfigField(name, _FieldType.BOOL, _FieldKind.SCALAR)


def ObjectField(name: str, *children: ConfigField) -> ConfigField:
    return ConfigField(name, _FieldType.OBJECT, _FieldKind.SCALAR, list(children))


class ConfigSpec:
    """Mirror of ``rpcn.ConfigSpec`` from the Go SDK."""

    __slots__ = ("_summary", "_description", "_version", "_categories", "_fields")

    def __init__(self) -> None:
        self._summary = ""
        self._description = ""
        self._version = ""
        self._categories: list[str] = []
        self._fields: list[ConfigField] = []

    def summary(self, text: str) -> "ConfigSpec":
        self._summary = text
        return self

    def description(self, text: str) -> "ConfigSpec":
        self._description = text
        return self

    def version(self, value: str) -> "ConfigSpec":
        self._version = value
        return self

    def categories(self, *cats: str) -> "ConfigSpec":
        self._categories.extend(cats)
        return self

    def field(self, field: ConfigField) -> "ConfigSpec":
        self._fields.append(field)
        return self

    def to_proto(self) -> _pb.ConfigSpec:
        return _pb.ConfigSpec(
            summary=self._summary,
            description=self._description,
            version=self._version,
            categories=list(self._categories),
            fields=[f.to_proto() for f in self._fields],
        )
