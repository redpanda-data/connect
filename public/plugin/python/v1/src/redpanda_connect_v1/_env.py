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

"""Plugin component registry mirroring rpcn.Environment from the Go SDK."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Protocol, runtime_checkable

from ._message import Message, MessageBatch
from ._spec import ConfigSpec


@runtime_checkable
class BatchProcessor(Protocol):
    """The minimum surface a processor implementation must expose. Mirrors
    benthos's ``service.BatchProcessor`` interface."""

    async def process_batch(self, batch: list[Message]) -> list[MessageBatch]: ...

    async def close(self) -> None: ...


BatchProcessorCtor = Callable[[dict[str, Any], Any], BatchProcessor]


@dataclass(slots=True)
class _RegisteredProcessor:
    name: str
    spec: ConfigSpec
    ctor: BatchProcessorCtor


class Environment:
    """Plugin component registry. Plugins build one of these in
    ``main()``, register components, and pass it to :func:`serve`."""

    __slots__ = ("_procs",)

    def __init__(self) -> None:
        self._procs: list[_RegisteredProcessor] = []

    def register_batch_processor(
        self,
        name: str,
        spec: ConfigSpec,
        ctor: BatchProcessorCtor,
    ) -> None:
        """Register a new batch-processor component.

        Mirrors ``(*service.Environment).RegisterBatchProcessor`` from
        benthos.
        """
        if not name:
            raise ValueError("component name is required")
        if spec is None:
            raise ValueError("component spec is required")
        if ctor is None:
            raise ValueError("component constructor is required")
        for p in self._procs:
            if p.name == name:
                raise ValueError(f"component {name!r} is already registered")
        self._procs.append(_RegisteredProcessor(name=name, spec=spec, ctor=ctor))

    def processors(self) -> list[_RegisteredProcessor]:
        return list(self._procs)
