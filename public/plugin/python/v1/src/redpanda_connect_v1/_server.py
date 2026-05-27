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

"""Async gRPC server implementing the PluginRuntime protocol."""

from __future__ import annotations

import asyncio
import itertools
import logging
import os
import signal
import sys
from typing import Any

import grpc  # type: ignore[import-untyped]
import grpc.aio  # type: ignore[import-untyped]

from ._env import BatchProcessor, Environment
from ._proto.redpanda.runtime.v2 import (
    plugin_runtime_pb2 as _pb,
    plugin_runtime_pb2_grpc as _grpc,
)
from ._value import (
    batch_to_proto,
    error_to_proto,
    proto_to_batch,
    proto_to_value,
)

_logger = logging.getLogger(__name__)

_HOST_PROTOCOL_VERSION = 1
_SDK_NAME = "rpcn-python"
_SDK_VERSION = "v1.0.0-poc"
_SDK_LANGUAGE = "python"


class _PluginRuntimeService(_grpc.PluginRuntimeServicer):
    """gRPC service implementation routing calls from the host to the
    plugin author's registered components."""

    def __init__(self, env: Environment) -> None:
        super().__init__()
        self._env = env
        self._instances: dict[int, _Instance] = {}
        self._id_seq = itertools.count(1)
        self._lock = asyncio.Lock()

    async def Handshake(
        self,
        request: _pb.HandshakeRequest,
        context: grpc.aio.ServicerContext,
    ) -> _pb.HandshakeResponse:
        if _HOST_PROTOCOL_VERSION not in request.supported_protocol_versions:
            return _pb.HandshakeResponse(
                error=error_to_proto(
                    RuntimeError(
                        f"no protocol-version overlap: host={list(request.supported_protocol_versions)} sdk=[{_HOST_PROTOCOL_VERSION}]"
                    )
                )
            )
        return _pb.HandshakeResponse(
            selected_protocol_version=_HOST_PROTOCOL_VERSION,
            sdk_name=_SDK_NAME,
            sdk_version=_SDK_VERSION,
            language=_SDK_LANGUAGE,
        )

    async def ListComponents(
        self,
        request: _pb.ListComponentsRequest,
        context: grpc.aio.ServicerContext,
    ) -> _pb.ListComponentsResponse:
        components = [
            _pb.ComponentDescriptor(
                name=p.name,
                kind=_pb.COMPONENT_KIND_PROCESSOR,
                spec=p.spec.to_proto(),
            )
            for p in self._env.processors()
        ]
        return _pb.ListComponentsResponse(components=components)

    async def OpenInstance(
        self,
        request: _pb.OpenInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> _pb.OpenInstanceResponse:
        ctor = None
        for p in self._env.processors():
            if p.name == request.component_name:
                ctor = p.ctor
                break
        if ctor is None:
            return _pb.OpenInstanceResponse(
                error=error_to_proto(ValueError(f"unknown component {request.component_name!r}"))
            )

        try:
            parsed = proto_to_value(request.config)
            if parsed is None:
                parsed = {}
            if not isinstance(parsed, dict):
                raise TypeError(f"plugin config must be a mapping (got {type(parsed).__name__})")
            component = ctor(parsed, None)
        except Exception as exc:  # noqa: BLE001
            return _pb.OpenInstanceResponse(error=error_to_proto(exc))

        async with self._lock:
            instance_id = next(self._id_seq)
            self._instances[instance_id] = _Instance(
                component_name=request.component_name,
                processor=component,
            )
        return _pb.OpenInstanceResponse(instance_id=instance_id)

    async def CloseInstance(
        self,
        request: _pb.CloseInstanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> _pb.CloseInstanceResponse:
        async with self._lock:
            inst = self._instances.pop(request.instance_id, None)
        if inst is None:
            return _pb.CloseInstanceResponse(
                error=error_to_proto(ValueError(f"unknown instance_id {request.instance_id}"))
            )
        try:
            await inst.processor.close()
        except Exception as exc:  # noqa: BLE001
            return _pb.CloseInstanceResponse(error=error_to_proto(exc))
        return _pb.CloseInstanceResponse()

    async def ProcessBatch(
        self,
        request: _pb.InstanceProcessRequest,
        context: grpc.aio.ServicerContext,
    ) -> _pb.InstanceProcessResponse:
        async with self._lock:
            inst = self._instances.get(request.instance_id)
        if inst is None:
            return _pb.InstanceProcessResponse(
                error=error_to_proto(ValueError(f"unknown processor instance_id {request.instance_id}"))
            )

        try:
            batch = proto_to_batch(request.batch)
            out_batches = await inst.processor.process_batch(batch)
        except Exception as exc:  # noqa: BLE001
            return _pb.InstanceProcessResponse(error=error_to_proto(exc))

        return _pb.InstanceProcessResponse(
            batches=[batch_to_proto(b) for b in out_batches],
        )


class _Instance:
    __slots__ = ("component_name", "processor")

    def __init__(self, component_name: str, processor: BatchProcessor) -> None:
        self.component_name = component_name
        self.processor = processor


def _parse_listen_address(addr: str) -> str:
    """Return the grpc.aio.add_insecure_port-style address string."""
    if addr.startswith("unix://"):
        return addr
    if addr.startswith("unix:"):
        return "unix:" + addr[len("unix:") :]
    if addr.startswith("tcp://"):
        return addr[len("tcp://") :]
    raise ValueError(f"unknown REDPANDA_CONNECT_PLUGIN_ADDRESS scheme: {addr!r}")


async def _serve_async(env: Environment) -> None:
    addr = os.environ.get("REDPANDA_CONNECT_PLUGIN_ADDRESS")
    if not addr:
        raise RuntimeError("REDPANDA_CONNECT_PLUGIN_ADDRESS not set")

    server = grpc.aio.server()
    service = _PluginRuntimeService(env)
    _grpc.add_PluginRuntimeServicer_to_server(service, server)
    server.add_insecure_port(_parse_listen_address(addr))
    await server.start()
    _logger.info("PluginRuntime listening on %s", addr)

    stop = asyncio.Event()

    def _on_signal(_sig: int) -> None:
        stop.set()

    try:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _on_signal, sig)
    except NotImplementedError:
        # Signal handling unavailable (e.g. on Windows). Fall back to
        # blocking until the gRPC server itself terminates.
        pass

    waiter = asyncio.create_task(server.wait_for_termination())
    stopper = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {waiter, stopper}, return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        if not waiter.done():
            await server.stop(grace=5)
            try:
                await waiter
            except asyncio.CancelledError:
                pass
        stopper.cancel()


def serve(env: Environment, *, log_level: int = logging.INFO) -> None:
    """Block the current thread serving the PluginRuntime protocol
    against the host gRPC client. Mirrors ``rpcn.Serve`` from Go."""
    logging.basicConfig(stream=sys.stderr, level=log_level, format="[plugin] %(message)s")
    try:
        asyncio.run(_serve_async(env))
    except KeyboardInterrupt:
        pass
    except Exception as exc:  # noqa: BLE001
        _logger.exception("serving plugin runtime: %s", exc)
        raise


# Suppress an unused-import lint without changing the public API.
_ = Any
