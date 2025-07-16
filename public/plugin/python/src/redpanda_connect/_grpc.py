# Copyright 2025 Redpanda Data, Inc.
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

import asyncio
import logging
import os
import signal
import sys
from datetime import timedelta
from typing import Callable, final, override

import grpc  # pyright: ignore[reportMissingTypeStubs]
import grpc.aio  # pyright: ignore[reportMissingTypeStubs]

from ._convert import batch_to_proto, error_to_proto, proto_to_batch, proto_to_error, proto_to_value
from ._proto.redpanda.runtime.v1alpha1 import (
    input_pb2,
    input_pb2_grpc,
    output_pb2,
    output_pb2_grpc,
    processor_pb2,
    processor_pb2_grpc,
)
from .core import (
    AckFn,
    Input,
    InputConstructor,
    Output,
    OutputConstructor,
    Processor,
    ProcessorConstructor,
)
from .errors import BaseError

_logger = logging.getLogger(__name__)


def _id_generator():
    id = 1
    while True:
        yield id
        id += 1


@final
class _InputService(input_pb2_grpc.BatchInputServiceServicer):
    ctor: InputConstructor
    input: Input | None = None
    acks: dict[int, AckFn] = {}
    id_gen = _id_generator()
    close_event: asyncio.Event

    def __init__(self, ctor: InputConstructor, close_event: asyncio.Event):
        super().__init__()
        self.ctor = ctor
        self.close_event = close_event

    @override
    async def Init(
        self,
        request: input_pb2.BatchInputInitRequest,
        context: grpc.aio.ServicerContext[
            input_pb2.BatchInputInitRequest, input_pb2.BatchInputInitResponse
        ],
    ) -> input_pb2.BatchInputInitResponse:
        resp = input_pb2.BatchInputInitResponse()
        try:
            self.input, resp.auto_replay_nacks = self.ctor(proto_to_value(request.config))
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to initialize input: {e}")))
        return resp

    @override
    async def Connect(
        self,
        request: input_pb2.BatchInputConnectRequest,
        context: grpc.aio.ServicerContext[
            input_pb2.BatchInputConnectRequest, input_pb2.BatchInputConnectResponse
        ],
    ) -> input_pb2.BatchInputConnectResponse:
        resp = input_pb2.BatchInputConnectResponse()
        if self.input is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Input not initialized")))
            return resp
        try:
            await self.input.connect()
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to connect input: {e}")))
        return resp

    @override
    async def ReadBatch(
        self,
        request: input_pb2.BatchInputReadRequest,
        context: grpc.aio.ServicerContext[
            input_pb2.BatchInputReadRequest, input_pb2.BatchInputReadResponse
        ],
    ) -> input_pb2.BatchInputReadResponse:
        resp = input_pb2.BatchInputReadResponse()
        if self.input is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Input not initialized")))
            return resp
        try:
            batch, ack = await self.input.read_batch()
            id = self.id_gen.__next__()
            self.acks[id] = ack
            resp.batch_id = id
            resp.batch.CopyFrom(batch_to_proto(batch))
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to connect input: {e}")))
        return resp

    @override
    async def Ack(
        self,
        request: input_pb2.BatchInputAckRequest,
        context: grpc.aio.ServicerContext[
            input_pb2.BatchInputAckRequest, input_pb2.BatchInputAckResponse
        ],
    ) -> input_pb2.BatchInputAckResponse:
        resp = input_pb2.BatchInputAckResponse()
        ack_fn = self.acks.pop(request.batch_id, None)
        if not ack_fn:
            return input_pb2.BatchInputAckResponse()
        try:
            await ack_fn(proto_to_error(request.error))
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to ack input: {e}")))
        return resp

    @override
    async def Close(
        self,
        request: input_pb2.BatchInputCloseRequest,
        context: grpc.aio.ServicerContext[
            input_pb2.BatchInputCloseRequest, input_pb2.BatchInputCloseResponse
        ],
    ) -> input_pb2.BatchInputCloseResponse:
        self.close_event.set()
        resp = input_pb2.BatchInputCloseResponse()
        if self.input is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Input not initialized")))
            return resp
        try:
            await self.input.close()
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to connect input: {e}")))
        return resp


@final
class _ProcessorService(processor_pb2_grpc.BatchProcessorServiceServicer):
    ctor: ProcessorConstructor
    component: Processor | None = None
    close_event: asyncio.Event

    def __init__(self, ctor: ProcessorConstructor, close_event: asyncio.Event):
        super().__init__()
        self.ctor = ctor
        self.close_event = close_event

    @override
    async def Init(
        self,
        request: processor_pb2.BatchProcessorInitRequest,
        context: grpc.aio.ServicerContext[
            processor_pb2.BatchProcessorInitRequest, processor_pb2.BatchProcessorInitResponse
        ],
    ) -> processor_pb2.BatchProcessorInitResponse:
        resp = processor_pb2.BatchProcessorInitResponse()
        try:
            self.component = self.ctor(proto_to_value(request.config))
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to initialize output: {e}")))
        return resp

    @override
    async def ProcessBatch(
        self,
        request: processor_pb2.BatchProcessorProcessBatchRequest,
        context: grpc.aio.ServicerContext[
            processor_pb2.BatchProcessorProcessBatchRequest,
            processor_pb2.BatchProcessorProcessBatchResponse,
        ],
    ) -> processor_pb2.BatchProcessorProcessBatchResponse:
        resp = processor_pb2.BatchProcessorProcessBatchResponse()
        if self.component is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Processor not initialized")))
            return resp
        try:
            batches = await self.component.process(proto_to_batch(request.batch))
            for batch in batches:
                resp.batches.append(batch_to_proto(batch))
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to initialize output: {e}")))
        return resp

    @override
    async def Close(
        self,
        request: processor_pb2.BatchProcessorCloseRequest,
        context: grpc.aio.ServicerContext[
            processor_pb2.BatchProcessorCloseRequest, processor_pb2.BatchProcessorCloseResponse
        ],
    ) -> processor_pb2.BatchProcessorCloseResponse:
        self.close_event.set()
        resp = processor_pb2.BatchProcessorCloseResponse()
        if self.component is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Processor not initialized")))
            return resp
        try:
            await self.component.close()
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to initialize output: {e}")))
        return resp


@final
class _OutputService(output_pb2_grpc.BatchOutputServiceServicer):
    ctor: OutputConstructor
    component: Output | None = None
    close_event: asyncio.Event

    def __init__(self, ctor: OutputConstructor, close_event: asyncio.Event):
        super().__init__()
        self.ctor = ctor
        self.close_event = close_event

    @override
    async def Init(
        self,
        request: output_pb2.BatchOutputInitRequest,
        context: grpc.aio.ServicerContext[
            output_pb2.BatchOutputInitRequest, output_pb2.BatchOutputInitResponse
        ],
    ) -> output_pb2.BatchOutputInitResponse:
        resp = output_pb2.BatchOutputInitResponse()
        try:
            self.component, resp.max_in_flight, batch_policy = self.ctor(
                proto_to_value(request.config)
            )
            resp.batch_policy.byte_size = batch_policy.byte_size
            resp.batch_policy.count = batch_policy.count
            period = batch_policy.period
            if period != timedelta():
                # The string format is parsed by time.ParseDuration in golang.
                resp.batch_policy.period = (
                    f"{period.days * 24}h{period.seconds}s{period.microseconds}us"
                )
            resp.batch_policy.check = batch_policy.check
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to initialize output: {e}")))
        return resp

    @override
    async def Connect(
        self,
        request: output_pb2.BatchOutputConnectRequest,
        context: grpc.aio.ServicerContext[
            output_pb2.BatchOutputConnectRequest, output_pb2.BatchOutputConnectResponse
        ],
    ) -> output_pb2.BatchOutputConnectResponse:
        resp = output_pb2.BatchOutputConnectResponse()
        if self.component is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Output not initialized")))
            return resp
        try:
            await self.component.connect()
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to connect output: {e}")))
        return resp

    @override
    async def Send(
        self,
        request: output_pb2.BatchOutputSendRequest,
        context: grpc.aio.ServicerContext[
            output_pb2.BatchOutputSendRequest, output_pb2.BatchOutputSendResponse
        ],
    ) -> output_pb2.BatchOutputSendResponse:
        resp = output_pb2.BatchOutputSendResponse()
        if self.component is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Output not initialized")))
            return resp
        try:
            await self.component.write_batch(proto_to_batch(request.batch))
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to send to output: {e}")))
        return resp

    @override
    async def Close(
        self,
        request: output_pb2.BatchOutputCloseRequest,
        context: grpc.aio.ServicerContext[
            output_pb2.BatchOutputCloseRequest, output_pb2.BatchOutputCloseResponse
        ],
    ) -> output_pb2.BatchOutputCloseResponse:
        self.close_event.set()
        resp = output_pb2.BatchOutputCloseResponse()
        if self.component is None:
            resp.error.CopyFrom(error_to_proto(BaseError("Output not initialized")))
            return resp
        try:
            await self.component.close()
        except BaseError as e:
            resp.error.CopyFrom(error_to_proto(e))
        except Exception as e:
            resp.error.CopyFrom(error_to_proto(BaseError(f"Failed to close output: {e}")))
        return resp


async def _serve_component(register: Callable[[grpc.aio.Server, asyncio.Event], None]):
    version = os.environ.get("REDPANDA_CONNECT_PLUGIN_VERSION", "1")
    if version != "1":
        _logger.fatal(f"Unsupported plugin version: {version}")
        sys.exit(1)
    addr = os.environ.get("REDPANDA_CONNECT_PLUGIN_ADDRESS", None)
    if not addr:
        _logger.fatal("REDPANDA_CONNECT_PLUGIN_ADDRESS not set")
        sys.exit(1)
    print("Successfully loaded Redpanda Connect RPC plugin")
    server = grpc.aio.server()
    closed_event = asyncio.Event()
    register(server, closed_event)
    _ = server.add_insecure_port(addr)
    await server.start()

    async def stop(sig: int):
        if sig == signal.SIGTERM:
            _logger.warning("Recieved SIGTERM stopping server immediately")
            await server.stop(grace=None)
        else:
            _logger.info(f"Recieved {signal.strsignal(sig)} waiting for server close")
            await closed_event.wait()
            await server.stop(grace=30)
        loop.remove_signal_handler(sig)

    try:
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda sig: asyncio.create_task(stop(sig)), sig)
        await server.wait_for_termination()
    finally:
        await server.stop(grace=None)


async def input_main(ctor: InputConstructor):
    """
    input_main is the entry point for the input plugin. It should be called in __main__
    and will block until plugin shutdown.
    """
    logging.basicConfig(encoding="utf-8", level=logging.DEBUG)

    def register(server: grpc.aio.Server, close_event: asyncio.Event):
        input_service = _InputService(ctor, close_event)
        input_pb2_grpc.add_BatchInputServiceServicer_to_server(input_service, server)

    await _serve_component(register)


async def processor_main(ctor: ProcessorConstructor):
    """
    processor_main is the entry point for the processor plugin. It should be called in __main__
    and will block until plugin shutdown.
    """
    logging.basicConfig(encoding="utf-8", level=logging.DEBUG)

    def register(server: grpc.aio.Server, close_event: asyncio.Event):
        processor_service = _ProcessorService(ctor, close_event)
        processor_pb2_grpc.add_BatchProcessorServiceServicer_to_server(processor_service, server)

    await _serve_component(register)


async def output_main(ctor: OutputConstructor):
    """
    output_main is the entry point for the output plugin. It should be called in __main__
    and will block until plugin shutdown.
    """
    logging.basicConfig(encoding="utf-8", level=logging.DEBUG)

    def register(server: grpc.aio.Server, close_event: asyncio.Event):
        output_service = _OutputService(ctor, close_event)
        output_pb2_grpc.add_BatchOutputServiceServicer_to_server(output_service, server)

    await _serve_component(register)
