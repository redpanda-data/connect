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
from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Protocol, TypeAlias, override

from .errors import BaseError, EndOfInputError, NotConnectedError

Value: TypeAlias = str | bytes | int | float | datetime | dict[str, "Value"] | list["Value"] | None
"""
A value are the types that are supported within Redpanda Connect (and Bloblang).
"""


@dataclass
class Message:
    """
    A message is a core abstraction around a value within Redpanda Connect.
    """

    payload: bytes | Value
    """
    The payload of the message. This can be a bytes object or a Value object.
    """
    metadata: dict[str, Value] = field(default_factory=lambda: ({}))
    """
    Metadata is a dictionary of key-value pairs that can be used to store
    additional information outside of the payload.
    """
    error: BaseError | None = None
    """
    An error bit set on the message. This is used to indicate that the message has
    hit an error while being processed.
    """


MessageBatch: TypeAlias = list[Message]
"""
A MessageBatch is a list of messages. Redpanda Connect pipelines generally work
on batches of messages being passed around.
"""

AckFn: TypeAlias = Callable[[BaseError | None], Awaitable[None]]
"""
An ack function is a function that is called when a message has been processed
by the output. The input maybe an error (which means the message was nack'd),
or it might be None indicating that the message was successfully sent to the output.
"""


class Input(Protocol):
    """
    An input is a source component that can generate batches of messages, which are
    then passed to the processor and output components.
    """

    async def connect(self) -> None:
        """
        Connect to the input source. This is called before any messages are read>
        """
        ...

    async def read_batch(self) -> tuple[MessageBatch, AckFn]:
        """
        Read a batch of messages from the input source, returning the batch of messages
        read along with a function that can be used to acknowledge (negatively or positively)
        the messages once they have been sent to the output.

        Any checkpointing should not be done until the ack function is called, in order to
        preserve at least once semantics.
        """
        ...

    async def close(self) -> None:
        """
        Close the input source and frees up any resources.
        """
        ...


AutoRetryNacks: TypeAlias = bool
"""
AutoRetryNacks is a boolean indicating whether the input should automatically
nack'd messages. This is useful for inputs that are not able to upstream nacks
which is generally the case unless you're building an input for a queuing system.
"""

InputConstructor: TypeAlias = Callable[[Value], tuple[Input, AutoRetryNacks]]
"""
An input constructor recieves the configuration specified in the configuration,
file, then returns the input and a boolean indicating whether the input should automatically
nack'd messages or not.
"""


def batch_input(func: Callable[[Value], AsyncIterator[MessageBatch]]) -> InputConstructor:
    """
    A decorator that wraps a generator of message batches.

    Note that this helper has limited error handling and no ability to checkpoint acknowledged
    batches. However, this decorator is still useful for one-shot sources that don't require
    checkpointing.

    Example:

        @batch_input
        async def my_input(_config: Value):
            for _ in range(10):
                yield [Message(b"hello"), Message(b"world")]
    """

    def ctor(config: Value) -> tuple[Input, AutoRetryNacks]:
        class FuncInput(Input):
            iter: AsyncIterator[MessageBatch] | None = None

            @override
            async def connect(self) -> None:
                self.iter = func(config)

            @override
            async def read_batch(self) -> tuple[MessageBatch, AckFn]:
                if self.iter is None:
                    raise NotConnectedError()
                try:
                    batch = await self.iter.__anext__()

                    async def ack_fn(_: BaseError | None):
                        pass

                    return batch, ack_fn
                except StopAsyncIteration:
                    raise EndOfInputError() from None

            @override
            async def close(self) -> None:
                self.iter = None

        return FuncInput(), True

    return ctor


def input(func: Callable[[Value], AsyncIterator[Message]]) -> InputConstructor:
    """
    A decorator that wraps a generator of messages.

    Note that this helper has limited error handling and no ability to checkpoint acknowledged
    messages. However, this decorator is still useful for one-shot sources that don't require
    checkpointing.

    Example:

        @input
        async def my_input():
            for _ in range(10):
                yield Message(b"hello")
    """

    async def wrapped(config: Value) -> AsyncIterator[MessageBatch]:
        iter = func(config)
        async for msg in iter:
            yield [msg]

    return batch_input(wrapped)


class Processor(Protocol):
    async def process(self, batch: MessageBatch) -> list[MessageBatch]:
        """
        Process a batch of messages into one or more resulting batches, or return
        an error if the entire batch could not be processed. If zero messages are
        returned and the error is nil then all messages are filtered.
        """
        ...

    async def close(self) -> None:
        """
        Close the processor and frees up any resources.
        """
        ...


ProcessorConstructor: TypeAlias = Callable[[Value], Processor]
"""
A processor constructor recieves the configuration specified in the configuration,
then returns a properly configured processor component.
"""


def batch_processor(func: Callable[[MessageBatch], list[MessageBatch]]) -> ProcessorConstructor:
    """
    A decorator that wraps a function that processes a single message and returns it to continue down the pipeline.
    """

    def ctor(_: Value) -> Processor:
        class FuncProcessor(Processor):
            @override
            async def process(self, batch: MessageBatch) -> list[MessageBatch]:
                return func(batch)

            @override
            async def close(self) -> None:
                pass

        return FuncProcessor()

    return ctor


def processor(func: Callable[[Message], Message]) -> ProcessorConstructor:
    """ 
    A decorator that wraps a function that processes a single message and returns it to continue down the pipeline.
    """

    def wrapped(batch: MessageBatch) -> list[MessageBatch]:
        return [[func(msg) for msg in batch]]

    return batch_processor(wrapped)


class Output(Protocol):
    """
    An output is a sink component that can receive batches of messages and send them somewhere.
    """

    async def connect(self) -> None:
        """
        Connect to the output sink. This is called before any messages are written.
        """
        ...

    async def write_batch(self, batch: MessageBatch) -> None:
        """
        Write a batch of messages to the output sink.
        """
        ...

    async def close(self) -> None:
        """
        Close the output sink and frees up any resources.
        """
        ...


@dataclass
class BatchPolicy:
    """
    A policy that defines how to batch messages before sending them to the output.
    """

    byte_size: int = 0
    """
    The size in bytes of messages to collect before flushing to the output.
    """
    count: int = 0
    """
    The number of messages to collect before flushing to the output.
    """
    period: timedelta = timedelta()
    """
    The time to wait before flushing to the output.
    """
    check: str = ""
    """
    A bloblang check to perform on each message. If it returns true, then the batch is flushed.
    """


OutputConstructor: TypeAlias = Callable[[Value], tuple[Output, int, BatchPolicy]]
"""
A constructor for an output. It should take the configuration and return a tuple of the output,
the maximum number of messages that can be in flight at once, and the batching policy to use.
"""


class BatchingOutputFunc(Protocol):
    """
    A function that takes a batch of messages and returns a list of batches.
    """

    async def __call__(self, config: Value, batches: AsyncIterator[MessageBatch]) -> None:
        """
        Called once when the output is connected, it should read from batches in a loop.
        """
        ...




def batch_output(
    max_in_flight: int = 1, batch_policy: BatchPolicy | None = None
) -> Callable[[BatchingOutputFunc], OutputConstructor]:
    """ 
    A decorator that wraps an output function that takes the configuration and stream of batches.
    """

    def wrapped(func: BatchingOutputFunc) -> OutputConstructor:
        def ctor(config: Value) -> tuple[Output, int, BatchPolicy]:
            queue = asyncio.Queue[tuple[MessageBatch, asyncio.Future[None]]](maxsize=max_in_flight)

            async def consumer() -> AsyncIterator[MessageBatch]:
                while True:
                    batch, fut = await queue.get()
                    yield batch
                    fut.set_result(None)
            async def noop() -> None:
                return
            class FuncOutput(Output):
                task: asyncio.Task[None] = asyncio.create_task(noop())

                @override
                async def connect(self) -> None:
                    self.task.cancel()
                    await self.task
                    self.task = asyncio.create_task(func(config, consumer()))

                @override
                async def write_batch(self, batch: MessageBatch) -> None:
                    fut = asyncio.Future[None]()
                    await queue.put((batch, fut))
                    done, _ = await asyncio.wait(
                        (fut, self.task), return_when=asyncio.FIRST_COMPLETED
                    )
                    for f in done:
                        err = f.exception()
                        if err is not None:
                            raise err

                @override
                async def close(self) -> None:
                    self.task.cancel()
                    await self.task

            return FuncOutput(), max_in_flight, batch_policy or BatchPolicy()

        return ctor

    return wrapped


class OutputFunc(Protocol):
    """
    An output function that recieves the configuration and a stream of messages that can be sent.
    """
    async def __call__(self, config: Value, messages: AsyncIterator[Message]) -> None: ...
    """
    Called once when the output is connected, it should read from messages in a loop.
    """

def output(max_in_flight: int = 1) -> Callable[[OutputFunc], OutputConstructor]:
    """
    A decorator that wraps an output function that takes the configuration and stream of messages.

    Args:
        max_in_flight: The maximum number of messages that can be in flight at once.
    """
    batching_output = batch_output(max_in_flight)

    def wrapped(func: OutputFunc) -> OutputConstructor:
        async def inner_wrapped(config: Value, batches: AsyncIterator[MessageBatch]) -> None:
            async def split_batches() -> AsyncIterator[Message]:
                async for batch in batches:
                    for msg in batch:
                        yield msg

            await func(config, split_batches())

        return batching_output(inner_wrapped)

    return wrapped
