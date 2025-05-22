from collections.abc import AsyncIterator, Awaitable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, TypeAlias, override

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


class Input:
    """
    An input is a source component that can generate batches of messages, which are
    then passed to the processor and output components.
    """

    async def connect(self) -> None:
        """
        Connect to the input source. This is called before any messages are read>
        """
        raise NotImplementedError("connect not implemented")

    async def read_batch(self) -> tuple[MessageBatch, AckFn]:
        """
        Read a batch of messages from the input source, returning the batch of messages
        read along with a function that can be used to acknowledge (negatively or positively)
        the messages once they have been sent to the output.

        Any checkpointing should not be done until the ack function is called, in order to
        preserve at least once semantics.
        """
        raise NotImplementedError("read_batch not implemented")

    async def close(self) -> None:
        """
        Close the input source and frees up any resources.
        """
        raise NotImplementedError("close not implemented")


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


class Processor:
    async def process(self, batch: MessageBatch) -> list[MessageBatch]:
        """
        Process a batch of messages into one or more resulting batches, or return
        an error if the entire batch could not be processed. If zero messages are
        returned and the error is nil then all messages are filtered.
        """
        _ = batch
        raise NotImplementedError("process not implemented")

    async def close(self) -> None:
        """
        Close the processor and frees up any resources.
        """
        raise NotImplementedError("close not implemented")


ProcessorConstructor: TypeAlias = Callable[[Value], Processor]
"""
A processor constructor recieves the configuration specified in the configuration,
then returns a properly configured processor component.
"""


def batch_processor(func: Callable[[MessageBatch], list[MessageBatch]]) -> ProcessorConstructor:
    """ """

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
    """ """

    def wrapped(batch: MessageBatch) -> list[MessageBatch]:
        return [[func(msg) for msg in batch]]

    return batch_processor(wrapped)


class Output:
    """
    An output is a sink component that can receive batches of messages and send them somewhere.
    """

    async def connect(self) -> None:
        """
        Connect to the output sink. This is called before any messages are written.
        """
        raise NotImplementedError("connect not implemented")

    async def write_batch(self, batch: MessageBatch) -> None:
        """
        Write a batch of messages to the output sink.
        """
        _ = batch
        raise NotImplementedError("read_batch not implemented")

    async def close(self) -> None:
        """
        Close the output sink and frees up any resources.
        """
        raise NotImplementedError("close not implemented")


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

"""


def batch_output(
    max_in_flight: int, batch_policy: BatchPolicy | None = None
) -> Callable[[Callable[[MessageBatch], None]], OutputConstructor]:
    """ """

    def wrapped(func: Callable[[MessageBatch], None]) -> OutputConstructor:
        def ctor(_: Value) -> tuple[Output, int, BatchPolicy]:
            class FuncOutput(Output):
                @override
                async def write_batch(self, batch: MessageBatch) -> None:
                    return func(batch)

                @override
                async def close(self) -> None:
                    pass

            return FuncOutput(), max_in_flight, batch_policy or BatchPolicy()

        return ctor

    return wrapped


def output(max_in_flight: int) -> Callable[[Callable[[Message], None]], OutputConstructor]:
    """ """
    batching_output = batch_output(max_in_flight)

    def wrapped(func: Callable[[Message], None]) -> OutputConstructor:
        def inner_wrapped(batch: MessageBatch) -> None:
            for msg in batch:
                func(msg)

        return batching_output(inner_wrapped)

    return wrapped
