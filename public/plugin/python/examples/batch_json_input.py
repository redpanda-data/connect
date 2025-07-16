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
from typing import cast, final, override

from redpanda_connect import Message, MessageBatch, Value, input_main
from redpanda_connect.core import AckFn, Input
from redpanda_connect.errors import BaseError, EndOfInputError


@final
class JsonInput(Input):
    """
    An example of using the core APIs to implement an input that has full control
    of its lifecycle and can read messages in batches.
    """

    _count: int
    _counter = 0

    def __init__(self, count: int):
        super().__init__()
        self._count = count
        logging.info(f"json input created with count: {self._count}")

    @override
    async def connect(self) -> None:
        """
        Connect to the input source. This is called before any messages are read
        """
        logging.info("python input connected")

    @override
    async def read_batch(self) -> tuple[MessageBatch, AckFn]:
        """
        Read a batch of messages from the input source, returning the batch of messages
        read along with a function that can be used to acknowledge (negatively or positively)
        the messages once they have been sent to the output.

        Any checkpointing should not be done until the ack function is called, in order to
        preserve at least once semantics.
        """
        if self._counter >= self._count:
            raise EndOfInputError()
        await asyncio.sleep(1)  # Simulate a delay in reading messages
        self._counter += 1
        my_count = self._counter

        async def ack_fn(err: BaseError | None):
            logging.info(f"acking batch {my_count}, err: {err}")

        return [Message(my_count)], ack_fn

    @override
    async def close(self) -> None:
        """
        Close the input source and frees up any resources.
        """
        logging.info("python input closed")


def json_generator(config: Value):
    count = cast(dict[str, Value], config).get("count", 10)
    auto_retry_nacks = True
    return JsonInput(cast(int, count)), auto_retry_nacks


asyncio.run(input_main(json_generator))
