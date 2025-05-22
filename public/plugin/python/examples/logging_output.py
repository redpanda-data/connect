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
from collections.abc import AsyncIterator
from typing import cast

from redpanda_connect import Message, Value, output, output_main

logger = logging.getLogger(__name__)


@output(max_in_flight=10)
async def logging_output(config: Value, messages: AsyncIterator[Message]):
    count = cast(dict[str, Value], config).get("repeat")
    async for msg in messages:
        for _ in range(cast(int, count)):
            logger.info(f"Received message: {msg}")
        await asyncio.sleep(0.1)


asyncio.run(output_main(logging_output))
