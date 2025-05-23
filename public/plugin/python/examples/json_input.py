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
from collections.abc import AsyncIterator
from typing import cast

from redpanda_connect import Message, Value, input, input_main


@input
async def json_generator(config: Value) -> AsyncIterator[Message]:
    count = cast(dict[str, Value], config).get("count", 10)
    for i in range(cast(int, count)):
        yield Message(payload={"number": i, "message": f"Message {i}"})
        await asyncio.sleep(1)  # Simulate some delay


asyncio.run(input_main(json_generator))
