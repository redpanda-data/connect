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
from redpanda_connect import (Message, processor, processor_main)

@processor
def fizzbuzz_processor(msg: Message) -> Message:
    v = msg.payload
    if not isinstance(v, int):
        raise ValueError("Payload must be an integer")
    if v % 3 == 0 and v % 5 == 0:
        msg.payload = "fizzbuzz"
    elif v % 3 == 0:
        msg.payload = "fizz"
    elif v % 5 == 0:
        msg.payload = "buzz"
    else:
        msg.payload = v
    return msg


asyncio.run(processor_main(fizzbuzz_processor))
