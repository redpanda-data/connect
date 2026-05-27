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

"""pycatshout is a v1-protocol PoC plugin binary written in Python that
registers two processors in a single subprocess:

* ``pycatshout`` — appends a configurable suffix and uppercases each
  message in the batch.
* ``pyreverser`` — reverses the bytes of each message in the batch.

Mirror of the Go ``catshout`` PoC plugin at
``internal/rpcplugin/v1/testdata/catshout``.
"""

from __future__ import annotations

from typing import Any

from redpanda_connect_v1 import (
    ConfigSpec,
    Environment,
    MessageBatch,
    StringField,
    serve,
)


class PyCatshoutProcessor:
    """Uppercase and append a suffix."""

    def __init__(self, suffix: str) -> None:
        self._suffix = suffix.encode("utf-8")

    async def process_batch(self, batch: MessageBatch) -> list[MessageBatch]:
        for msg in batch:
            data = msg.as_bytes()
            msg.set_bytes(b"MEOW! " + data.upper() + self._suffix)
        return [batch]

    async def close(self) -> None:  # noqa: D401
        pass


class PyReverserProcessor:
    """Reverse the bytes of each message."""

    async def process_batch(self, batch: MessageBatch) -> list[MessageBatch]:
        for msg in batch:
            data = msg.as_bytes()
            msg.set_bytes(data[::-1])
        return [batch]

    async def close(self) -> None:  # noqa: D401
        pass


def _new_catshout(config: dict[str, Any], _resources: Any) -> PyCatshoutProcessor:
    return PyCatshoutProcessor(suffix=config.get("suffix", ""))


def _new_reverser(_config: dict[str, Any], _resources: Any) -> PyReverserProcessor:
    return PyReverserProcessor()


def main() -> None:
    env = Environment()
    env.register_batch_processor(
        "pycatshout",
        ConfigSpec()
            .summary("Uppercases each message and appends a configurable suffix.")
            .field(
                StringField("suffix")
                .description("Bytes appended to every message after uppercasing.")
                .default("")
            ),
        _new_catshout,
    )
    env.register_batch_processor(
        "pyreverser",
        ConfigSpec().summary("Reverses the bytes of each message in the batch."),
        _new_reverser,
    )
    serve(env)


if __name__ == "__main__":
    main()
