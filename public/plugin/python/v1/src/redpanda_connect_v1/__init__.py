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

"""Redpanda Connect v1 plugin SDK for Python (proof of concept).

Plugin authors build an ``Environment`` and register components against
it, then call :func:`serve` to run the gRPC PluginRuntime protocol
against the connect host:

.. code-block:: python

    from redpanda_connect_v1 import (
        ConfigSpec, Environment, MessageBatch, StringField, serve,
    )

    class MyProcessor:
        def __init__(self, suffix: str):
            self.suffix = suffix.encode()

        async def process_batch(self, batch):
            for msg in batch:
                msg.set_bytes(msg.as_bytes().upper() + self.suffix)
            return [batch]

        async def close(self):
            pass

    def ctor(config, _resources):
        return MyProcessor(suffix=config["suffix"])

    env = Environment()
    env.register_batch_processor(
        "shouter",
        ConfigSpec()
            .summary("Uppercase + suffix")
            .field(StringField("suffix").default("")),
        ctor,
    )
    serve(env)
"""

from ._env import BatchProcessor, Environment
from ._message import Message, MessageBatch
from ._server import serve
from ._spec import (
    BoolField,
    ConfigField,
    ConfigSpec,
    FloatField,
    IntField,
    ObjectField,
    StringField,
)

__all__ = [
    "BatchProcessor",
    "BoolField",
    "ConfigField",
    "ConfigSpec",
    "Environment",
    "FloatField",
    "IntField",
    "Message",
    "MessageBatch",
    "ObjectField",
    "StringField",
    "serve",
]
