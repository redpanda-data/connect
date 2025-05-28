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

"""
A Python package for writing Redpanda Connect components (inputs, processors and outputs).
"""

from ._grpc import input_main, output_main, processor_main
from .core import (
    Message,
    MessageBatch,
    Value,
    batch_input,
    batch_processor,
    input,
    output,
    processor,
)
from .errors import (
    BackoffError,
    BaseError,
    EndOfInputError,
    NotConnectedError,
)

__all__ = [
    "input_main",
    "output_main",
    "processor_main",
    "Message",
    "MessageBatch",
    "batch_input",
    "batch_processor",
    "Value",
    "input",
    "processor",
    "output",
    "BaseError",
    "BackoffError",
    "NotConnectedError",
    "EndOfInputError",
]
