"""
A Python package for writing Redpanda Connect components (inputs, processors and outputs).
"""

from ._grpc import input_main, output_main, processor_main
from .core import (
    Message,
    MessageBatch,
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
    "input",
    "processor",
    "output",
    "BaseError",
    "BackoffError",
    "NotConnectedError",
    "EndOfInputError",
]
