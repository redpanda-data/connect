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
Error classes for the Redpanda Connect package.
"""

from datetime import timedelta


class BaseError(Exception):
    """Base class for all exceptions raised by this package."""

    message: str

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class BackoffError(BaseError):
    duration: timedelta
    """Raised when a backoff is required."""

    def __init__(self, message: str, duration: timedelta) -> None:
        super().__init__(message)
        self.duration = duration


class NotConnectedError(BaseError):
    """Raised when the client is not connected to the server."""

    def __init__(self) -> None:
        super().__init__("Client is not connected to the server.")


class EndOfInputError(BaseError):
    """Raised when the end of input is reached."""

    def __init__(self) -> None:
        super().__init__("End of input reached.")
