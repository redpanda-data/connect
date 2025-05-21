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
