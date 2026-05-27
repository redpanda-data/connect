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

"""Lightweight Message + MessageBatch wrappers that match the shape of
the benthos service.Message / service.MessageBatch types from a plugin
author's point of view."""

from __future__ import annotations

from typing import Any


class Message:
    """A single message flowing through a plugin component.

    The payload is either ``bytes`` or a structured Python value (dict,
    list, scalar). Metadata is a free-form dict. ``error`` is set by
    upstream components when a per-message failure should be propagated
    without aborting the whole batch.
    """

    __slots__ = ("_payload", "_metadata", "_error")

    def __init__(
        self,
        payload: bytes | str | dict[str, Any] | list[Any] | int | float | bool | None = None,
        metadata: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        if isinstance(payload, str):
            self._payload: Any = payload.encode("utf-8")
        else:
            self._payload = payload
        self._metadata: dict[str, Any] = dict(metadata) if metadata else {}
        self._error = error

    # ------------------------------------------------------------------
    # payload accessors mirroring (*service.Message).AsBytes / .SetBytes
    # ------------------------------------------------------------------

    def as_bytes(self) -> bytes:
        """Return the payload as bytes. Raises ``TypeError`` if the
        payload is structured."""
        if isinstance(self._payload, bytes):
            return self._payload
        raise TypeError(f"payload is not bytes (got {type(self._payload).__name__})")

    def set_bytes(self, data: bytes | str) -> None:
        """Replace the payload with new bytes."""
        if isinstance(data, str):
            self._payload = data.encode("utf-8")
        else:
            self._payload = data

    def as_structured(self) -> Any:
        """Return the structured payload (dict/list/scalar)."""
        if isinstance(self._payload, bytes):
            raise TypeError("payload is bytes, not structured")
        return self._payload

    def set_structured(self, value: Any) -> None:
        """Replace the payload with a structured value."""
        self._payload = value

    # ------------------------------------------------------------------
    # metadata + error
    # ------------------------------------------------------------------

    @property
    def metadata(self) -> dict[str, Any]:
        return self._metadata

    @property
    def error(self) -> str | None:
        return self._error

    @error.setter
    def error(self, value: str | None) -> None:
        self._error = value

    @property
    def payload(self) -> Any:
        """Direct access to the raw payload (bytes or structured)."""
        return self._payload


# MessageBatch is just a list of Messages. Plugin authors iterate,
# mutate in place, and return a list-of-batches in process_batch.
MessageBatch = list[Message]
