from __future__ import annotations

import time
from typing import Optional, Sequence


class PresolvError(Exception):
    """Base class; `index` is the position of the failed query in its input."""

    def __init__(self, message: str = "", *, index: Optional[int] = None) -> None:
        super().__init__(message)
        self.index = index


class DnsTimeoutError(PresolvError): ...


class NetworkError(PresolvError): ...


class ProtocolError(PresolvError): ...


class ConnectionPoolExhausted(PresolvError): ...


class LameServerError(PresolvError):
    def __init__(
        self,
        message: str = "",
        *,
        index: Optional[int] = None,
        nameservers: Sequence[str] = (),
        retry_at: float = 0.0,
    ) -> None:
        super().__init__(message, index=index)
        self.nameservers = tuple(nameservers)
        self.retry_at = retry_at  # epoch seconds


_KINDS = {
    "timeout": DnsTimeoutError,
    "network": NetworkError,
    "protocol": ProtocolError,
    "pool_exhausted": ConnectionPoolExhausted,
}


def from_native(
    kind: str, message: str, index: int, nameservers: Sequence[str], retry_in: float
) -> PresolvError:
    if kind == "lame":
        return LameServerError(
            message, index=index, nameservers=nameservers, retry_at=time.time() + retry_in
        )
    return _KINDS.get(kind, PresolvError)(message, index=index)
