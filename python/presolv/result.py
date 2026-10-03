from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union

import dns.message

from .errors import PresolvError


@dataclass(frozen=True)
class Result:
    response: Optional[Union[dns.message.Message, dict]]
    error: Optional[PresolvError]
    nameserver: str
    protocol: str
    rtt_ms: float
    index: int
