from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Literal, Optional, Union

import dns.flags
import dns.message


@dataclass(frozen=True)
class Query:
    message: Optional[dns.message.Message] = None
    qname: Optional[str] = None
    rdtype: Union[str, int] = "A"
    nameserver: Optional[str] = None
    port: Optional[int] = None
    protocol: Literal["udp", "tcp"] = "udp"
    recursion_desired: bool = True
    checking_disabled: bool = False
    dnssec_ok: bool = False
    flags: int = 0

    def __post_init__(self) -> None:
        if self.protocol not in ("udp", "tcp"):
            raise ValueError(f"unsupported protocol: {self.protocol!r}")
        if self.nameserver is not None:
            ipaddress.ip_address(self.nameserver)  # ValueError unless bare IPv4/IPv6 literal
        if self.port is not None and not (0 < self.port < 65536):
            raise ValueError(f"invalid port: {self.port!r}")
        if self.message is not None and self.qname is not None:
            raise ValueError("pass either `message` or `qname`, not both")
        if self.message is None:
            if self.qname is None:
                raise ValueError("Query requires either `message` or `qname`")
            message = dns.message.make_query(
                self.qname, self.rdtype, want_dnssec=self.dnssec_ok
            )
            # raw `flags` first; dedicated fields take precedence over it
            message.flags |= self.flags
            if not self.recursion_desired:
                message.flags &= ~dns.flags.RD
            if self.checking_disabled:
                message.flags |= dns.flags.CD
            object.__setattr__(self, "message", message)


def coerce_query(item: Union[Query, dns.message.Message]) -> Query:
    if isinstance(item, Query):
        return item
    if isinstance(item, dns.message.Message):
        return Query(item)
    raise TypeError(f"expected Query or dns.message.Message, got {type(item).__name__}")
