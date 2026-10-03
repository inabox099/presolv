from __future__ import annotations

from typing import Union

import dns.edns
import dns.exception
import dns.flags
import dns.message
import dns.opcode
import dns.rcode
import dns.rdataclass
import dns.rdatatype

from .errors import ProtocolError


def message_from_wire(wire: bytes) -> dns.message.Message:
    try:
        return dns.message.from_wire(wire)
    except (dns.exception.DNSException, ValueError, IndexError) as exc:
        raise ProtocolError(f"unparseable DNS response: {exc}") from exc


def _section(rrsets) -> list:
    out = []
    for rrset in rrsets:
        if rrset.rdtype == dns.rdatatype.OPT:
            continue
        name = rrset.name.to_text()
        rtype = dns.rdatatype.to_text(rrset.rdtype)
        rclass = dns.rdataclass.to_text(rrset.rdclass)
        for rdata in rrset:
            out.append(
                {"name": name, "type": rtype, "class": rclass,
                 "ttl": rrset.ttl, "data": rdata.to_text()}
            )
    return out


def message_to_dict(msg: dns.message.Message) -> dict:
    edns = None
    if msg.edns >= 0:
        edns = {
            "version": msg.edns,
            "flags": dns.flags.edns_to_text(msg.ednsflags).split(),
            "payload": msg.payload,
            "options": [{"code": int(o.otype), "data": o.to_wire().hex()} for o in msg.options],
        }
    return {
        "id": msg.id,
        "opcode": dns.opcode.to_text(msg.opcode()),
        "rcode": dns.rcode.to_text(msg.rcode()),
        "flags": dns.flags.to_text(msg.flags).split(),
        "question": [
            {"name": rr.name.to_text(), "type": dns.rdatatype.to_text(rr.rdtype),
             "class": dns.rdataclass.to_text(rr.rdclass)}
            for rr in msg.question
        ],
        "answer": _section(msg.answer),
        "authority": _section(msg.authority),
        "additional": _section(msg.additional),
        "edns": edns,
    }


def convert_response(wire: bytes, fmt: str) -> Union[dns.message.Message, dict]:
    msg = message_from_wire(wire)
    return message_to_dict(msg) if fmt == "dict" else msg
