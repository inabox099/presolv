import json

import dns.edns
import dns.flags
import dns.message
import dns.rcode
import dns.rrset
import pytest

from presolv import _convert, errors


def _response(with_edns=False):
    q = dns.message.make_query("example.com", "A")
    r = dns.message.make_response(q)
    if with_edns:
        r.use_edns(0, dns.flags.DO, 1232)  # make_response never copies DO; set it explicitly
    r.flags |= dns.flags.RA
    r.answer.append(dns.rrset.from_text("example.com.", 300, "IN", "A", "192.0.2.1", "192.0.2.2"))
    r.authority.append(dns.rrset.from_text("example.com.", 60, "IN", "NS", "ns1.example.com."))
    return r


def test_message_from_wire_roundtrip():
    r = _response()
    m = _convert.message_from_wire(r.to_wire())
    assert m.id == r.id
    assert {rr.to_text() for rr in m.answer[0]} == {rr.to_text() for rr in r.answer[0]}


def test_message_from_wire_garbage_is_protocol_error():
    with pytest.raises(errors.ProtocolError):
        _convert.message_from_wire(b"\x00\x01\x02")


def test_dict_shape_and_flattening():
    d = _convert.message_to_dict(_response())
    assert d["id"] > -1 and d["opcode"] == "QUERY" and d["rcode"] == "NOERROR"
    assert set(d["flags"]) == {"QR", "RD", "RA"}
    assert d["question"] == [{"name": "example.com.", "type": "A", "class": "IN"}]
    assert d["answer"] == [
        {"name": "example.com.", "type": "A", "class": "IN", "ttl": 300, "data": "192.0.2.1"},
        {"name": "example.com.", "type": "A", "class": "IN", "ttl": 300, "data": "192.0.2.2"},
    ]
    assert d["authority"][0]["data"] == "ns1.example.com."
    assert d["additional"] == []
    assert d["edns"] is None
    assert set(d) == {"id", "opcode", "rcode", "flags", "question", "answer",
                      "authority", "additional", "edns"}


def test_dict_is_json_serializable():
    json.dumps(_convert.message_to_dict(_response(with_edns=True)))


def test_edns_extracted_and_opt_excluded_from_additional():
    wire = _response(with_edns=True).to_wire()
    d = _convert.message_to_dict(_convert.message_from_wire(wire))
    assert d["edns"]["version"] == 0
    assert "DO" in d["edns"]["flags"]
    assert isinstance(d["edns"]["payload"], int)
    assert d["edns"]["options"] == []
    assert all(rr["type"] != "OPT" for rr in d["additional"])


def test_rcode_name():
    r = _response()
    r.set_rcode(dns.rcode.NXDOMAIN)
    assert _convert.message_to_dict(r)["rcode"] == "NXDOMAIN"


def test_convert_response_formats():
    wire = _response().to_wire()
    assert isinstance(_convert.convert_response(wire, "message"), dns.message.Message)
    assert isinstance(_convert.convert_response(wire, "dict"), dict)
