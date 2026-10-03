import dns.flags
import dns.message
import pytest

from presolv.query import Query, coerce_query


def test_qname_builds_message():
    q = Query(qname="example.com", rdtype="MX")
    assert q.message.question[0].to_text() == "example.com. IN MX"
    assert q.message.flags & dns.flags.RD


def test_message_used_as_is_and_flags_ignored():
    m = dns.message.make_query("example.com", "A")
    q = Query(m, recursion_desired=False, checking_disabled=True, dnssec_ok=True, flags=dns.flags.AD)
    assert q.message is m and q.message.flags & dns.flags.RD and not q.message.flags & dns.flags.CD


def test_message_and_qname_are_exclusive_and_one_required():
    with pytest.raises(ValueError):
        Query(dns.message.make_query("a.com", "A"), qname="a.com")
    with pytest.raises(ValueError):
        Query()


def test_flag_fields():
    q = Query(qname="example.com", recursion_desired=False, checking_disabled=True, dnssec_ok=True)
    assert not q.message.flags & dns.flags.RD
    assert q.message.flags & dns.flags.CD
    assert q.message.ednsflags & dns.flags.DO


def test_dedicated_flags_win_over_raw_flags():
    q = Query(qname="example.com", recursion_desired=False, flags=dns.flags.RD | dns.flags.AD)
    assert not q.message.flags & dns.flags.RD  # RD cleared after raw flags applied
    assert q.message.flags & dns.flags.AD


@pytest.mark.parametrize("bad", ["tls", "https", "UDP", ""])
def test_unsupported_protocol_raises_value_error(bad):
    with pytest.raises(ValueError):
        Query(qname="example.com", protocol=bad)


@pytest.mark.parametrize("bad", ["8.8.8.8:53", "[::1]", "dns.google", "999.1.1.1", ""])
def test_nameserver_must_be_bare_ip_literal(bad):
    with pytest.raises(ValueError):
        Query(qname="example.com", nameserver=bad)


@pytest.mark.parametrize("ok", ["8.8.8.8", "2001:4860:4860::8888"])
def test_nameserver_accepts_ip_literals(ok):
    assert Query(qname="example.com", nameserver=ok).nameserver == ok


@pytest.mark.parametrize("bad", [0, -1, 65536])
def test_port_validated(bad):
    with pytest.raises(ValueError):
        Query(qname="example.com", port=bad)


def test_coerce_bare_message():
    m = dns.message.make_query("example.com", "A")
    q = coerce_query(m)
    assert isinstance(q, Query) and q.message is m and q.nameserver is None and q.protocol == "udp"
    assert coerce_query(q) is q


def test_coerce_rejects_other_types():
    with pytest.raises(TypeError):
        coerce_query("example.com")
