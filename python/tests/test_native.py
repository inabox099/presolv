import time

import dns.message
import pytest

from presolv import _presolv


def _wait(session, timeout=3.0):
    end = time.time() + timeout
    while time.time() < end:
        raw = session.next(0.1)
        if raw is not None:
            return raw
    raise AssertionError("no result")


def _engine(ns_list, **kw):
    args = dict(
        nameservers=ns_list, timeout=1.0, retries=0, rate_limit=None, burst=None,
        pool_size=10, idle_timeout=60.0, blacklist_after=None, blacklist_duration=30.0,
        worker_threads=2,
    )
    args.update(kw)
    return _presolv.NativeEngine(**args)


def test_roundtrip_returns_wire_and_metadata(mock_dns):
    mock = mock_dns()
    eng = _engine([mock.ns])
    s = eng.session()
    q = dns.message.make_query("example.com", "A")
    s.submit(5, q.to_wire(), None, "udp")
    index, wire, kind, message, ns, proto, rtt_ms, lame_ns, lame_in = _wait(s)
    assert (index, kind, proto) == (5, None, "udp")
    assert ns == f"127.0.0.1:{mock.port}"
    assert rtt_ms > 0
    assert dns.message.from_wire(wire).id == q.id
    eng.close()


def test_next_times_out_with_none(mock_dns):
    eng = _engine([mock_dns().ns])
    s = eng.session()
    assert s.next(0.05) is None
    eng.close()


def test_error_is_reported_as_kind(mock_dns):
    mock = mock_dns()
    eng = _engine([mock.ns], timeout=0.2)
    s = eng.session()
    s.submit(0, dns.message.make_query("drop.example.com", "A").to_wire(), None, "udp")
    raw = _wait(s)
    assert raw[1] is None and raw[2] == "timeout"
    eng.close()


def test_lame_error_carries_nameservers_and_retry_in(mock_dns):
    mock = mock_dns(drop_all=True)
    eng = _engine([mock.ns], timeout=0.1, blacklist_after=1, blacklist_duration=5.0)
    s = eng.session()
    w = dns.message.make_query("example.com", "A").to_wire()
    s.submit(0, w, None, "udp")
    assert _wait(s)[2] == "timeout"
    s.submit(1, w, None, "udp")
    raw = _wait(s)
    assert raw[2] == "lame"
    assert raw[7] == [f"127.0.0.1:{mock.port}"]
    assert 0 < raw[8] <= 5.0
    eng.close()


def test_pinned_nameserver_and_bad_inputs(mock_dns):
    mock = mock_dns()
    eng = _engine([("127.0.0.1", 9)])
    s = eng.session()
    s.submit(0, dns.message.make_query("example.com", "A").to_wire(), mock.ns, "tcp")
    raw = _wait(s)
    assert raw[2] is None and raw[5] == "tcp"
    with pytest.raises(ValueError):
        s.submit(1, b"x" * 20, ("not-an-ip", 53), "udp")
    with pytest.raises(ValueError):
        s.submit(1, b"x" * 20, None, "tls")
    eng.close()


def test_sessions_are_isolated(mock_dns):
    eng = _engine([mock_dns().ns])
    a, b = eng.session(), eng.session()
    a.submit(1, dns.message.make_query("a.example", "A").to_wire(), None, "udp")
    assert b.next(0.2) is None
    assert _wait(a)[0] == 1
    eng.close()


def test_system_nameservers_returns_list_of_pairs():
    got = _presolv.system_nameservers()
    assert isinstance(got, list)
    assert all(isinstance(ip, str) and port == 53 for ip, port in got)
