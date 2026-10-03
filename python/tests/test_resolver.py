import threading
import time

import dns.message
import dns.rcode
import pytest

import presolv
from presolv import Query, Resolver


def mk(mock, **kw):
    kw.setdefault("timeout", 2.0)
    kw.setdefault("retries", 0)
    return Resolver(nameservers=[mock.ns], **kw)


def test_basic_resolve_message_format(mock_dns):
    with mk(mock_dns()) as r:
        (res,) = list(r.resolve([Query(qname="example.com")]))
    assert res.error is None and res.index == 0 and res.protocol == "udp"
    assert res.rtt_ms > 0 and res.nameserver
    assert isinstance(res.response, dns.message.Message)
    assert res.response.answer[0][0].to_text() == "192.0.2.1"


def test_response_id_equals_submitted_id(mock_dns):
    q = dns.message.make_query("example.com", "A")
    with mk(mock_dns()) as r:
        (res,) = list(r.resolve([q]))  # bare Message accepted
    assert res.response.id == q.id


def test_dict_format(mock_dns):
    with mk(mock_dns(), response_format="dict") as r:
        (res,) = list(r.resolve([Query(qname="example.com")]))
    assert res.response["rcode"] == "NOERROR"
    assert res.response["answer"][0]["data"] == "192.0.2.1"


def test_tcp_protocol(mock_dns):
    m = mock_dns()
    with mk(m) as r:
        (res,) = list(r.resolve([Query(qname="example.com", protocol="tcp")]))
    assert res.error is None and res.protocol == "tcp"
    assert m.count(proto="tcp") == 1 and m.count(proto="udp") == 0


def test_pinned_nameserver_overrides_default(mock_dns):
    default, pinned = mock_dns(), mock_dns()
    with mk(default) as r:
        q = Query(qname="example.com", nameserver="127.0.0.1", port=pinned.port)
        (res,) = list(r.resolve([q]))
    assert res.error is None
    assert pinned.count() == 1 and default.count() == 0


def test_nxdomain_is_a_response_not_an_error(mock_dns):
    with mk(mock_dns()) as r:
        (res,) = list(r.resolve([Query(qname="nxdomain.example.com")]))
    assert res.error is None and res.response.rcode() == dns.rcode.NXDOMAIN


def test_results_in_completion_order_with_correct_index(mock_dns):
    with mk(mock_dns()) as r:
        out = list(r.resolve([Query(qname="slow.example.com"), Query(qname="fast.example.com")]))
    assert [x.index for x in out] == [1, 0]


def test_empty_input_terminates(mock_dns):
    with mk(mock_dns()) as r:
        assert list(r.resolve([])) == []


def test_blocking_source_does_not_stall_completed_results(mock_dns):
    stamps = {}

    def source():
        yield Query(qname="a.example.com")
        time.sleep(0.8)  # broker consumer waiting on an empty topic
        stamps["second_yielded"] = time.monotonic()
        yield Query(qname="b.example.com")

    with mk(mock_dns()) as r:
        it = r.resolve(source())
        first = next(it)
        assert first.index == 0
        # first result delivered while the source is still blocked producing item 2
        assert "second_yielded" not in stamps
        assert len(list(it)) == 1
        assert "second_yielded" in stamps


def test_source_exception_surfaces_after_submitted_results(mock_dns):
    def source():
        yield Query(qname="a.example.com")
        raise RuntimeError("broker died")

    with mk(mock_dns()) as r:
        it = r.resolve(source())
        assert next(it).index == 0
        with pytest.raises(RuntimeError, match="broker died"):
            next(it)
        with pytest.raises(StopIteration):
            next(it)


def test_bad_item_type_surfaces_as_type_error(mock_dns):
    with mk(mock_dns()) as r:
        with pytest.raises(TypeError):
            list(r.resolve(["example.com"]))


def test_break_early_then_close(mock_dns):
    with mk(mock_dns()) as r:
        it = r.resolve(Query(qname=f"q{i}.example.com") for i in range(100))
        next(it)
        it.close()
        with pytest.raises(StopIteration):
            next(it)


def test_resolve_after_close_raises(mock_dns):
    r = mk(mock_dns())
    r.close()
    r.close()  # idempotent
    with pytest.raises(RuntimeError):
        r.resolve([])


def test_resolve_stream_invokes_callback_per_result(mock_dns):
    got = []
    with mk(mock_dns()) as r:
        r.resolve_stream((Query(qname=f"q{i}.example.com") for i in range(10)), got.append)
    assert sorted(x.index for x in got) == list(range(10))


def test_resolve_stream_source_failure_goes_to_on_error(mock_dns):
    errs = []

    def source():
        yield Query(qname="a.example.com")
        raise RuntimeError("boom")

    got = []
    with mk(mock_dns()) as r:
        r.resolve_stream(source(), got.append, on_error=errs.append)
    assert len(got) == 1 and len(errs) == 1 and str(errs[0]) == "boom"


def test_resolve_stream_source_failure_without_on_error_raises(mock_dns):
    def source():
        raise RuntimeError("boom")
        yield

    with mk(mock_dns()) as r:
        with pytest.raises(RuntimeError):
            r.resolve_stream(source(), lambda _: None)


def test_resolve_stream_callback_exception_propagates_and_stops(mock_dns):
    seen = []

    def cb(res):
        seen.append(res)
        raise ValueError("handler bug")

    with mk(mock_dns()) as r:
        with pytest.raises(ValueError, match="handler bug"):
            r.resolve_stream((Query(qname=f"q{i}.example.com") for i in range(50)), cb)
    assert len(seen) == 1


def test_default_nameservers_come_from_system_config(monkeypatch):
    from presolv import _presolv

    calls = []
    monkeypatch.setattr(_presolv, "system_nameservers", lambda: calls.append(1) or [("127.0.0.1", 53)])
    r = Resolver()
    r.close()
    assert calls == [1]


def test_invalid_arguments():
    with pytest.raises(ValueError):
        Resolver(nameservers=["not-an-ip"])
    with pytest.raises(ValueError):
        Resolver(nameservers=["127.0.0.1"], response_format="xml")
    with pytest.raises(ValueError):
        Resolver(nameservers=["127.0.0.1"], concurrency=0)
