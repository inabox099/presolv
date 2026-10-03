import threading
import time

import dns.flags
import dns.message
import pytest

import presolv
from presolv import (
    ConnectionPoolExhausted, DnsTimeoutError, LameServerError, NetworkError,
    ProtocolError, Query, Resolver,
)

from conftest import closed_tcp_port


def one(r, q):
    (res,) = list(r.resolve([q]))
    return res


# ---- spec §10 matrix ---------------------------------------------------------
def test_timeout_after_retries(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=0.2, retries=1, blacklist_after=None) as r:
        res = one(r, Query(qname="drop.example.com"))
    assert isinstance(res.error, DnsTimeoutError) and res.response is None and res.index == 0
    assert m.count("drop") == 2  # first attempt + 1 retry


def test_network_error_connection_refused():
    with Resolver(nameservers=["127.0.0.1"], timeout=0.5, retries=0, blacklist_after=None) as r:
        q = Query(qname="example.com", protocol="tcp", nameserver="127.0.0.1", port=closed_tcp_port())
        res = one(r, q)
    assert isinstance(res.error, NetworkError)


def test_malformed_response_is_protocol_error_and_not_retried(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=1.0, retries=3) as r:
        res = one(r, Query(qname="short.example.com"))
    assert isinstance(res.error, ProtocolError)
    assert m.count("short") == 1


def test_truncated_response_returned_as_is_without_tcp_fallback(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=1.0) as r:
        res = one(r, Query(qname="tc.example.com"))
    assert res.error is None and res.response.flags & dns.flags.TC
    assert m.count(proto="tcp") == 0


# ---- raise_on_error ----------------------------------------------------------
def test_raise_on_error_raises_with_index_and_iterator_stays_usable(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=0.2, retries=0, raise_on_error=True,
                  blacklist_after=None) as r:
        it = r.resolve([Query(qname="drop.example.com"), Query(qname="slow.example.com")])
        got, errs = [], []
        while True:
            try:
                got.append(next(it))
            except StopIteration:
                break
            except presolv.PresolvError as e:
                errs.append(e)
    # slow answers after 0.3s > timeout 0.2 => both time out
    assert len(errs) == 2 and all(isinstance(e, DnsTimeoutError) for e in errs)
    assert sorted(e.index for e in errs) == [0, 1]


def test_raise_on_error_mixed(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=0.2, retries=0, raise_on_error=True) as r:
        it = r.resolve([Query(qname="drop.example.com"), Query(qname="ok.example.com")])
        ok, bad = [], []
        while True:
            try:
                ok.append(next(it))
            except StopIteration:
                break
            except presolv.PresolvError as e:
                bad.append(e)
    assert [x.index for x in ok] == [1] and [e.index for e in bad] == [0]


def test_resolve_stream_raise_on_error_propagates(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=0.2, retries=0, raise_on_error=True) as r:
        with pytest.raises(DnsTimeoutError):
            r.resolve_stream([Query(qname="drop.example.com")], lambda _: None)


# ---- lame-server blacklisting (spec §9) --------------------------------------
def test_blacklist_fail_fast_and_recovery(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=0.1, retries=0,
                  blacklist_after=2, blacklist_duration=0.5) as r:
        for _ in range(2):
            assert isinstance(one(r, Query(qname="drop.example.com")).error, DnsTimeoutError)
        sent = m.count()
        t = time.time()
        res = one(r, Query(qname="ok.example.com"))
        assert isinstance(res.error, LameServerError)
        assert res.error.nameservers and t < res.error.retry_at <= t + 0.6
        assert m.count() == sent, "no network attempt while blacklisted"
        time.sleep(0.6)
        assert one(r, Query(qname="ok.example.com")).error is None  # recovered


def test_pinned_blacklisted_nameserver_fails_fast(mock_dns):
    dead, good = mock_dns(drop_all=True), mock_dns()
    with Resolver(nameservers=[good.ns], timeout=0.1, retries=0,
                  blacklist_after=1, blacklist_duration=5.0) as r:
        q = Query(qname="example.com", nameserver="127.0.0.1", port=dead.port)
        assert isinstance(one(r, q).error, DnsTimeoutError)
        sent = dead.count()
        assert isinstance(one(r, q).error, LameServerError)
        assert dead.count() == sent
        assert one(r, Query(qname="example.com")).error is None  # default server unaffected


def test_blacklisting_can_be_disabled(mock_dns):
    m = mock_dns(drop_all=True)
    with Resolver(nameservers=[m.ns], timeout=0.05, retries=0, blacklist_after=None) as r:
        errs = [one(r, Query(qname="example.com")).error for _ in range(8)]
    assert all(isinstance(e, DnsTimeoutError) for e in errs)


def test_blacklist_raise_on_error_carries_retry_at(mock_dns):
    m = mock_dns(drop_all=True)
    with Resolver(nameservers=[m.ns], timeout=0.05, retries=0, blacklist_after=1,
                  blacklist_duration=5.0, raise_on_error=True) as r:
        with pytest.raises(DnsTimeoutError):
            list(r.resolve([Query(qname="example.com")]))
        with pytest.raises(LameServerError) as ei:
            list(r.resolve([Query(qname="example.com")]))
    assert ei.value.retry_at > time.time()


# ---- nameserver selection (spec §9) ------------------------------------------
def test_round_robin_across_default_nameservers(mock_dns):
    a, b = mock_dns(), mock_dns()
    with Resolver(nameservers=[a.ns, b.ns], timeout=1.0) as r:
        for i in range(6):
            assert one(r, Query(qname=f"q{i}.example.com")).error is None
    assert (a.count(), b.count()) == (3, 3)


def test_retry_fails_over_to_next_nameserver(mock_dns):
    dead, good = mock_dns(drop_all=True), mock_dns()
    with Resolver(nameservers=[dead.ns, good.ns], timeout=0.2, retries=1, blacklist_after=None) as r:
        res = one(r, Query(qname="example.com"))
    assert res.error is None
    assert res.nameserver == f"127.0.0.1:{good.port}"


# ---- id rewriting (spec §6) --------------------------------------------------
def test_many_messages_with_identical_ids_are_not_confused(mock_dns):
    queries = []
    for i in range(60):
        m = dns.message.make_query(f"host{i}.example.com", "A")
        m.id = 0
        queries.append(Query(m))
    with Resolver(nameservers=[mock_dns().ns], timeout=3.0) as r:
        results = list(r.resolve(queries))
    assert len(results) == 60
    for res in results:
        assert res.error is None and res.response.id == 0
        assert res.response.question[0].name.to_text() == f"host{res.index}.example.com."


# ---- concurrency / backpressure (spec §5.1, §6) ------------------------------
def test_concurrency_bounds_how_far_the_source_is_read(mock_dns):
    pulled = []

    def source():
        for i in range(6):
            pulled.append(i)
            yield Query(qname=f"slow{i}.example.com")  # each takes 0.3 s on the server

    with Resolver(nameservers=[mock_dns().ns], timeout=3.0, concurrency=2) as r:
        it = r.resolve(source())
        time.sleep(0.15)  # 2 in flight, none consumed yet
        assert len(pulled) == 2, pulled
        assert len(list(it)) == 6


def test_slow_consumer_keeps_slots_occupied_until_results_are_consumed(mock_dns):
    pulled = []

    def source():
        for i in range(8):
            pulled.append(i)
            yield Query(qname=f"q{i}.example.com")

    with Resolver(nameservers=[mock_dns().ns], timeout=3.0, concurrency=3) as r:
        it = r.resolve(source())
        time.sleep(0.5)  # all 3 answered quickly, but nothing consumed => no more pulls
        assert len(pulled) == 3
        next(it)
        time.sleep(0.2)
        assert len(pulled) == 4
        it.close()


def test_large_batch_completes(mock_dns):
    n = 1000
    with Resolver(nameservers=[mock_dns().ns], timeout=5.0, concurrency=200) as r:
        results = list(r.resolve(Query(qname=f"q{i}.example.com") for i in range(n)))
    assert sorted(x.index for x in results) == list(range(n))
    assert all(x.error is None for x in results)


def test_concurrent_resolve_calls_have_independent_indexes(mock_dns):
    out = {}

    def run(tag):
        out[tag] = list(r.resolve(Query(qname=f"{tag}{i}.example.com") for i in range(40)))

    with Resolver(nameservers=[mock_dns().ns], timeout=3.0, concurrency=50) as r:
        ts = [threading.Thread(target=run, args=(t,)) for t in ("a", "b")]
        [t.start() for t in ts]
        [t.join() for t in ts]
    for tag in ("a", "b"):
        assert sorted(x.index for x in out[tag]) == list(range(40))
        for x in out[tag]:
            assert x.response.question[0].name.to_text().startswith(f"{tag}{x.index}.")


# ---- rate limit & pool (spec §7) ---------------------------------------------
def test_rate_limit_paces_queries(mock_dns):
    with Resolver(nameservers=[mock_dns().ns], timeout=2.0, rate_limit=20, burst=1) as r:
        t = time.monotonic()
        results = list(r.resolve(Query(qname=f"q{i}.example.com") for i in range(6)))
        elapsed = time.monotonic() - t
    assert all(x.error is None for x in results)
    assert elapsed >= 0.2, elapsed  # 5 intervals of 50 ms after the first immediate token


def test_pool_exhausted_when_cap_reached_by_busy_connection(mock_dns):
    a, b = mock_dns(), mock_dns()

    def src():
        yield Query(qname="slow.example.com")  # each 100ms attempt times out (.3s server delay);
        # retries=3 keeps re-occupying the only pool slot across ~400ms of attempts
        time.sleep(0.03)  # feeder is sequential: guarantees `slow` is in flight first
        yield Query(qname="example.com", nameserver="127.0.0.1", port=b.port)

    with Resolver(nameservers=[a.ns], timeout=0.1, retries=3, pool_size=1) as r:
        res = {x.index: x for x in r.resolve(src())}
    assert isinstance(res[1].error, ConnectionPoolExhausted)
    assert b.count() == 0
