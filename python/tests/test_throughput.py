"""Throughput benchmark: measures max sustained queries/sec against a mock DNS server.

Not a strict correctness test - the QPS floor asserted here is set low enough to
avoid flaking on slow/loaded CI machines while still catching gross regressions
(e.g. accidental serialization of concurrent queries).
"""

import time

from presolv import Query, Resolver

WARMUP_QUERIES = 500
MEASURED_QUERIES = 20_000
MIN_QPS = 500  # conservative floor; a healthy engine sustains far more locally


def _run(r, n):
    return list(r.resolve(Query(qname="ok.example.com") for _ in range(n)))


def test_max_throughput_qps(mock_dns):
    m = mock_dns()
    with Resolver(nameservers=[m.ns], timeout=2.0, retries=0, concurrency=500) as r:
        _run(r, WARMUP_QUERIES)  # exclude connection/pool/thread warm-up from the timing

        start = time.perf_counter()
        results = _run(r, MEASURED_QUERIES)
        elapsed = time.perf_counter() - start

    assert len(results) == MEASURED_QUERIES
    assert all(res.error is None for res in results)

    qps = MEASURED_QUERIES / elapsed
    print(f"\nthroughput: {MEASURED_QUERIES} queries in {elapsed:.3f}s -> {qps:,.0f} qps")
    assert qps > MIN_QPS
