import itertools
import time
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import presolv as p
from tests.conftest import MockDns

CONCURRENCIES = [1, 5, 25, 50, 100, 250, 500, 1000, 2000, 2500, 5000, 10000, 20000]
WARMUP_SECONDS = 0.5
MEASURE_SECONDS = 2.0


def _drain_for(it, duration):
    """Pull results from a resolve() iterator for `duration` seconds; return count."""
    count = 0
    deadline = time.perf_counter() + duration
    while time.perf_counter() < deadline:
        next(it)
        count += 1
    return count


def measure_qps(mock, concurrency):
    with p.Resolver(
        nameservers=['127.0.0.1'], 
        timeout=5.0, 
        retries=2, 
        concurrency=concurrency,
        response_format="dict") as r:

        queries = (p.Query(qname="ok.example.com") for _ in itertools.count())
        it = r.resolve(queries)
        _drain_for(it, WARMUP_SECONDS)  # exclude connection/pool warm-up from the timing

        start = time.perf_counter()
        count = _drain_for(it, MEASURE_SECONDS)
        elapsed = time.perf_counter() - start
        it.close()

    return count / elapsed


def main():
    mock = MockDns()
    try:
        qps_values = []
        for concurrency in CONCURRENCIES:
            qps = measure_qps(mock, concurrency)
            print(f"concurrency={concurrency:>5} -> {qps:,.0f} qps")
            qps_values.append(qps)
    finally:
        mock.close()

    out_path = Path(__file__).with_name("throughput_vs_concurrency.png")
    plt.figure(figsize=(8, 5))
    plt.plot(CONCURRENCIES, qps_values, marker="o")
    plt.xscale("log", base=2)
    plt.xticks(CONCURRENCIES, [str(c) for c in CONCURRENCIES])
    plt.xlabel("concurrency")
    plt.ylabel("queries / second")
    plt.title("presolv throughput vs. concurrency (mock DNS server)")
    plt.grid(True, which="both", ls="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    print(f"\nsaved plot to {out_path}")


if __name__ == "__main__":
    main()