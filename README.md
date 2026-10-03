# presolv

High-performance DNS resolver for Python. The core engine is Rust/tokio
(exposed via PyO3); the Python surface is synchronous, dnspython-based, and
built for bulk and streaming resolution — resolving large query lists,
draining queues, or feeding results to/from message brokers.

- **Throughput**: all network I/O is asynchronous (tokio) regardless of the
  synchronous Python API.
- **Bounded resources**: a global rate limiter and connection pool cap
  concurrency against downstream nameservers.
- **Streaming-friendly**: `resolve()`/`resolve_stream()` accept any iterable
  — lists, generators, or a generator wrapping a broker consumer — and
  presolv has no broker-specific dependencies.
- **Lame-server detection**: nameservers that repeatedly fail to respond are
  temporarily blacklisted so they stop being sent queries.

See `spec.md` for the full design; this README covers install and usage.

## Install

```bash
pip install presolv
```

## Example

```python
import presolv
import dns.message

resolver = presolv.Resolver(rate_limit=200, pool_size=50, concurrency=500)

queries = [
    presolv.Query(dns.message.make_query("example.com", "A"), nameserver="8.8.8.8", protocol="tcp"),
    presolv.Query(dns.message.make_query("example.org", "AAAA"), nameserver="1.1.1.1"),
]

results = []
for result in resolver.resolve(queries):
    if result.error is not None:
        print(f"query {result.index} failed: {result.error}")
    else:
        results.append(result.response)

resolver.close()
```

Results arrive in **completion order**, not input order; each `Result`
carries an `index` matching the position of the original query so you can
correlate back to the input (e.g. to ack a specific broker message).

## Response formats

`Resolver(response_format="message")` (default) returns `dns.message.Message`
objects. `response_format="dict"` returns a plain, JSON-serializable nested
dict instead — useful for publishing results downstream without requiring
consumers to depend on `dnspython`. See the `dict` shape in `spec.md` §5.7.

## Broker usage patterns

Presolv has no dependency on Kafka, RabbitMQ, or any other broker client
library, and ships no broker-specific adapter classes.

**Pull model** (`resolve`):

```python
import presolv
import dns.message

def from_kafka(consumer):
    for msg in consumer:
        query = dns.message.from_wire(msg.value())
        yield presolv.Query(query, nameserver="8.8.8.8", protocol="udp")

resolver = presolv.Resolver(rate_limit=500)
for result in resolver.resolve(from_kafka(consumer)):
    handle(result)
    consumer.commit()  # ack after processing
```

**Push model** (`resolve_stream`, useful when offset/ack must happen exactly
when a result is produced):

```python
def handle_result(result: presolv.Result) -> None:
    publish_downstream(result)
    channel.basic_ack(delivery_tag=...)

resolver.resolve_stream(from_rabbitmq(consumer), on_result=handle_result)
```

## Security note

Presolv pools long-lived UDP sockets, so the source port is fixed per
`(nameserver, protocol)` key; spoofing resistance relies on DNS message-ID
randomness only (ports are not randomized per query, unlike a traditional
stub resolver opening a fresh socket per request). Use presolv against
operator-chosen/trusted resolvers, not over hostile networks.

## Out of scope (v1)

- DNS-over-TLS (DoT) and DNS-over-HTTPS (DoH) — `"udp"`/`"tcp"` only.
- Automatic TCP fallback on a truncated (TC-flagged) UDP response — the
  truncated response is returned as-is; resubmit with `protocol="tcp"` if
  needed.
- DNSSEC validation, response caching, an asyncio-native API, and built-in
  broker adapter classes.

## Development

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install maturin pytest dnspython
maturin develop
cargo test        # Rust unit + integration tests
pytest -q         # Python test suite
```
