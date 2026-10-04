"""Example: consume DNS query requests from RabbitMQ, resolve them with
presolv, and publish each result onto a response queue (spec.md §11 "push
model": `resolve_stream`, act on each result as it's produced).

Run alongside `rabbitmq_publish.py`, which feeds random hostnames onto the
request queue.

Requires a RabbitMQ broker reachable at localhost:5672, e.g.:
    podman run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
        rabbitmq:4-management
and the `pika` package: pip install pika

Configure via RABBITMQ_HOST / RABBITMQ_USER / RABBITMQ_PASSWORD env vars.

Threading note: presolv's `resolve_stream` pulls from the input generator on
an internal background thread, while `on_result` callbacks run on the
calling (main) thread. pika's `BlockingConnection` is not safe to share
across threads, so this example uses two independent connections: one
touched only by the generator (background thread, consuming requests) and
one touched only by `on_result` (main thread, publishing responses).
Consuming uses `auto_ack=True` for simplicity -- a request is considered
handled as soon as the broker hands it over, not once its response has
actually been published.
"""

from __future__ import annotations

import json
from typing import Iterator

import presolv
from _rabbitmq_common import REQUEST_QUEUE, RESPONSE_QUEUE, connect


def from_rabbitmq(queue_name: str) -> Iterator[presolv.Query]:
    """Open its own connection and block on `queue_name`, yielding one `Query`
    per message as it arrives. Runs entirely on presolv's feeder thread."""
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=queue_name, durable=True)
    try:
        for _method, _properties, body in channel.consume(queue_name, auto_ack=True):
            yield presolv.Query(qname=body.decode())
    finally:
        connection.close()


def main() -> None:
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=RESPONSE_QUEUE, durable=True)

    count = 0

    def on_result(result: presolv.Result) -> None:
        nonlocal count
        payload = {"error": str(result.error)} if result.error is not None else result.response
        channel.basic_publish(
            exchange="", routing_key=RESPONSE_QUEUE, body=json.dumps(payload).encode()
        )
        count += 1
        print(f"[{result.index}] resolved -> published to {RESPONSE_QUEUE!r}")

    print(
        f"listening on queue {REQUEST_QUEUE!r}, "
        f"publishing responses to {RESPONSE_QUEUE!r} (Ctrl+C to stop)"
    )
    try:
        with presolv.Resolver(response_format="dict") as resolver:
            resolver.resolve_stream(from_rabbitmq(REQUEST_QUEUE), on_result=on_result)
    except KeyboardInterrupt:
        print(f"\nstopped after publishing {count} responses")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
