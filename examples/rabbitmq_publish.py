"""Example: publish random DNS query requests onto a RabbitMQ queue.

Run alongside `rabbitmq_resolve.py`, which drains the queue, resolves each
hostname with presolv, and publishes each result onto a response queue.

Requires a RabbitMQ broker reachable at localhost:5672, e.g.:
    podman run -d --name rabbitmq -p 5672:5672 -p 15672:15672 \
        rabbitmq:4-management
and the `pika` package: pip install pika

Configure via RABBITMQ_HOST / RABBITMQ_USER / RABBITMQ_PASSWORD env vars.
"""

from __future__ import annotations

import random
import time

from _rabbitmq_common import REQUEST_QUEUE, connect

HOSTNAME_POOL = [
    "example.com",
    "python.org",
    "rabbitmq.com",
    "github.com",
    "wikipedia.org",
    "cloudflare.com",
    "does-not-exist.invalid",
]

PUBLISH_INTERVAL_RANGE = (0.0001, 0.0002)  # seconds between publishes


def main() -> None:
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=REQUEST_QUEUE, durable=True)

    count = 0
    print(f"publishing random queries to queue {REQUEST_QUEUE!r} (Ctrl+C to stop)")
    try:
        while True:
            hostname = random.choice(HOSTNAME_POOL)
            channel.basic_publish(exchange="", routing_key=REQUEST_QUEUE, body=hostname.encode())
            count += 1
            print(f"[{count}] published {hostname!r}")
            time.sleep(random.uniform(*PUBLISH_INTERVAL_RANGE))
    except KeyboardInterrupt:
        print(f"\nstopped after publishing {count} queries")
    finally:
        connection.close()


if __name__ == "__main__":
    main()
