"""Shared RabbitMQ connection settings for the `rabbitmq_publish.py` /
`rabbitmq_resolve.py` example pair.
"""

from __future__ import annotations

import os

import pika

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
RABBITMQ_USER = os.environ.get("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.environ.get("RABBITMQ_PASSWORD", "guest")

REQUEST_QUEUE = "dns_queries"
RESPONSE_QUEUE = "dns_responses"


def connect() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    return pika.BlockingConnection(params)
