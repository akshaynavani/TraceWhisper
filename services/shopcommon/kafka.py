"""JSON event producer/consumer on top of aiokafka, with trace propagation.

The producer injects W3C ``traceparent``/``tracestate`` into Kafka record
headers; the consumer extracts them so the processing span becomes a child of
the publishing span. That single detail is what lets a checkout trace continue
into the asynchronous orders service.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord
from opentelemetry import propagate, trace
from opentelemetry.trace import SpanKind

from .metrics import KAFKA_MESSAGES, KAFKA_PROCESS_LATENCY
from .telemetry import mark_error

log = logging.getLogger(__name__)
_tracer = trace.get_tracer("tracewhisper.shop.kafka")

Handler = Callable[[dict[str, Any], ConsumerRecord], Awaitable[None]]


def _inject_headers() -> list[tuple[str, bytes]]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(k, v.encode()) for k, v in carrier.items()]


def _extract_context(headers: list[tuple[str, bytes]] | None):
    carrier = {k: v.decode() for k, v in (headers or []) if v is not None}
    return propagate.extract(carrier)


class EventProducer:
    def __init__(self, bootstrap: str, client_id: str):
        self.client_id = client_id
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            client_id=client_id,
            value_serializer=lambda v: json.dumps(v).encode(),
            key_serializer=lambda k: k.encode() if k else None,
            acks="all",
            enable_idempotence=True,
        )

    async def start(self) -> None:
        await _retry(self._producer.start, "producer")

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(self, topic: str, key: str | None, value: dict[str, Any]) -> None:
        with _tracer.start_as_current_span(
            f"{topic} publish",
            kind=SpanKind.PRODUCER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": topic,
                "messaging.operation": "publish",
                "messaging.kafka.message.key": key or "",
                "messaging.client_id": self.client_id,
            },
        ) as span:
            try:
                metadata = await self._producer.send_and_wait(
                    topic, value=value, key=key, headers=_inject_headers()
                )
                span.set_attribute("messaging.kafka.destination.partition", metadata.partition)
                span.set_attribute("messaging.kafka.message.offset", metadata.offset)
                KAFKA_MESSAGES.labels(self.client_id, topic, "out", "ok").inc()
            except Exception as exc:
                mark_error(span, exc)
                KAFKA_MESSAGES.labels(self.client_id, topic, "out", "error").inc()
                raise


class EventConsumer:
    def __init__(self, bootstrap: str, topic: str, group_id: str, handler: Handler):
        self.topic = topic
        self.group_id = group_id
        self._handler = handler
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            value_deserializer=lambda b: json.loads(b.decode()),
        )
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        await _retry(self._consumer.start, "consumer")
        self._task = asyncio.create_task(self._loop(), name=f"consume:{self.topic}")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self._consumer.stop()

    async def _loop(self) -> None:
        async for record in self._consumer:
            await self._process(record)
            await self._consumer.commit()

    async def _process(self, record: ConsumerRecord) -> None:
        parent = _extract_context(record.headers)
        queue_ms = max(0, int(time.time() * 1000) - record.timestamp)
        with _tracer.start_as_current_span(
            f"{record.topic} process",
            context=parent,
            kind=SpanKind.CONSUMER,
            attributes={
                "messaging.system": "kafka",
                "messaging.destination.name": record.topic,
                "messaging.operation": "process",
                "messaging.kafka.consumer.group": self.group_id,
                "messaging.kafka.destination.partition": record.partition,
                "messaging.kafka.message.offset": record.offset,
                "messaging.kafka.queue_time_ms": queue_ms,
            },
        ) as span:
            start = time.perf_counter()
            try:
                await self._handler(record.value, record)
                KAFKA_MESSAGES.labels(self.group_id, record.topic, "in", "ok").inc()
            except Exception as exc:  # handler decides on DLQ; never kill the loop
                mark_error(span, exc)
                KAFKA_MESSAGES.labels(self.group_id, record.topic, "in", "error").inc()
                log.exception("unhandled error processing %s@%s", record.topic, record.offset)
            finally:
                KAFKA_PROCESS_LATENCY.labels(self.group_id, record.topic).observe(time.perf_counter() - start)


async def _retry(start: Callable[[], Awaitable[None]], what: str, attempts: int = 30) -> None:
    """Kafka often comes up after the services in docker-compose; keep trying."""
    for attempt in range(1, attempts + 1):
        try:
            await start()
            return
        except Exception as exc:  # noqa: BLE001 - aiokafka raises a variety of errors here
            if attempt == attempts:
                raise
            log.warning("kafka %s not ready (%s), retry %d/%d", what, exc, attempt, attempts)
            await asyncio.sleep(2)
