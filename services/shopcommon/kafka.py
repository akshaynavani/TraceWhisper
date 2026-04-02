"""JSON event producer/consumer on top of aiokafka."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Awaitable, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

log = logging.getLogger(__name__)

Handler = Callable[[dict[str, Any], ConsumerRecord], Awaitable[None]]


class EventProducer:
    def __init__(self, bootstrap: str, client_id: str):
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
        headers: list[tuple[str, bytes]] = []
        await self._producer.send_and_wait(topic, value=value, key=key, headers=headers)


class EventConsumer:
    def __init__(self, bootstrap: str, topic: str, group_id: str, handler: Handler):
        self.topic = topic
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
            try:
                await self._handler(record.value, record)
            except Exception:  # handler decides on DLQ; never kill the loop
                log.exception("unhandled error processing %s@%s", record.topic, record.offset)
            await self._consumer.commit()


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
