# VERSION ARREGLADA Y ROBUSTA
# - UN SOLO hilo hace poll()
# - start() crea el consumer antes del hilo (sin race)
# - poll() compatible con KafkaConsumer
# - Reconexión con backoff
# - Commit seguro
# - DLQ persistente

import threading
import json
import time
import uuid
from typing import Callable, Optional, Dict
from datetime import datetime
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer, TopicPartition, OffsetAndMetadata
from kafka.errors import (
    KafkaError,
    CommitFailedError,
)

from pulpo.util.util import require_env
from pulpo.logueador import log

KAFKA_BROKER = require_env("KAFKA_BROKER")
MAX_RETRY_ATTEMPTS = int(require_env("KAFKA_MAX_RETRIES"))
RETRY_BACKOFF_MS = int(require_env("KAFKA_RETRY_BACKOFF_MS"))
COMMIT_INTERVAL_MS = int(require_env("KAFKA_COMMIT_INTERVAL_MS"))


class KafkaEventConsumer:
    def __init__(
        self,
        topic: str,
        callback: Callable,
        bootstrap_servers: str = KAFKA_BROKER,
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
        max_retries: int = MAX_RETRY_ATTEMPTS,
        retry_backoff_ms: int = RETRY_BACKOFF_MS,
        enable_dlq: bool = False,
        dlq_topic: Optional[str] = None,
        batch_commit: bool = True,
        commit_interval_ms: int = COMMIT_INTERVAL_MS,
        max_poll_interval_ms: int = 1800000,
        pass_raw_message: bool = False,
    ):
        self.topic = topic
        self.callback = callback
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id or f"consumer_{topic}_{uuid.uuid4().hex[:8]}"
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.enable_dlq = enable_dlq
        self.dlq_topic = dlq_topic or f"{topic}.dlq"
        self.batch_commit = batch_commit
        self.commit_interval_ms = commit_interval_ms
        self.max_poll_interval_ms = max_poll_interval_ms
        self.pass_raw_message = pass_raw_message

        self._consumer: Optional[KafkaConsumer] = None
        self._dlq_producer: Optional[KafkaProducer] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

        self._pending_offsets: Dict[TopicPartition, int] = {}
        self._last_commit_time = time.time()

        self.metrics = {
            "messages_processed": 0,
            "messages_failed": 0,
            "messages_retried": 0,
            "messages_sent_to_dlq": 0,
            "poll_errors": 0,
            "processing_errors": defaultdict(int),
        }

        log.info(f"[{self.group_id}] Consumidor inicializado para topic '{self.topic}'")

    # ------------------------------------------------------------------
    # CREACIÓN
    # ------------------------------------------------------------------

    def _create_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            max_poll_interval_ms=self.max_poll_interval_ms,
            max_poll_records=1,
            value_deserializer=lambda m: m,
            key_deserializer=lambda k: k.decode() if k else None,
        )

    def _create_dlq_producer(self):
        if not self._dlq_producer:
            self._dlq_producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )

    # ------------------------------------------------------------------
    # PROCESAMIENTO
    # ------------------------------------------------------------------

    def _deserialize(self, raw: bytes):
        try:
            return json.loads(raw.decode())
        except Exception:
            return raw.decode(errors="ignore")

    def _process_with_retry(self, msg) -> bool:
        data = msg if self.pass_raw_message else self._deserialize(msg.value)

        for attempt in range(1, self.max_retries + 1):
            try:
                self.callback(data)
                self.metrics["messages_processed"] += 1
                if attempt > 1:
                    self.metrics["messages_retried"] += 1
                return True

            except Exception as e:
                self.metrics["processing_errors"][type(e).__name__] += 1

                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff_ms / 1000)
                else:
                    self.metrics["messages_failed"] += 1
                    if self.enable_dlq:
                        self._send_to_dlq(msg, data, e)
                    return False

    def _send_to_dlq(self, msg, data, error):
        try:
            self._create_dlq_producer()
            self._dlq_producer.send(
                self.dlq_topic,
                value={
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "data": data,
                    "error": str(error),
                    "ts": datetime.utcnow().isoformat(),
                },
            )
            self._dlq_producer.flush()
            self.metrics["messages_sent_to_dlq"] += 1
        except Exception as e:
            log.error(f"DLQ error: {e}")

    # ------------------------------------------------------------------
    # COMMITS
    # ------------------------------------------------------------------

    def _should_commit(self):
        if not self.batch_commit:
            return True
        return (time.time() - self._last_commit_time) * 1000 >= self.commit_interval_ms

    def _commit_offsets(self):
        with self._lock:
            consumer = self._consumer

        if not consumer or getattr(consumer, "_closed", True):
            return

        offsets = {
            tp: OffsetAndMetadata(offset + 1, None)
            for tp, offset in self._pending_offsets.items()
        }

        try:
            consumer.commit(offsets=offsets)
            self._pending_offsets.clear()
            self._last_commit_time = time.time()
        except CommitFailedError:
            log.warning("Rebalance detectado, limpiando offsets pendientes")
            self._pending_offsets.clear()

    # ------------------------------------------------------------------
    # LOOP PRINCIPAL
    # ------------------------------------------------------------------

    def _consume_loop(self):
        reconnect_attempts = 0
        max_reconnects = 5

        while self._running:
            try:
                with self._lock:
                    consumer = self._consumer

                if not consumer:
                    time.sleep(1)
                    continue

                records = consumer.poll(timeout_ms=1000)
                if not records:
                    continue

                reconnect_attempts = 0

                for tp, msgs in records.items():
                    for msg in msgs:
                        ok = self._process_with_retry(msg)
                        self._pending_offsets[tp] = msg.offset

                        if ok and self._should_commit():
                            self._commit_offsets()

            except KafkaError as e:
                reconnect_attempts += 1
                log.error(f"Kafka error: {e} (retry {reconnect_attempts})")

                if reconnect_attempts > max_reconnects:
                    log.critical("🛑 Máximo de reintentos Kafka alcanzado")
                    break

                backoff = min(2 ** reconnect_attempts, 30)
                time.sleep(backoff)

                with self._lock:
                    try:
                        if self._consumer:
                            self._consumer.close()
                    except Exception:
                        pass
                    self._consumer = self._create_consumer()

            except Exception as e:
                log.error(f"Error inesperado: {e}", exc_info=True)
                time.sleep(1)

        self._cleanup()

    # ------------------------------------------------------------------
    # POLL COMPATIBLE
    # ------------------------------------------------------------------

    def poll(self, timeout_ms: int = 1000):
        """
        Wrapper compatible con KafkaConsumer.poll()
        Devuelve {TopicPartition: [ConsumerRecord, ...]}
        """
        with self._lock:
            consumer = self._consumer

        if not consumer or getattr(consumer, "_closed", True):
            return {}

        try:
            return consumer.poll(timeout_ms=timeout_ms)
        except Exception as e:
            log.error(f"❌ Error en poll(): {e}", exc_info=True)
            return {}
        
    # ------------------------------------------------------------------
    # CONTROL
    # ------------------------------------------------------------------

    def start(self):
        if self._running:
            return

        with self._lock:
            self._consumer = self._create_consumer()

        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop,
            name=f"kafka-consumer-{self.topic}",
            daemon=True,
        )
        self._thread.start()

    def stop(self):
        self._running = False
        if self._thread:
            self._thread.join(10)

    def _cleanup(self):
        if self._pending_offsets:
            self._commit_offsets()
        if self._consumer:
            self._consumer.close()
        if self._dlq_producer:
            self._dlq_producer.close()

    def get_metrics(self):
        return dict(self.metrics)
    
    def is_running(self) -> bool:
        """
        Indica si el consumidor está activo.
        API pública usada por gestor_tareas.
        """
        return bool(self._running)
