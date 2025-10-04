import asyncio
from aiokafka import AIOKafkaConsumer, ConsumerStoppedError
from aiokafka.structs import TopicPartition, OffsetAndMetadata
from kafka import KafkaConsumer, TopicPartition
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError
import os
from pulpo.logueador import log
import uuid

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")

class KafkaEventConsumer:
    def __init__(self, topic: str, callback: callable, id_grupo: str = "global",
                 max_concurrent: int = 5, process_timeout: int = 60, max_commit_retries: int = 3):
        """
        :param topic: Tópico de Kafka.
        :param callback: Función async que procesa cada mensaje.
        :param id_grupo: Consumer group.
        :param max_concurrent: Máximo de mensajes procesados en paralelo.
        :param process_timeout: Timeout (s) para cada callback.
        :param max_commit_retries: Reintentos de commit ante fallo.
        """
        self.topic = topic
        self.callback = callback
        self.id_grupo = id_grupo
        self.max_concurrent = max_concurrent
        self.process_timeout = process_timeout
        self.max_commit_retries = max_commit_retries

        self.consumer = None
        self.consumer_task = None
        self.workers = []
        self.queue = asyncio.Queue()


    async def start(
        self,
        broker: str = KAFKA_BROKER,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        auto_commit_interval_ms: int = 5000,
        group_id: str = None,
        group_instance_id: str = None,
        max_partition_fetch_bytes: int = 1048576,  # 1 MB por partición
        fetch_max_bytes: int = 52428800,           # 50 MB total
        fetch_min_bytes: int = 1,
        fetch_max_wait_ms: int = 500,
        session_timeout_ms: int = 30000,           # 30s
        heartbeat_interval_ms: int = 10000,        # 10s
        max_poll_interval_ms: int = 300000,        # 5 minutos
        request_timeout_ms: int = 40000,           # 40s
        retry_backoff_ms: int = 100,               # espera mínima entre reintentos
        connections_max_idle_ms: int = 540000,     # 9 minutos
        isolation_level: str = "read_committed",
        metadata_max_age_ms: int = 300000,         # 5 min para refrescar metadatos
        check_crcs: bool = True,
        api_version: str | None = None,
        security_protocol: str = "PLAINTEXT",
        ssl_context=None,
        sasl_mechanism: str | None = None,
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        client_id: str = None,
        max_concurrent: int = 5,                   # número de workers concurrentes
    ):
        """
        Inicia el consumidor Kafka con configuración avanzada.
        """

        self.max_concurrent = max_concurrent
        self.workers = []
        self.id_grupo = group_id or self.id_grupo or "grupo_default"
        self.group_instance_id = group_instance_id or f"{self.id_grupo}_{uuid.uuid4()}"

        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=broker,
            group_id=self.id_grupo,
            group_instance_id=self.group_instance_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            auto_commit_interval_ms=auto_commit_interval_ms,
            max_partition_fetch_bytes=max_partition_fetch_bytes,
            fetch_max_bytes=fetch_max_bytes,
            fetch_min_bytes=fetch_min_bytes,
            fetch_max_wait_ms=fetch_max_wait_ms,
            session_timeout_ms=session_timeout_ms,
            heartbeat_interval_ms=heartbeat_interval_ms,
            max_poll_interval_ms=max_poll_interval_ms,
            request_timeout_ms=request_timeout_ms,
            retry_backoff_ms=retry_backoff_ms,
            connections_max_idle_ms=connections_max_idle_ms,
            isolation_level=isolation_level,
            metadata_max_age_ms=metadata_max_age_ms,
            check_crcs=check_crcs,
            api_version=api_version,
            security_protocol=security_protocol,
            ssl_context=ssl_context,
            sasl_mechanism=sasl_mechanism,
            sasl_plain_username=sasl_plain_username,
            sasl_plain_password=sasl_plain_password,
            client_id=client_id or f"pv_{self.id_grupo}_{uuid.uuid4()}",
        )

        # 🔹 Arrancar el consumidor Kafka
        await self.consumer.start()

        # 🔹 Esperar asignación de particiones con timeout
        try:
            await asyncio.wait_for(self.consumer._subscription.wait_for_assignment(), timeout=10)
            print(f"✅ Particiones asignadas a {self.id_grupo}")
        except asyncio.TimeoutError:
            print(f"⚠️ Timeout esperando asignación de particiones para {self.id_grupo}")

        # 🔹 Lanzar el bucle de consumo
        self.consumer_task = asyncio.create_task(self._consume_loop())

        # 🔹 Crear workers concurrentes
        for _ in range(self.max_concurrent):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)

        print(f"🚀 Consumer '{self.id_grupo}' iniciado con instancia '{self.group_instance_id}'")


    async def stop(self):
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                pass
        for worker in self.workers:
            worker.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)
        if self.consumer:
            await self.consumer.stop()

    async def _consume_loop(self):
        while True:
            try:
                async for message in self.consumer:
                    await self.queue.put(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"[!] Error en el loop de consumo: {e}. Reintentando en 10s...")
                await asyncio.sleep(10)
                await self._reconnect()

    async def _reconnect(self):
        if self.consumer:
            await self.consumer.stop()
        await self.start()

    async def _worker_loop(self):
        """Procesa mensajes en paralelo desde la queue con timeout y commit con reintentos"""
        while True:
            try:
                message = await self.queue.get()
                try:
                    # Ejecuta el callback con timeout
                    await asyncio.wait_for(self.callback(message), timeout=self.process_timeout)

                    # Commit con reintentos
                    for attempt in range(1, self.max_commit_retries + 1):
                        try:
                            tp = TopicPartition(message.topic, message.partition)
                            offsets = {tp: OffsetAndMetadata(message.offset + 1, "procesado")}
                            await self.consumer.commit(offsets=offsets)
                            break
                        except Exception as e:
                            log.error(f"[!] Commit fallo (intento {attempt}): {e}")
                            if attempt == self.max_commit_retries:
                                log.error(f"[!] Commit final fallido para offset {message.offset}")
                            else:
                                await asyncio.sleep(2 ** attempt)  # backoff exponencial
                except asyncio.TimeoutError:
                    log.error(f"[!] Timeout procesando mensaje offset {message.offset}")
                except Exception as e:
                    log.error(f"[!] Error procesando mensaje offset {message.offset} en el worker: {e}")
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break


    def leer_offset(self, offset: int, partition: int = 0, timeout_ms: int = 5000):
        """
        Versión síncrona para leer un offset específico.
        """
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                group_id=self.id_grupo,
                enable_auto_commit=False,
                auto_offset_reset="none",
                consumer_timeout_ms=timeout_ms
            )

            tp = TopicPartition(self.topic, partition)

            # Validar que la partición existe
            partitions = consumer.partitions_for_topic(self.topic)
            if partitions is None:
                raise RuntimeError(f"El topic '{self.topic}' no existe en el broker {KAFKA_BROKER}")
            if partition not in partitions:
                raise RuntimeError(f"La partición {partition} no existe en el topic '{self.topic}'")

            consumer.assign([tp])
            consumer.seek(tp, offset)

            for msg in consumer:
                if msg.offset == offset:
                    return msg
                elif msg.offset > offset:
                    break  

            return None

        except NoBrokersAvailable:
            raise RuntimeError(f"No se pudo conectar al broker: {KAFKA_BROKER}")
        except KafkaTimeoutError:
            raise RuntimeError(f"Timeout al leer offset {offset}")
        except KafkaError as e:
            raise RuntimeError(f"Error de Kafka: {e}")
        finally:
            if consumer is not None:
                consumer.close()