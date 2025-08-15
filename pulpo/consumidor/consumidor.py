import asyncio
from aiokafka import AIOKafkaConsumer, ConsumerStoppedError
import os

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from kafka.structs import TopicPartition

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

    async def start(self, broker=KAFKA_BROKER):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=broker,
            group_id=self.id_grupo,
            session_timeout_ms=120000,   # 120s margen de heartbeat
            heartbeat_interval_ms=30000, # cada 30s
            max_poll_interval_ms=600000, # 10 min para callbacks largos
            request_timeout_ms=90000,
            retry_backoff_ms=5000,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            isolation_level="read_committed"
        )
        await self.consumer.start()

        # Start consumer loop
        self.consumer_task = asyncio.create_task(self._consume_loop())

        # Start worker tasks
        for _ in range(self.max_concurrent):
            worker = asyncio.create_task(self._worker_loop())
            self.workers.append(worker)

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
        """Loop principal de consumo que alimenta la queue interna"""
        try:
            async for message in self.consumer:
                await self.queue.put(message)
        except asyncio.CancelledError:
            pass
        except ConsumerStoppedError:
            pass
        except Exception as e:
            print(f"[!] Error en el loop de consumo: {e}")

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
                            await self.consumer.commit()
                            break
                        except Exception as e:
                            print(f"[!] Commit fallo (intento {attempt}): {e}")
                            if attempt == self.max_commit_retries:
                                print(f"[!] Commit final fallido para offset {message.offset}")
                            else:
                                await asyncio.sleep(2 ** attempt)  # backoff exponencial
                except asyncio.TimeoutError:
                    print(f"[!] Timeout procesando mensaje offset {message.offset}")
                except Exception as e:
                    print(f"[!] Error procesando mensaje offset {message.offset}: {e}")
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break

    def leer_offset(self, offset: int, partition: int = 0, timeout_ms: int = 5000):
        """
        Versión síncrona para leer un offset específico.
        """
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                group_id=self.id_grupo,
                enable_auto_commit=False,
                auto_offset_reset="none",
                consumer_timeout_ms=timeout_ms
            )

            # Asignar partición y buscar offset
            tp = TopicPartition(self.topic, partition)
            consumer.assign([tp])
            consumer.seek(tp, offset)

            # Leer mensaje (bloqueante hasta timeout_ms)
            for msg in consumer:
                if msg.offset == offset:
                    return msg
                elif msg.offset > offset:
                    break  # Pasamos el offset buscado

            return None

        except NoBrokersAvailable:
            raise RuntimeError(f"No se pudo conectar al broker: {KAFKA_BROKER}")
        except KafkaTimeoutError:
            raise RuntimeError(f"Timeout al leer offset {offset}")
        finally:
            if consumer:
                consumer.close()

