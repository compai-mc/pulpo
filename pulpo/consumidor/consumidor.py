import asyncio
from aiokafka import AIOKafkaConsumer
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")

class KafkaEventConsumer:
    def __init__(self, topic: str, callback: callable, id_grupo: str = "global", max_concurrent: int = 5):
        self.topic = topic
        self.callback = callback
        self.id_grupo = id_grupo
        self.consumer = None
        self.consumer_task = None
        self.workers = []
        self.max_concurrent = max_concurrent
        self.queue = asyncio.Queue()

    async def start(self, broker=KAFKA_BROKER):
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=broker,
            group_id=self.id_grupo,
            session_timeout_ms=120000,       # 120s margen
            heartbeat_interval_ms=30000,     # 30s
            max_poll_interval_ms=600000,     # 10min
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
        except Exception as e:
            print(f"[!] Error en el loop de consumo: {e}")

    async def _worker_loop(self):
        """Procesa mensajes en paralelo desde la queue"""
        while True:
            try:
                message = await self.queue.get()
                try:
                    await self.callback(message)
                    await self.consumer.commit()  # commit seguro después de procesar
                except Exception as e:
                    print(f"[!] Error procesando mensaje {message.offset}: {e}")
                finally:
                    self.queue.task_done()
            except asyncio.CancelledError:
                break
