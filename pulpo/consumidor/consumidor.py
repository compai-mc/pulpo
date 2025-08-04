# mi_paquete/consumer.py
from aiokafka import AIOKafkaConsumer
from aiokafka import TopicPartition
import asyncio
import os

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from kafka.structs import TopicPartition

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")

class KafkaEventConsumer:
    def __init__(self, topic: str, callback: callable, id_grupo: str = "global"):
        """
        Constructor para el consumidor.
        :param topic: Tópico de Kafka que se desea consumir.
        :param callback: Función que se llamará cuando un mensaje sea recibido.
        """
        self.consumer = None
        self.topic = topic
        self.callback = callback  # Guardamos el callback
        self.consumer_task = None 
        self.id_grupo = id_grupo

    async def start(self, broker = KAFKA_BROKER):
        """Inicia el consumidor de Kafka."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=broker,  # Asegúrate de que sea IP/host accesible desde el contenedor
            group_id=self.id_grupo,
            # 🔹 Timeouts ajustados para entornos inestables:
            session_timeout_ms=30000,           # 30 segundos (default: 10s)
            heartbeat_interval_ms=10000,        # 10 segundos (default: 3s)
            max_poll_interval_ms=300000,        # 5 minutos (default: 5m)
            request_timeout_ms=40000,           # 40 segundos (default: 40s)
            retry_backoff_ms=2000,              # 2 segundos entre reintentos (default: 100ms)
            auto_offset_reset="earliest",       # Lee desde el inicio si no hay offset
            isolation_level="read_committed",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000
            
        )

        await self.consumer.start()

        # Crea la tarea del consumidor en paralelo
        if self.consumer_task is None:
            self.consumer_task = asyncio.create_task(self.consume())

    async def stop(self):
        """Detiene el consumidor de Kafka."""

        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task  # espera que termine
            except asyncio.CancelledError:
                print("La tarea de consumo fue cancelada correctamente.")
            self.consumer_task = None

        if self.consumer:
            await self.consumer.stop()

    async def consume(self):
        """Consume los mensajes de Kafka y ejecuta el callback."""
        async for message in self.consumer:
            print(f"Mensaje recibido en el tópico {self.topic}: {message.value.decode('utf-8')}")
            # Consumimos el mensaje
            await self.consumer.commit()
            # Llamamos al callback con el mensaje recibido
            await self.callback(message)


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
            for msg in self.consumer:
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

