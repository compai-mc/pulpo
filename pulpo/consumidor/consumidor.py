import asyncio
import os
from aiokafka import AIOKafkaConsumer
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from kafka.structs import TopicPartition

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")


class KafkaEventConsumer:
    def __init__(self, topic: str, callback: callable, id_grupo: str = "global", max_concurrent: int = 5):
        """
        :param topic: Tópico de Kafka a consumir.
        :param callback: Función async que recibe cada mensaje.
        :param id_grupo: ID del consumer group (estable para evitar relecturas).
        :param max_concurrent: Máximo de mensajes procesándose en paralelo.
        """
        self.consumer = None
        self.topic = topic
        self.callback = callback
        self.consumer_task = None
        self.id_grupo = id_grupo
        self.semaphore = asyncio.Semaphore(max_concurrent)  # Limitar concurrencia

    async def start(self, broker=KAFKA_BROKER):
        """Inicia el consumidor de Kafka."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=broker,
            group_id=self.id_grupo,
            session_timeout_ms=45000,
            heartbeat_interval_ms=15000,
            max_poll_interval_ms=600000,  # 10 min para callbacks pesados
            request_timeout_ms=70000,
            retry_backoff_ms=2000,
            auto_offset_reset="latest",   # Arranca desde lo más reciente si no hay offset
            isolation_level="read_committed",
            enable_auto_commit=False      # Commit manual
        )

        await self.consumer.start()

        if self.consumer_task is None:
            self.consumer_task = asyncio.create_task(self.consume_loop())

    async def stop(self):
        """Detiene el consumidor de Kafka."""
        if self.consumer_task:
            self.consumer_task.cancel()
            try:
                await self.consumer_task
            except asyncio.CancelledError:
                print("Tarea de consumo cancelada correctamente.")
            self.consumer_task = None

        if self.consumer:
            await self.consumer.stop()

    async def consume_loop(self):
        """Bucle principal de consumo."""
        try:
            async for message in self.consumer:
                # No bloquear el loop principal: lanzar procesamiento en paralelo
                asyncio.create_task(self.process_message(message))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"Error en bucle de consumo: {e}")

    async def process_message(self, message):
        """Procesa un mensaje respetando el límite de concurrencia."""
        async with self.semaphore:
            try:
                await self.callback(message)  # Procesar mensaje
                await self.consumer.commit()  # Confirmar offset
            except Exception as e:
                print(f"Error procesando mensaje {message.offset} en {self.topic}: {e}")

    def leer_offset(self, offset: int, partition: int = 0, timeout_ms: int = 5000):
        """Lectura síncrona de un offset específico."""
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
        finally:
            if consumer:
                consumer.close()
