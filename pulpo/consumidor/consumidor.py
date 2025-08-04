# mi_paquete/consumer.py
from aiokafka import AIOKafkaConsumer
from aiokafka import TopicPartition
import asyncio
import os

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


    async def leer_offset(self, offset: int, partition: int = 0, timeout_ms: int = 5000):
        """
        Lee el mensaje en el offset indicado sin consumirlo (sin commit).
        :param offset: Offset que se desea leer.
        :param partition: Partición del tópico (por defecto 0).
        :param timeout_ms: Tiempo máximo de espera para el mensaje.
        :return: Mensaje si existe, None si no hay mensaje.
        """
        if not self.consumer:
            raise RuntimeError("El consumidor no está iniciado. Llama primero a start().")

        # Asignar manualmente solo esa partición
        tp = TopicPartition(self.topic, partition)
        await self.consumer.assign([tp])

        # Posicionarse en el offset deseado
        await self.consumer.seek(tp, offset)

        # Intentar obtener el mensaje sin avanzar el offset global
        result = await self.consumer.getmany(timeout_ms=timeout_ms)

        # Extraer el mensaje en ese offset, si existe
        for records in result.values():
            for msg in records:
                if msg.offset == offset:
                    return msg

        return None  # No encontrado            
