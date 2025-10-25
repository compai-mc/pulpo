import asyncio
import os
from aiokafka import AIOKafkaConsumer, TopicPartition

KAFKA_BROKER = os.getenv("KAFKA_BROKER")

class KafkaTopicMonitorAsync:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers

    async def estado_topico(self, topic: str, group_id: str = "global") -> dict:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False
        )
        await consumer.start()
        try:
            partitions = consumer.partitions_for_topic(topic)
            if partitions is None:
                raise Exception(f"Tópico '{topic}' no existe")

            lag_total = 0
            lag_por_particion = {}

            for partition in partitions:
                tp = TopicPartition(topic, partition)
                # Asignar partición
                consumer.assign([tp])
                # Mover cursor al final para obtener el último offset
                await consumer.seek_to_end(tp)
                last_offset = await consumer.position(tp) - 1

                committed = await consumer.committed(tp)
                if committed is None:
                    committed = 0

                lag = max(last_offset - committed, 0)
                lag_por_particion[partition] = {
                    "last_offset": last_offset,
                    "committed": committed,
                    "lag": lag
                }
                lag_total += lag

            return {"lag_total": lag_total, "lag_por_particion": lag_por_particion}
        finally:
            await consumer.stop()

# Ejemplo de uso async
async def main():
    monitor = KafkaTopicMonitorAsync(bootstrap_servers=KAFKA_BROKER)
    topic = "compai_procesos"
    lag = await monitor.estado_topico(topic)
    print(f"Mensajes pendientes en '{topic}': {lag}")

if __name__ == "__main__":
    asyncio.run(main())
