import json
import os
import traceback
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

from pulpo.logueador import log
from ..util.util import require_env

KAFKA_BROKER = require_env("KAFKA_BROKER")

def crear_topico(kafka_broker: str, topic_name: str, num_particiones: int = 1, replication_factor: int = 1) -> bool:
    """
    Verifica si un tópico existe en Kafka. Si no existe, intenta crearlo.

    :param kafka_broker: Dirección del broker (ej: 'localhost:9092')
    :param topic_name: Nombre del tópico
    :param num_particiones: Número de particiones (por defecto 1)
    :param replication_factor: Factor de replicación (por defecto 1)
    :return: True si se creó, False si ya existía o hubo error
    """
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=kafka_broker)
        topics_existentes = admin_client.list_topics()

        if topic_name in topics_existentes:
            log.info(f"El tópico '{topic_name}' ya existe.")
            return False

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_particiones,
            replication_factor=replication_factor
        )

        admin_client.create_topics([new_topic])
        log.info(f"Tópico '{topic_name}' creado correctamente.")
        return True

    except TopicAlreadyExistsError:
        log.info(f"El tópico '{topic_name}' ya existía.")
        return False
    except NoBrokersAvailable:
        log.error(f"No se pudo conectar al broker '{kafka_broker}'. ¿Está corriendo Kafka?")
        return False
    except Exception as e:
        log.error(f"Error al crear el tópico '{topic_name}': {e}", exc_info=True)
        return False
    finally:
        try:
            admin_client.close()
        except Exception:
            pass


class KafkaEventPublisher:
    def __init__(self, broker: str = KAFKA_BROKER):
        self.broker = broker
        self.producer: KafkaProducer | None = None

    def start(self):
        """Inicia el productor de Kafka (síncrono)."""
        try:
            log.info(f"[KafkaEventPublisher] 🔄 Conectando a {self.broker}")
            self.producer = KafkaProducer(
                bootstrap_servers=self.broker,
                acks="all",
                retries=3,
                linger_ms=10,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            log.info("[KafkaEventPublisher] ✅ Productor conectado correctamente.")
        except Exception as e:
            tb = traceback.format_exc()
            log.error(f"[KafkaEventPublisher] ❌ Error al iniciar productor: {e}\n{tb}")
            self.producer = None

    def stop(self):
        """Detiene el productor, vaciando el buffer."""
        if self.producer:
            try:
                self.producer.flush()
                log.info("[KafkaEventPublisher] 🔌 Productor detenido (flush completado).")
            except Exception as e:
                log.error(f"Error al detener productor: {e}")
            finally:
                self.producer.close()
                self.producer = None

    def publish(self, topic: str, message: dict):
        """
        Publica un mensaje en un tópico. Si el tópico no existe, lo crea.
        """
        if not self.producer:
            raise RuntimeError("❌ El productor no está iniciado. Llama a start() antes de publicar.")

        crear_topico(self.broker, topic)

        try:
            future = self.producer.send(topic, message)
            result = future.get(timeout=10)
            log.debug(f"📤 Mensaje publicado en '{result.topic}' [part {result.partition}] offset {result.offset}")
        except Exception as e:
            log.error(f"❌ Error publicando en tópico '{topic}': {e}", exc_info=True)

    def publish_commit(self, topic: str, message: dict):
        """
        Simula un envío 'transaccional' (bloqueante + confirmación).
        """
        if not self.producer:
            raise RuntimeError("El productor no está iniciado.")

        try:
            future = self.producer.send(topic, message)
            result = future.get(timeout=10)
            log.info(f"✅ Mensaje 'transaccional' publicado en '{result.topic}' offset {result.offset}")
        except Exception as e:
            log.error(f"❌ Error en publicación transaccional: {e}", exc_info=True)


# ========================================================
# 🔧 Ejemplo de uso
# ========================================================

if __name__ == "__main__":
    publisher = KafkaEventPublisher()
    publisher.start()

    mensaje = {"id": 1, "texto": "Hola Kafka con kafka-python"}
    publisher.publish("prueba_topic", mensaje)

    publisher.stop()
