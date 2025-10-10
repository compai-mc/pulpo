import json
import os
import traceback
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from pulpo.logueador import log

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")


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
        admin_client = AdminClient({"bootstrap.servers": kafka_broker})
        cluster_metadata = admin_client.list_topics(timeout=5)

        if topic_name in cluster_metadata.topics:
            log.info(f"El tópico '{topic_name}' ya existe.")
            return False  # ya existía

        new_topic = NewTopic(topic_name, num_particiones, replication_factor)
        fs = admin_client.create_topics([new_topic])

        # Esperar confirmación
        fs[topic_name].result()
        log.info(f"Tópico '{topic_name}' creado correctamente.")
        return True

    except Exception as e:
        log.error(f"Error al crear el tópico '{topic_name}': {e}")
        return False


class KafkaEventPublisher:
    def __init__(self, broker: str = KAFKA_BROKER):
        self.broker = broker
        self.producer: Producer | None = None

    def start(self):
        """Inicia el productor de Kafka (síncrono)."""
        try:
            log.info(f"[KafkaEventPublisher] 🔄 Conectando a {self.broker}")
            self.producer = Producer({
                "bootstrap.servers": self.broker,
                "acks": "all",
                "enable.idempotence": True,
                "request.timeout.ms": 10000,
                "message.timeout.ms": 15000
            })
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
                self.producer = None

    def publish(self, topic: str, message: dict):
        """
        Publica un mensaje en un tópico. Si el tópico no existe, lo crea.
        """
        if not self.producer:
            raise RuntimeError("❌ El productor no está iniciado. Llama a start() antes de publicar.")

        # Asegurar que el tópico existe
        crear_topico(self.broker, topic)

        message_str = json.dumps(message)

        def delivery_report(err, msg):
            if err is not None:
                log.error(f"❌ Error publicando en '{topic}': {err}")
            else:
                log.debug(f"📤 Mensaje publicado en '{msg.topic()}' [part {msg.partition()}] offset {msg.offset()}")

        try:
            self.producer.produce(
                topic,
                value=message_str.encode("utf-8"),
                callback=delivery_report
            )
            self.producer.flush()  # Espera a que se confirme el envío
        except Exception as e:
            log.error(f"Error publicando en tópico '{topic}': {e}", exc_info=True)

    def publish_commit(self, topic: str, message: dict):
        """
        Simula un envío 'transaccional' (bloqueante + confirmación),
        aunque confluent_kafka requiere configuración avanzada para transacciones reales.
        """
        if not self.producer:
            raise RuntimeError("El productor no está iniciado.")

        try:
            message_str = json.dumps(message)
            self.producer.produce(
                topic,
                value=message_str.encode("utf-8")
            )
            self.producer.flush()
            log.info(f"✅ Mensaje 'transaccional' publicado en '{topic}'.")
        except Exception as e:
            log.error(f"❌ Error en publicación transaccional: {e}")


# ========================================================
# 🔧 Ejemplo de uso
# ========================================================

if __name__ == "__main__":
    publisher = KafkaEventPublisher()
    publisher.start()

    mensaje = {"id": 1, "texto": "Hola Kafka síncrono"}
    publisher.publish("prueba_topic", mensaje)

    publisher.stop()
