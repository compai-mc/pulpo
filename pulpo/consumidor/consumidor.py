import threading
import time
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError, KafkaError
import os
import uuid
from pulpo.logueador import log

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "alcazar:29092")


class KafkaEventConsumer:
    def __init__(
        self,
        topic: str,
        callback: callable,
        group_id: str = "global",
        max_concurrent: int = 5,
        process_timeout: int = 60,
        max_commit_retries: int = 3,
        parallel: bool = False,  
    ):
        """
        :param topic: Tópico de Kafka.
        :param callback: Función síncrona que procesa cada mensaje.
        :param group_id: Consumer group.
        :param max_concurrent: Número máximo de hilos procesando mensajes (solo si parallel=True).
        :param process_timeout: Timeout (s) para procesar cada mensaje.
        :param max_commit_retries: Reintentos de commit ante fallo.
        :param parallel: Si False, procesará los mensajes uno a uno.
        """
        self.topic = topic
        self.callback = callback
        self.group_id = group_id
        self.max_concurrent = max_concurrent
        self.process_timeout = process_timeout
        self.max_commit_retries = max_commit_retries
        self.parallel = parallel

        self.consumer = None
        self.threads = []
        self.running = False
        self._lock = threading.Lock()

    # -------------------------------------------------------------------------
    # INICIO Y DETENCIÓN
    # -------------------------------------------------------------------------
    def start(
        self,
        broker: str = KAFKA_BROKER,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = False,
        group_instance_id: str = None,
        session_timeout_ms: int = 90000,
        heartbeat_interval_ms: int = 10000,
        max_poll_interval_ms: int = 900000,
        request_timeout_ms: int = 100000,
        retry_backoff_ms: int = 100,
        connections_max_idle_ms: int = 540000,
        isolation_level: str = "read_committed",
        metadata_max_age_ms: int = 300000,
        check_crcs: bool = True,
        security_protocol: str = "PLAINTEXT",
        ssl_context=None,
        sasl_mechanism: str | None = None,
        sasl_plain_username: str | None = None,
        sasl_plain_password: str | None = None,
        client_id: str = None,
    ):
        """Inicia el consumidor Kafka con configuración avanzada."""

        self.group_instance_id = group_instance_id or f"{self.group_id}_{uuid.uuid4()}"
        self.running = True

        try:
            self.consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=broker,
                group_id=self.group_id,
                enable_auto_commit=enable_auto_commit,
                auto_offset_reset=auto_offset_reset,
                client_id=client_id or f"pv_{self.group_id}_{uuid.uuid4()}",
                request_timeout_ms=request_timeout_ms,
                session_timeout_ms=session_timeout_ms,
                heartbeat_interval_ms=heartbeat_interval_ms,
                max_poll_interval_ms=max_poll_interval_ms,
                connections_max_idle_ms=connections_max_idle_ms,
                security_protocol=security_protocol,
                ssl_context=ssl_context,
                sasl_mechanism=sasl_mechanism,
                sasl_plain_username=sasl_plain_username,
                sasl_plain_password=sasl_plain_password,
                check_crcs=check_crcs,
                isolation_level=isolation_level,
                metadata_max_age_ms=metadata_max_age_ms,
                retry_backoff_ms=retry_backoff_ms,
            )

            # Esperar asignación de particiones
            parts = self.consumer.partitions_for_topic(self.topic)
            if parts:
                log.info(f"✅ Particiones asignadas: {parts}")
            else:
                log.warning(f"⚠️ No se encontraron particiones para el topic {self.topic}")

            if self.parallel:
                # Modo paralelo
                for i in range(self.max_concurrent):
                    t = threading.Thread(target=self._consume_loop, name=f"worker-{i}", daemon=True)
                    t.start()
                    self.threads.append(t)
                log.info(f"🚀 Consumer '{self.group_id}' iniciado en modo paralelo ({self.max_concurrent} hilos)")
            else:
                # Modo secuencial
                t = threading.Thread(target=self._consume_loop, name="worker-secuencial", daemon=True)
                t.start()
                self.threads.append(t)
                log.info(f"🚀 Consumer '{self.group_id}' iniciado en modo secuencial (uno a uno)")

        except NoBrokersAvailable:
            raise RuntimeError(f"No se pudo conectar al broker: {broker}")
        except Exception as e:
            raise RuntimeError(f"Error iniciando el consumidor: {e}")

    def stop(self):
        """Detiene el consumidor y todos los hilos."""
        self.running = False
        if self.consumer:
            try:
                self.consumer.close()
                log.info(f"🛑 Consumer '{self.group_id}' detenido correctamente")
            except Exception as e:
                log.warning(f"[{self.group_id}] Error al detener el consumidor: {e}")

        for t in self.threads:
            t.join(timeout=2)

    # -------------------------------------------------------------------------
    # RECONEXIÓN
    # -------------------------------------------------------------------------
    def _reconnect(self, retries=3):
        """Intento de reconexión síncrona."""
        with self._lock:
            for attempt in range(1, retries + 1):
                try:
                    if self.consumer:
                        self.consumer.close()
                    log.warning(f"[{self.group_id}] Reintentando conexión ({attempt}/{retries})...")
                    time.sleep(5 * attempt)
                    self.start()
                    return
                except Exception as e:
                    log.error(f"[{self.group_id}] Error al reconectar: {e}")
            log.critical(f"[{self.group_id}] Fallo reconectando tras {retries} intentos.")

    # -------------------------------------------------------------------------
    # BUCLE PRINCIPAL DE CONSUMO
    # -------------------------------------------------------------------------
    def _consume_loop(self):
        """Loop principal de consumo con commit y control de errores."""
        while self.running:
            try:
                for message in self.consumer:
                    if not self.running:
                        break
                    if self.parallel:
                        threading.Thread(target=self._process_message, args=(message,), daemon=True).start()
                    else:
                        self._process_message(message)
            except KafkaError as e:
                log.error(f"[!] Error en el loop de consumo: {e}. Reintentando en 10s...")
                time.sleep(10)
                self._reconnect()
            except Exception as e:
                log.error(f"[!] Excepción general en el consumo: {e}")
                time.sleep(5)

    # -------------------------------------------------------------------------
    # PROCESAMIENTO DE MENSAJE Y COMMIT
    # -------------------------------------------------------------------------
    def _process_message(self, message):
        """Procesa cada mensaje con timeout y commits con reintento."""

        log.debug(f"Mensaje recibido: tipo={type(message)}, topic={getattr(message, 'topic', None)}, partition={getattr(message, 'partition', None)}, offset={getattr(message, 'offset', None)}")


        try:
            if not message or not hasattr(message, "topic"):
                log.warning("[!] Mensaje inválido o vacío recibido, se ignora.")
                return

            start_time = time.time()
            try:
                self.callback(message, self.consumer)
            except Exception as e:
                log.error(f"[!] Error dentro de callback: {e}")
                return
            elapsed = time.time() - start_time

            if elapsed > self.process_timeout:
                log.warning(f"[!] Procesamiento lento ({elapsed:.1f}s) para offset {message.offset}")

            tp = TopicPartition(message.topic, message.partition)
            offsets = {tp: OffsetAndMetadata(message.offset + 1, "procesado", 0)}

            # Evitar commits simultáneos (thread-safe)
            with self._lock:
                for attempt in range(1, self.max_commit_retries + 1):
                    try:
                        if not self.consumer:
                            log.warning("[!] Consumer no disponible para commit.")
                            return
                        log.debug(f"Commit info: topic={message.topic}, partition={message.partition}, offset={message.offset}")
                        self.consumer.commit(offsets=offsets)
                        break
                    except IndexError:
                        log.error("[!] Commit falló: partición ya no asignada (IndexError). Reintentando...")
                        time.sleep(1)
                        continue
                    except Exception as e:
                        log.error(f"[!] Commit fallo (intento {attempt}): {e}")
                        if attempt == self.max_commit_retries:
                            log.error(f"[!] Commit final fallido para offset {message.offset}")
                        else:
                            time.sleep(2 ** attempt)

        except Exception as e:
            log.error(f"[!] Error procesando mensaje offset {getattr(message, 'offset', '?')}: {e}")


    # -------------------------------------------------------------------------
    # LECTURA DIRECTA DE UN OFFSET
    # -------------------------------------------------------------------------
    def leer_offset(self, offset: int, partition: int = 0, timeout_ms: int = 5000):
        """Lee un offset específico de forma síncrona."""
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=KAFKA_BROKER,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="none",
                consumer_timeout_ms=timeout_ms
            )

            tp = TopicPartition(self.topic, partition)

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


################################################
#   Pruebas
################################################
def procesar_mensaje(msg, consumer):
    print(f"📨 Mensaje recibido: {msg.value.decode()} de {msg.topic}-{msg.partition}@{msg.offset}")


if __name__ == "__main__":
    # 🔹 Cambia `parallel=False` si quieres procesar mensajes uno a uno
    consumer = KafkaEventConsumer(
        "mi_topic",
        procesar_mensaje,
        group_id="grupo_sync",
        parallel=False,  # Cambia a True para paralelo
    )
    consumer.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        consumer.stop()
