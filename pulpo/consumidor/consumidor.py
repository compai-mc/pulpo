import threading
import json
import logging
import time
import uuid
from typing import Callable, Optional, Dict, Any
from kafka import KafkaConsumer, TopicPartition, OffsetAndMetadata
from kafka.errors import KafkaError, KafkaTimeoutError, CommitFailedError, KafkaConnectionError, RebalanceInProgressError
from datetime import datetime
from collections import defaultdict
from pulpo.util.util import require_env

KAFKA_BROKER = require_env("KAFKA_BROKER")
MAX_RETRY_ATTEMPTS = int(require_env("KAFKA_MAX_RETRIES"))
RETRY_BACKOFF_MS = int(require_env("KAFKA_RETRY_BACKOFF_MS"))
COMMIT_INTERVAL_MS = int(require_env("KAFKA_COMMIT_INTERVAL_MS"))
HEALTH_CHECK_INTERVAL = int(require_env("KAFKA_HEALTH_CHECK_INTERVAL"))

HEALTH_CHECK_INTERVAL=int(require_env("HEALTH_CHECK_INTERVAL"))
UNASSIGNED_TIMEOUT=int(require_env("UNASSIGNED_TIMEOUT"))


"""logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("consumidor")
"""

from pulpo.logueador import log
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
#log.set_log_file(f"log/consumidor[{log_time}].log")
log.set_log_level("DEBUG")


class KafkaEventConsumer:
    """
    Consumidor Kafka robusto con:
    - Reintentos automáticos
    - Commits batch y manuales
    - Monitoreo de salud
    - Manejo de errores avanzado
    - Dead Letter Queue (DLQ)
    - Métricas de procesamiento
    - Soporte para callbacks con mensaje completo o solo datos
    """

    def __init__(
        self,
        topic: str,
        callback: Callable,
        bootstrap_servers: str = KAFKA_BROKER,
        group_id: Optional[str] = None,
        auto_offset_reset: str = "latest",
        max_retries: int = MAX_RETRY_ATTEMPTS,
        retry_backoff_ms: int = RETRY_BACKOFF_MS,
        enable_dlq: bool = False,
        dlq_topic: Optional[str] = None,
        batch_commit: bool = True,
        commit_interval_ms: int = COMMIT_INTERVAL_MS,
        session_timeout_ms: int = 30000,
        heartbeat_interval_ms: int = 10000,
        max_poll_interval_ms: int = 300000,
        pass_raw_message: bool = False,
        
    ):
        self.topic = topic
        self.callback = callback
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id or f"consumer_{topic}_{uuid.uuid4().hex[:8]}"
        self.auto_offset_reset = auto_offset_reset
        self.max_retries = max_retries
        self.retry_backoff_ms = retry_backoff_ms
        self.enable_dlq = enable_dlq
        self.dlq_topic = dlq_topic or f"{topic}.dlq"
        self.batch_commit = batch_commit
        self.commit_interval_ms = commit_interval_ms
        self.session_timeout_ms = session_timeout_ms
        self.heartbeat_interval_ms = heartbeat_interval_ms
        self.max_poll_interval_ms = max_poll_interval_ms
        self.pass_raw_message = pass_raw_message

        # Estado
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._last_commit_time = time.time()
        self._pending_offsets: Dict[TopicPartition, int] = {}
        
        self._lock = threading.Lock()                    
        self.enable_auto_commit = False   

        self._mantener_polling = False  # Control del hilo de polling
        self._hilo_poll = None

        # Métricas
        self.metrics = {
            "messages_processed": 0,
            "messages_failed": 0,
            "messages_retried": 0,
            "messages_sent_to_dlq": 0,
            "last_message_timestamp": None,
            "processing_errors": defaultdict(int),
        }
        
        # Health check
        self._last_health_check = time.time()
        self._is_healthy = True

        # Flag temporal para saber si el último envío a DLQ fue exitoso
        self._last_dlq_ok: Optional[bool] = None

        callback_type = "mensaje completo" if self.pass_raw_message else "solo datos"
        log.info(
            f"[{self.group_id}] Consumidor inicializado para topic '{self.topic}' "
            f"(retries={self.max_retries}, DLQ={'enabled' if self.enable_dlq else 'disabled'}, "
            f"callback={callback_type})"
        )

    # ============================================================
    # CREACIÓN Y CONFIGURACIÓN DEL CONSUMIDOR
    # ============================================================

    def _create_consumer(self) -> KafkaConsumer:
        """Crea un consumidor Kafka con configuración robusta."""
        return KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            session_timeout_ms=self.session_timeout_ms,
            heartbeat_interval_ms=self.heartbeat_interval_ms,
            max_poll_interval_ms=self.max_poll_interval_ms,
            value_deserializer=lambda m: m,  # Recibimos bytes raw
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
            fetch_min_bytes=1,
            fetch_max_wait_ms=500,
            max_poll_records=100,
            connections_max_idle_ms=540000,
            request_timeout_ms=40000,
            retry_backoff_ms=self.retry_backoff_ms,
        )


    # ============================================================
    # PROCESAMIENTO DE MENSAJES
    # ============================================================

    def _deserialize_message(self, raw_value: bytes) -> Any:
        """Deserializa el mensaje, intentando JSON primero."""
        try:
            decoded = raw_value.decode("utf-8")
            try:
                return json.loads(decoded)
            except json.JSONDecodeError:
                return decoded
        except UnicodeDecodeError:
            log.warning("⚠️ No se pudo decodificar el mensaje como UTF-8")
            return raw_value

    def _process_message_with_retry(self, msg) -> bool:
        """
        Procesa un mensaje con reintentos automáticos.
        Retorna True si se procesó exitosamente, False si falló definitivamente.
        """
        # Solo deserializar si NO pasamos el mensaje completo
        if not self.pass_raw_message:
            data = self._deserialize_message(msg.value)
        
        for attempt in range(1, self.max_retries + 1):
            try:
                log.debug(f"[{msg.partition}:{msg.offset}] Procesando (intento {attempt}/{self.max_retries})")
                
                # Ejecutar callback según configuración
                if self.pass_raw_message:
                    self.callback(msg)  # Pasa el mensaje completo de Kafka
                else:
                    self.callback(data)  # Pasa solo los datos deserializados
                
                # Éxito
                self.metrics["messages_processed"] += 1
                self.metrics["last_message_timestamp"] = datetime.now().isoformat()
                
                if attempt > 1:
                    self.metrics["messages_retried"] += 1
                    log.info(f"✅ Mensaje procesado tras {attempt} intentos")
                else:
                    log.info(f"✅ Mensaje [{msg.partition}:{msg.offset}] procesado correctamente")
                
                # limpiar flag de DLQ previa
                self._last_dlq_ok = None
                return True
                
            except Exception as e:
                error_type = type(e).__name__
                self.metrics["processing_errors"][error_type] += 1
                
                if attempt < self.max_retries:
                    backoff = self.retry_backoff_ms * attempt / 1000.0
                    log.warning(
                        f"⚠️ Error en intento {attempt}/{self.max_retries}: {error_type}: {e}. "
                        f"Reintentando en {backoff}s..."
                    )
                    time.sleep(backoff)
                else:
                    log.error(
                        f"❌ Fallo definitivo tras {self.max_retries} intentos: {error_type}: {e}"
                    )
                    self.metrics["messages_failed"] += 1
                    
                    # Enviar a DLQ si está habilitado
                    if self.enable_dlq:
                        data_for_dlq = data if not self.pass_raw_message else self._deserialize_message(msg.value)
                        dlq_ok = self._send_to_dlq(msg, data_for_dlq, e)
                        # registrar resultado para la lógica de commit
                        self._last_dlq_ok = bool(dlq_ok)
                        if not dlq_ok:
                            log.error("❌ El envío a DLQ falló; revisa conectividad del broker o topic de DLQ")
                    else:
                        # si DLQ no está habilitado, limpiar flag
                        self._last_dlq_ok = None
                    
                    return False
        
        return False

    def _send_to_dlq(self, msg, data: Any, error: Exception):
        """Envía un mensaje fallido a la Dead Letter Queue."""
        try:
            from kafka import KafkaProducer
            
            producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            
            dlq_message = {
                "original_topic": msg.topic,
                "original_partition": msg.partition,
                "original_offset": msg.offset,
                "original_timestamp": msg.timestamp,
                "data": data if isinstance(data, (dict, list, str)) else str(data),
                "error": str(error),
                "error_type": type(error).__name__,
                "failed_at": datetime.now().isoformat(),
                "group_id": self.group_id,
            }
            
            producer.send(self.dlq_topic, value=dlq_message)
            producer.flush()
            producer.close()
            
            self.metrics["messages_sent_to_dlq"] += 1
            log.warning(f"📮 Mensaje enviado a DLQ: {self.dlq_topic}")
            return True
            
        except Exception as dlq_error:
            log.error(f"❌ Error enviando a DLQ: {dlq_error}")
            return False

    # ============================================================
    # COMMITS
    # ============================================================

    def _should_commit(self) -> bool:
        """Determina si es momento de hacer commit."""
        if not self.batch_commit:
            return True
        
        elapsed_ms = (time.time() - self._last_commit_time) * 1000
        return elapsed_ms >= self.commit_interval_ms or len(self._pending_offsets) >= 50


    def _commit_offsets(self):
        """Commit de los offsets pendientes (manejo seguro para kafka-python)."""

        if not self._pending_offsets:
            return

        # Construcción de offsets a commitear:
        offsets = {
            tp: OffsetAndMetadata(offset + 1, "", -1)
            for tp, offset in self._pending_offsets.items()
        }

        try:
            # kafka-python hace internamente toda la gestión del rebalance.
            self._consumer.commit(offsets=offsets)

            log.info(f"💾 Commit exitoso de {len(offsets)} particiones")
            print(f"💾 Commit exitoso de {len(offsets)} particiones")

            self._pending_offsets.clear()
            self._last_commit_time = time.time()

        except CommitFailedError as e:
            # Esto es NORMAL en Kafka cuando hay rebalance.
            log.warning(f"⚠️ CommitFailedError (hay rebalance, se reintentará): {e}")
            print(f"⚠️ CommitFailedError (hay rebalance, se reintentará): {e}")
            # No hacer nada. En el siguiente poll() Kafka recupera estado y listo.

        except Exception as e:
            log.error(f"❌ Error inesperado en commit: {e}")
            print(f"❌ Error inesperado en commit: {e}")

    # ============================================================
    # BUCLE PRINCIPAL
    # ============================================================

    def _consume_loop(self):
        """Bucle principal de consumo con manejo robusto de errores."""
        log.info(f"🚀 [{self.group_id}] Conectando a Kafka en {self.bootstrap_servers}")
        
        reconnect_attempts = 0
        max_reconnect_attempts = 5
        

        while self._running:
            try:
                # Crear consumidor si no existe o está cerrado
                if self._consumer is None:
                    # Esto SOLO ocurre si start() falló antes de crear el consumidor
                    log.info(f"[{self.group_id}] Creando consumidor inicial...")
                    print(f"[{self.group_id}] Creando consumidor inicial...")
                    self._consumer = self._create_consumer()
                    reconnect_attempts = 0
                    continue

                if getattr(self._consumer, "_closed", False):
                    log.warning(f"[{self.group_id}] ⚠️ Consumidor cerrado, recreando…")
                    print(f"[{self.group_id}] ⚠️ Consumidor cerrado, recreando…")
                    self._consumer = self._create_consumer()
                    reconnect_attempts = 0
                    continue
                    
                # Polling de mensajes
                messages = self._consumer.poll(timeout_ms=1000, max_records=100)
                
                if not messages:
                    # Health check periódico
                    self._periodic_health_check()
                    continue
                
                # Procesar mensajes
                for tp, msgs in messages.items():
                    for msg in msgs:
                        if not self._running:
                            break
                                    
                        success = self._process_message_with_retry(msg)
                        
                        if success:
                            self._pending_offsets[tp] = msg.offset
                        else:
                            # Siempre marcar offset localmente para control
                            self._pending_offsets[tp] = msg.offset
                            # Si usamos DLQ, solo forzar commit inmediato si el envío a DLQ fue exitoso
                            if self.enable_dlq:
                                if getattr(self, "_last_dlq_ok", False):
                                    try:
                                        self._commit_offsets()
                                    except Exception as e:
                                        log.error(f"❌ Error forzando commit tras DLQ: {e}")
                                else:
                                    log.error("⚠️ DLQ no disponible o envío fallido; no se avanza offset para permitir reintentos/alerta.")
                            # limpiar flag temporal
                            self._last_dlq_ok = None
                        
                        if self._should_commit():
                            self._commit_offsets()
                
                # Commit final de lo que quede
                if self._pending_offsets:
                    self._commit_offsets()
            
            except AssertionError as e:
                # Kafka lanza esto si se intenta hacer poll() sobre un consumidor cerrado
                if "KafkaConsumer is closed" in str(e):
                    log.warning(f"[{self.group_id}] ⚠️ Consumidor cerrado detectado, reiniciando...")
                    if self._consumer:
                        try:
                            self._consumer.close()
                        except Exception:
                            pass
                    self._consumer = None
                    time.sleep(3)
                    continue
                else:
                    log.error(f"[{self.group_id}] ❌ AssertionError inesperado: {e}", exc_info=True)
                    time.sleep(5)

            except KafkaTimeoutError:
                log.warning("⏱️ Timeout en Kafka, reintentando...")
                time.sleep(1)
                
            except KafkaError as e:
                log.error(f"❌ Error de Kafka: {e}")
                self._handle_kafka_error(e)
                reconnect_attempts += 1
                
                if reconnect_attempts >= max_reconnect_attempts:
                    log.critical(f"🛑 Máximo de reintentos alcanzado ({max_reconnect_attempts})")
                    self._is_healthy = False
                    break
                
                backoff = min(2 ** reconnect_attempts, 30)
                log.info(f"🔄 Reintentando conexión en {backoff}s...")
                time.sleep(backoff)
                
            except Exception as e:
                log.error(f"❌ Error inesperado en bucle de consumo: {e}", exc_info=True)
                time.sleep(5)
        
        # Cleanup
        self._cleanup()


    def _handle_kafka_error(self, error: KafkaError):
        """Maneja errores específicos de Kafka."""
        if self._consumer:
            try:
                self._consumer.close()
            except:
                pass
            self._consumer = None
        
        log.warning(f"🔧 Consumidor cerrado debido a error: {error}")



    def _periodic_health_check(self):
        """Verifica la salud del consumidor periódicamente y lo reinicia si no hay particiones asignadas durante demasiado tiempo."""
        now = time.time()
        if now - self._last_health_check >= HEALTH_CHECK_INTERVAL:
            self._last_health_check = now

            if self._consumer:
                try:
                    assignment = self._consumer.assignment()
                    if assignment:
                        # Está todo bien
                        if not self._is_healthy:
                            log.info(f"✅ Consumidor recuperado - Particiones asignadas: {len(assignment)}")
                        self._is_healthy = True
                        self._last_assigned = now  # registramos el último momento saludable
                    else:
                        
                        if not hasattr(self, "_last_assigned"):
                            self._last_assigned = now
                            return

                        log.warning("⚠️ No hay particiones asignadas")

                        # Si lleva demasiado tiempo sin particiones, reinicia
                        if now - self._last_assigned > UNASSIGNED_TIMEOUT:
                            log.warning("⏳ Reiniciando consumidor tras 60s sin particiones...")
                            self._restart_consumer()
                            self._last_assigned = now
                            self._is_healthy = False
                        else:
                            self._is_healthy = False

                except Exception as e:
                    log.error(f"❌ Error en health check: {e}")
                    self._is_healthy = False

    def _restart_consumer(self):
        try:
            with self._lock:
                if self._consumer:
                    try:
                        self._consumer.close()
                        log.info("🧹 Consumidor cerrado correctamente")
                    except Exception:
                        pass
                    self._consumer = None

            time.sleep(2)

            # Crear un nuevo consumidor usando los atributos correctos
            new_consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,   # <--- usar bootstrap_servers correcto
                group_id=self.group_id,
                enable_auto_commit=self.enable_auto_commit,  # <--- atributo definido en __init__
                auto_offset_reset=self.auto_offset_reset,
                session_timeout_ms=self.session_timeout_ms,
                heartbeat_interval_ms=self.heartbeat_interval_ms,
                max_poll_interval_ms=self.max_poll_interval_ms,
                value_deserializer=lambda m: m,
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
            )

            with self._lock:
                self._consumer = new_consumer

            log.info("🔄 Consumidor reiniciado (nuevo KafkaConsumer creado).")
            # No forzar subscribe si ya pasaste topic al constructor; pero asegurar subscribe no hace daño:
            try:
                self._consumer.subscribe([self.topic])
            except Exception:
                # algunos clientes cuando se construyen con topics ya inscritos no necesitan subscribe
                pass

        except Exception as e:
            log.error(f"❌ Error al reiniciar el consumidor: {e}", exc_info=True)


    def wait_until_assigned(self, timeout: int = 10) -> bool:
        """Espera hasta que el consumidor tenga una asignación de particiones."""
        start = time.time()
        while True:
            with self._lock:
                consumer = self._consumer
            if consumer:
                try:
                    # assignment() puede devolver set() inicialmente
                    assignment = consumer.assignment()
                    if assignment:
                        return True
                except Exception:
                    # si consumer todavía no está totalmente inicializado, ignorar y reintentar
                    pass
            if time.time() - start > timeout:
                return False
            time.sleep(0.2)


    def _cleanup(self):
        """Limpia recursos al detener."""
        log.info(f"🧹 [{self.group_id}] Limpiando recursos...")
        
        # Commit final
        if self._pending_offsets:
            self.safe_commit()
        
        # Cerrar consumidor
        if self._consumer:
            try:
                self._consumer.close()
            except Exception as e:
                log.error(f"Error cerrando consumidor: {e}")
            finally:
                self._consumer = None
        
        log.info(f"🛑 [{self.group_id}] Consumidor detenido correctamente")
        
    # ============================================================
    # CONTROL DEL CONSUMIDOR
    # ============================================================


    def start(self):
        if self._running:
            log.warning("⚠️ El consumidor ya está en ejecución")
            return

        # Crear el consumer ANTES del hilo
        self._consumer = self._create_consumer()

        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop,
            name=f"kafka-consumer-{self.topic}",
            daemon=True
        )
        self._thread.start()
        log.info(f"✅ [{self.group_id}] Hilo de consumo iniciado")

        # Esperar a que el consumer se cree y obtenga asignación de particiones
        ready = self.wait_until_assigned(timeout=15)
        if ready:
            log.info(f"✅ [{self.group_id}] Consumidor listo y asignado.")
        else:
            log.warning(f"⚠️ [{self.group_id}] Timeout esperando asignación (start).")


    def stop(self, timeout: int = 10):
        """Detiene el consumidor de forma ordenada."""
        if not self._running:
            log.warning("⚠️ El consumidor no está en ejecución")
            return
        
        log.info(f"🛑 [{self.group_id}] Deteniendo consumidor...")
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=timeout)
            if self._thread.is_alive():
                log.warning(f"⚠️ El hilo no terminó en {timeout}s")
        
        log.info(f"✅ [{self.group_id}] Consumidor detenido")


    def poll(self, timeout_ms: int = 1000, wait_ready: bool = False):
        if wait_ready:
            if not self.wait_until_assigned(timeout=10):
                log.error("❌ Timeout esperando inicialización y asignación del consumidor")
                return {"status": "error", "message": "Timeout esperando asignación de particiones"}

        with self._lock:
            consumer = self._consumer

        if not consumer:
            #log.error("❌ Consumidor no inicializado")
            return {"status": "error", "message": "No hay consumidor inicializado"}

        try:
            consumer.poll(timeout_ms=timeout_ms)
            return {"status": "ok", "message": "Polling realizado correctamente"}
        except AssertionError as e:
            log.error(f"❌ Poll falló: {e}")
            return  {"status": "error", "message": str(e)}  



    def safe_commit(self):
        if not self._consumer:
            return

        while True:
            try:
                self._consumer.commit()
                return  # éxito
            except RebalanceInProgressError:
                log.warning("🔄 Commit aplazado: rebalance en progreso. Retentando tras poll()...")
                self._consumer.poll(timeout_ms=100)
                time.sleep(0.1)
            except Exception as e:
                log.error(f"❌ Error inesperado en commit: {e}")
                return


    def safe_commit_offsets(self, offsets):
        if not self._consumer:
            return

        while True:
            try:
                self._consumer.commit(offsets=offsets)
                return
            except RebalanceInProgressError:
                log.warning("🔄 Commit aplazado: rebalance en progreso. Retentando tras poll()…")
                self._consumer.poll(timeout_ms=100)
                time.sleep(0.1)
            except Exception as e:
                log.error(f"❌ Error inesperado en commit: {e}")
                return



    def _mantener_consumidor_vivo(self):
        """Hilo de background que mantiene vivo el consumer Kafka."""
        while self._mantener_polling:
            try:
                self.poll(0)
            except Exception as e:
                msg = str(e)

                # Errores esperados -> no mostrar
                if (
                    "RebalanceInProgress" in msg or
                    "UnknownMemberId" in msg or
                    "No hay consumidor inicializado" in msg or
                    "KafkaConsumer is closed" in msg
                ):
                    pass  # silencio
                else:
                    print(f"⚠️ Error inesperado en poll: {e}")

            time.sleep(1)


    def iniciar_polling_background(self):
        """Inicia el hilo de polling."""
        self._mantener_polling = True
        self._hilo_poll = threading.Thread(target=self._mantener_consumidor_vivo, daemon=True)
        self._hilo_poll.start()

    def detener_polling_background(self):
        """Detiene el hilo de polling."""
        self._mantener_polling = False
        if hasattr(self, "_hilo_poll"):
            self._hilo_poll.join(timeout=2)

        
    def get_metrics(self) -> Dict[str, Any]:
        """Retorna las métricas del consumidor."""
        return {
            **self.metrics,
            "is_healthy": self._is_healthy,
            "is_running": self._running,
            "pending_commits": len(self._pending_offsets),
        }

    def is_healthy(self) -> bool:
        """Verifica si el consumidor está saludable."""
        return self._is_healthy and self._running


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

        except KafkaConnectionError:
            raise RuntimeError(f"No se pudo conectar al broker: {KAFKA_BROKER}")
        except KafkaTimeoutError:
            raise RuntimeError(f"Timeout al leer offset {offset}")
        except KafkaError as e:
            raise RuntimeError(f"Error de Kafka: {e}")
        finally:
            if consumer is not None:
                consumer.close()


# ============================================================
# EJEMPLO DE USO - OPCIÓN 1: MENSAJE COMPLETO
# ============================================================
if __name__ == "__main__":
    
    def callback_prueba(mensaje):
        """Callback que recibe el mensaje completo de Kafka."""
        payload = json.loads(mensaje.value.decode("utf-8"))
        print("📦 Payload decodificado:", payload)
        print(f"   - Topic: {mensaje.topic}")
        print(f"   - Partition: {mensaje.partition}")
        print(f"   - Offset: {mensaje.offset}")
        
        # Simular procesamiento
        time.sleep(0.5)
        print("✅ Mensaje procesado correctamente")

    consumer = KafkaEventConsumer(
        topic="rfq.processed",
        callback=callback_prueba,
        bootstrap_servers="alcazar:29092",
        max_retries=3,
        enable_dlq=True,
        batch_commit=True,
        commit_interval_ms=5000,
        pass_raw_message=True,  # 👈 IMPORTANTE: Pasa mensaje completo
    )

    consumer.start()

    print("🟢 Consumidor Kafka arrancado. Ctrl+C para salir.")
    print("📊 Métricas disponibles con consumer.get_metrics()")
    
    try:
        while True:
            time.sleep(10)
            # Mostrar métricas periódicamente
            metrics = consumer.get_metrics()
            print(f"\n📊 Métricas: {json.dumps(metrics, indent=2, default=str)}")
    except KeyboardInterrupt:
        print("\n👋 Deteniendo consumidor...")
        consumer.stop()
        print("✅ Finalizado correctamente")