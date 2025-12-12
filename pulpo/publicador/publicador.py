import json
import os
import threading
import time
import traceback
from collections import defaultdict
from datetime import datetime
from typing import Optional, Dict, Any

from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import (
    TopicAlreadyExistsError, 
    NoBrokersAvailable, 
    KafkaError, 
    KafkaTimeoutError,
    MessageSizeTooLargeError,
    RequestTimedOutError
)

from pulpo.logueador import log
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
#log.set_log_file(f"log/publicador[{log_time}].log")
log.set_log_level("DEBUG")

from pulpo.util.util import require_env

# Environment variables with defaults
KAFKA_BROKER = require_env("KAFKA_BROKER")
SEND_TIMEOUT_MS = int(os.getenv("KAFKA_SEND_TIMEOUT_MS", "30000"))  # 30 seconds
MAX_REQUEST_SIZE = int(os.getenv("KAFKA_MAX_REQUEST_SIZE", "1048576"))  # 1MB
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "10"))
RETRIES = int(os.getenv("KAFKA_RETRIES", "3"))
HEALTH_CHECK_INTERVAL = int(os.getenv("KAFKA_PRODUCER_HEALTH_CHECK_INTERVAL", "60"))  # 1 minute

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
    """
    Productor robusto de Kafka con thread-safety, métricas, health checks y manejo de errores completo.
    
    Features:
    - Thread-safe con locks para acceso concurrente
    - Métricas detalladas de mensajes enviados/fallidos
    - Health check periódico de conectividad
    - Manejo específico de errores Kafka
    - Timeouts configurables
    - Auto-creación de tópicos
    """
    
    def __init__(
        self, 
        broker: str = KAFKA_BROKER,
        send_timeout_ms: int = SEND_TIMEOUT_MS,
        max_request_size: int = MAX_REQUEST_SIZE,
        linger_ms: int = LINGER_MS,
        retries: int = RETRIES,
        health_check_interval: int = HEALTH_CHECK_INTERVAL
    ):
        self.broker = broker
        self.send_timeout_ms = send_timeout_ms
        self.max_request_size = max_request_size
        self.linger_ms = linger_ms
        self.retries = retries
        self.health_check_interval = health_check_interval
        
        # Producer instance
        self._producer: Optional[KafkaProducer] = None
        
        # Thread-safety
        self._lock = threading.Lock()
        self._closed = False
        
        # Metrics
        self._metrics = defaultdict(int)
        self._last_send_time = time.time()
        self._last_health_check = time.time()
        
        # Health monitoring
        self._health_check_thread: Optional[threading.Thread] = None
        self._stop_health_check = threading.Event()
        
        log.info(f"[KafkaEventPublisher] 📝 Inicializado con broker={broker}, timeout={send_timeout_ms}ms")

    def start(self):
        """Inicia el productor de Kafka con configuración robusta."""
        with self._lock:
            if self._producer is not None:
                log.warning("[KafkaEventPublisher] ⚠️ El productor ya está iniciado")
                return
            
            try:
                log.info(f"[KafkaEventPublisher] 🔄 Conectando a {self.broker}")
                
                self._producer = KafkaProducer(
                    bootstrap_servers=self.broker,
                    acks="all",  # Asegura que el líder y todas las réplicas confirmen
                    retries=self.retries,
                    linger_ms=self.linger_ms,  # Batch pequeños para baja latencia
                    request_timeout_ms=self.send_timeout_ms,
                    max_request_size=self.max_request_size,
                    compression_type='gzip',  # Compresión para optimizar red
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    # Configuración de buffer
                    buffer_memory=33554432,  # 32MB buffer
                    batch_size=16384,  # 16KB por batch
                    max_in_flight_requests_per_connection=5,
                )
                
                self._closed = False
                self._metrics.clear()
                self._last_send_time = time.time()
                
                # Iniciar health check
                self._start_health_check_thread()
                
                log.info("[KafkaEventPublisher] ✅ Productor conectado correctamente")
                
            except NoBrokersAvailable as e:
                log.error(f"[KafkaEventPublisher] ❌ No hay brokers disponibles en {self.broker}: {e}")
                self._producer = None
                raise
            except Exception as e:
                tb = traceback.format_exc()
                log.error(f"[KafkaEventPublisher] ❌ Error al iniciar productor: {e}\n{tb}")
                self._producer = None
                raise

    def stop(self, timeout: int = 30):
        """
        Detiene el productor de forma segura, vaciando el buffer.
        
        Args:
            timeout: Tiempo máximo en segundos para esperar el flush
        """
        with self._lock:
            if self._producer is None:
                log.info("[KafkaEventPublisher] ℹ️ El productor ya estaba detenido")
                return
            
            try:
                self._closed = True
                
                # Detener health check
                self._stop_health_check.set()
                if self._health_check_thread and self._health_check_thread.is_alive():
                    self._health_check_thread.join(timeout=5)
                
                log.info("[KafkaEventPublisher] 🔄 Flushing mensajes pendientes...")
                self._producer.flush(timeout=timeout)
                log.info("[KafkaEventPublisher] ✅ Flush completado")
                
                # Log métricas finales
                self._log_metrics()
                
            except KafkaTimeoutError:
                log.error(f"[KafkaEventPublisher] ⏰ Timeout ({timeout}s) al hacer flush")
            except Exception as e:
                log.error(f"[KafkaEventPublisher] ❌ Error al detener productor: {e}")
            finally:
                try:
                    self._producer.close(timeout=5)
                except Exception as e:
                    log.error(f"[KafkaEventPublisher] ⚠️ Error al cerrar productor: {e}")
                
                self._producer = None
                log.info("[KafkaEventPublisher] 🔌 Productor detenido")

    def is_closed(self) -> bool:
        """Verifica si el productor está cerrado."""
        with self._lock:
            return self._closed or self._producer is None

    def publish(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publica un mensaje en un tópico. Si el tópico no existe, lo crea automáticamente.
        
        Args:
            topic: Nombre del tópico
            message: Diccionario con el mensaje
            key: Clave opcional para particionamiento (si no se especifica, usa job_id del mensaje)
            
        Returns:
            True si se publicó exitosamente, False en caso contrario
        """
        if self.is_closed():
            log.error("[KafkaEventPublisher] ❌ El productor está cerrado. Llama a start() antes de publicar")
            return False

        # Auto-crear tópico si no existe
        crear_topico(self.broker, topic)

        # Determinar key: parámetro > job_id del mensaje > None
        if key is None:
            key = message.get("job_id")
        
        key_bytes = key.encode('utf-8') if key else None
        
        try:
            with self._lock:
                if self._producer is None:
                    log.error("[KafkaEventPublisher] ❌ Productor no disponible")
                    self._metrics['send_failed'] += 1
                    return False
                
                future = self._producer.send(
                    topic,
                    key=key_bytes,
                    value=message
                )
            
            # Esperar confirmación con timeout
            result = future.get(timeout=self.send_timeout_ms / 1000.0)
            
            with self._lock:
                self._metrics['messages_sent'] += 1
                self._last_send_time = time.time()
            
            log.debug(
                f"📤 Mensaje publicado en '{result.topic}' "
                f"[partition {result.partition}] offset {result.offset} key={key}"
            )
            return True
            
        except MessageSizeTooLargeError as e:
            self._metrics['error_message_too_large'] += 1
            log.error(
                f"❌ Mensaje demasiado grande para '{topic}': {e}. "
                f"Tamaño máximo: {self.max_request_size} bytes"
            )
            return False
            
        except KafkaTimeoutError as e:
            self._metrics['error_timeout'] += 1
            log.error(f"⏰ Timeout ({self.send_timeout_ms}ms) publicando en '{topic}': {e}")
            return False
            
        except RequestTimedOutError as e:
            self._metrics['error_request_timeout'] += 1
            log.error(f"⏰ Request timeout publicando en '{topic}': {e}")
            return False
            
        except NoBrokersAvailable as e:
            self._metrics['error_no_brokers'] += 1
            log.error(f"❌ No hay brokers disponibles para '{topic}': {e}")
            return False
            
        except KafkaError as e:
            self._metrics['error_kafka'] += 1
            log.error(f"❌ Error Kafka publicando en '{topic}': {e}", exc_info=True)
            return False
            
        except Exception as e:
            self._metrics['error_unknown'] += 1
            log.error(f"❌ Error desconocido publicando en '{topic}': {e}", exc_info=True)
            return False

    def publish_commit(self, topic: str, message: Dict[str, Any], key: Optional[str] = None) -> bool:
        """
        Publica un mensaje y espera confirmación completa (flush inmediato).
        Más lento pero garantiza durabilidad inmediata.
        
        Args:
            topic: Nombre del tópico
            message: Diccionario con el mensaje
            key: Clave opcional para particionamiento
            
        Returns:
            True si se publicó y confirmó exitosamente, False en caso contrario
        """
        if self.is_closed():
            log.error("[KafkaEventPublisher] ❌ El productor está cerrado")
            return False

        success = self.publish(topic, message, key)
        
        if success:
            try:
                with self._lock:
                    if self._producer:
                        self._producer.flush(timeout=5)
                        log.info(f"✅ Mensaje 'transaccional' confirmado en '{topic}' key={key}")
                        return True
            except KafkaTimeoutError:
                log.error(f"⏰ Timeout en flush para '{topic}'")
                return False
            except Exception as e:
                log.error(f"❌ Error en flush para '{topic}': {e}")
                return False
        
        return False

    def get_metrics(self) -> Dict[str, Any]:
        """Retorna métricas actuales del productor."""
        with self._lock:
            return {
                **dict(self._metrics),
                'last_send_time': self._last_send_time,
                'is_closed': self._closed,
                'broker': self.broker
            }

    def _log_metrics(self):
        """Log de métricas para debugging."""
        metrics = self.get_metrics()
        log.info(
            f"[KafkaEventPublisher] 📊 Métricas: "
            f"enviados={metrics.get('messages_sent', 0)}, "
            f"errores_timeout={metrics.get('error_timeout', 0)}, "
            f"errores_kafka={metrics.get('error_kafka', 0)}, "
            f"sin_brokers={metrics.get('error_no_brokers', 0)}"
        )

    def _start_health_check_thread(self):
        """Inicia hilo de health check en background."""
        if self.health_check_interval <= 0:
            return
        
        self._stop_health_check.clear()
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            name="kafka-producer-health-check",
            daemon=True
        )
        self._health_check_thread.start()
        log.info(f"[KafkaEventPublisher] 🏥 Health check iniciado (intervalo: {self.health_check_interval}s)")

    def _health_check_loop(self):
        """Loop de health check periódico."""
        while not self._stop_health_check.is_set():
            try:
                time.sleep(self.health_check_interval)
                
                if self._stop_health_check.is_set():
                    break
                
                with self._lock:
                    if self._producer is None or self._closed:
                        log.warning("[KafkaEventPublisher] 🏥 Health check: productor no disponible")
                        continue
                    
                    # Verificar que el productor esté vivo
                    try:
                        # Verificar conectividad de forma más robusta
                        if hasattr(self._producer, 'bootstrap_connected'):
                            is_connected = self._producer.bootstrap_connected()
                        else:
                            # Fallback: verificar si _client existe y está conectado
                            is_connected = (
                                hasattr(self._producer, '_client') and 
                                self._producer._client is not None and
                                hasattr(self._producer._client, 'bootstrap_connected') and
                                self._producer._client.bootstrap_connected()
                            )
                        
                        if not is_connected:
                            # WARNING en vez de ERROR ya que el productor puede funcionar igualmente
                            log.warning("[KafkaEventPublisher] 🏥 Health check: bootstrap_connected() reporta desconexión (puede ser falso positivo)")
                            self._metrics['health_check_failures'] += 1
                        else:
                            log.debug("[KafkaEventPublisher] 🏥 Health check OK")
                            self._metrics['health_check_success'] += 1
                            
                    except AttributeError as ae:
                        log.debug(f"[KafkaEventPublisher] 🏥 Health check: método no disponible, asumiendo OK")
                        self._metrics['health_check_success'] += 1  # Asumimos OK si no podemos verificar
                    except Exception as e:
                        log.warning(f"[KafkaEventPublisher] 🏥 Health check error (no crítico): {e}")
                        self._metrics['health_check_failures'] += 1
                
                self._last_health_check = time.time()
                
            except Exception as e:
                log.error(f"[KafkaEventPublisher] ❌ Error en health check loop: {e}")


# ========================================================
# 🔧 Ejemplo de uso
# ========================================================

if __name__ == "__main__":
    publisher = KafkaEventPublisher()
    publisher.start()

    mensaje = {"id": 1, "texto": "Hola Kafka con kafka-python"}
    publisher.publish("prueba_topic", mensaje)

    publisher.stop()
