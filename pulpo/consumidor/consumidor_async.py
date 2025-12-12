import asyncio
import json
import logging
import time
import uuid
from typing import Callable, Optional, Dict, Any, Coroutine
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition, OffsetAndMetadata
from aiokafka.errors import (
    KafkaError, KafkaTimeoutError, CommitFailedError, 
    KafkaConnectionError, IllegalStateError
)
from datetime import datetime
from collections import defaultdict
from pulpo.util.util import require_env

KAFKA_BROKER = require_env("KAFKA_BROKER")
MAX_RETRY_ATTEMPTS = int(require_env("KAFKA_MAX_RETRIES"))
RETRY_BACKOFF_MS = int(require_env("KAFKA_RETRY_BACKOFF_MS"))
COMMIT_INTERVAL_MS = int(require_env("KAFKA_COMMIT_INTERVAL_MS"))
HEALTH_CHECK_INTERVAL = int(require_env("HEALTH_CHECK_INTERVAL"))
UNASSIGNED_TIMEOUT = int(require_env("UNASSIGNED_TIMEOUT"))

from pulpo.logueador import log
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
log.set_log_level("DEBUG")


class AsyncKafkaEventConsumer:
    """
    Consumidor Kafka asíncrono robusto con:
    - Reintentos automáticos
    - Commits batch y manuales
    - Monitoreo de salud
    - Manejo de errores avanzado
    - Dead Letter Queue (DLQ)
    - Métricas de procesamiento
    - Soporte para callbacks con mensaje completo o solo datos
    - Procesamiento concurrente de múltiples mensajes
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
        max_poll_interval_ms: int = 900000,  # 15 minutos para callbacks largos
        pass_raw_message: bool = False,
        max_concurrent_messages: int = 1,  # Máximo de mensajes procesándose en paralelo
    ):
        self.topic = topic
        self.callback = callback
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id or f"async_consumer_{topic}_{uuid.uuid4().hex[:8]}"
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
        self.max_concurrent_messages = max_concurrent_messages

        # Estado
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_commit_time = time.time()
        self._pending_offsets: Dict[TopicPartition, int] = {}
        self._processing_semaphore = asyncio.Semaphore(max_concurrent_messages)

        # Métricas
        self.metrics = {
            "messages_processed": 0,
            "messages_failed": 0,
            "messages_retried": 0,
            "messages_sent_to_dlq": 0,
            "last_message_timestamp": None,
            "processing_errors": defaultdict(int),
            "concurrent_processing": 0,
        }
        
        # Health check
        self._last_health_check = time.time()
        self._last_assigned = time.time()
        self._is_healthy = True

        # Flag temporal para saber si el último envío a DLQ fue exitoso
        self._last_dlq_ok: Optional[bool] = None

        callback_type = "mensaje completo" if self.pass_raw_message else "solo datos"
        log.info(
            f"[{self.group_id}] Consumidor ASYNC inicializado para topic '{self.topic}' "
            f"(retries={self.max_retries}, DLQ={'enabled' if self.enable_dlq else 'disabled'}, "
            f"callback={callback_type}, max_concurrent={self.max_concurrent_messages})"
        )

    # ============================================================
    # CREACIÓN Y CONFIGURACIÓN DEL CONSUMIDOR
    # ============================================================

    async def _create_consumer(self) -> AIOKafkaConsumer:
        """Crea un consumidor Kafka asíncrono con configuración robusta."""
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            enable_auto_commit=False,
            auto_offset_reset=self.auto_offset_reset,
            session_timeout_ms=self.session_timeout_ms,
            heartbeat_interval_ms=self.heartbeat_interval_ms,
            max_poll_interval_ms=self.max_poll_interval_ms,
            max_poll_records=self.max_concurrent_messages,
        )
        await consumer.start()
        return consumer

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

    async def _process_message_with_retry(self, msg) -> bool:
        """
        Procesa un mensaje con reintentos automáticos de forma asíncrona.
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
                    if asyncio.iscoroutinefunction(self.callback):
                        await self.callback(msg)
                    else:
                        self.callback(msg)
                else:
                    if asyncio.iscoroutinefunction(self.callback):
                        await self.callback(data)
                    else:
                        self.callback(data)
                
                # Éxito
                self.metrics["messages_processed"] += 1
                self.metrics["last_message_timestamp"] = datetime.now().isoformat()
                
                if attempt > 1:
                    self.metrics["messages_retried"] += 1
                    log.info(f"✅ Mensaje procesado tras {attempt} intentos")
                else:
                    log.info(f"✅ Mensaje [{msg.partition}:{msg.offset}] procesado correctamente")
                
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
                    await asyncio.sleep(backoff)
                else:
                    log.error(
                        f"❌ Fallo definitivo tras {self.max_retries} intentos: {error_type}: {e}"
                    )
                    self.metrics["messages_failed"] += 1
                    
                    # Enviar a DLQ si está habilitado
                    if self.enable_dlq:
                        data_for_dlq = data if not self.pass_raw_message else self._deserialize_message(msg.value)
                        dlq_ok = await self._send_to_dlq(msg, data_for_dlq, e)
                        self._last_dlq_ok = bool(dlq_ok)
                        if not dlq_ok:
                            log.error("❌ El envío a DLQ falló; revisa conectividad del broker o topic de DLQ")
                    else:
                        self._last_dlq_ok = None
                    
                    return False
        
        return False

    async def _send_to_dlq(self, msg, data: Any, error: Exception):
        """Envía un mensaje fallido a la Dead Letter Queue."""
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await producer.start()
            
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
            
            await producer.send_and_wait(self.dlq_topic, value=dlq_message)
            await producer.stop()
            
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

    async def _commit_offsets(self):
        """Commit de los offsets pendientes (manejo seguro para aiokafka)."""
        if not self._pending_offsets or not self._consumer:
            return

        # Construcción de offsets a commitear
        offsets = {
            tp: OffsetAndMetadata(offset + 1, "")
            for tp, offset in self._pending_offsets.items()
        }

        try:
            await self._consumer.commit(offsets)
            log.info(f"💾 Commit exitoso de {len(offsets)} particiones")
            print(f"💾 Commit exitoso de {len(offsets)} particiones")
            
            self._pending_offsets.clear()
            self._last_commit_time = time.time()

        except CommitFailedError as e:
            log.warning(f"⚠️ CommitFailedError (hay rebalance, se reintentará): {e}")
            print(f"⚠️ CommitFailedError (hay rebalance, se reintentará): {e}")

        except Exception as e:
            log.error(f"❌ Error inesperado en commit: {e}")
            print(f"❌ Error inesperado en commit: {e}")

    # ============================================================
    # BUCLE PRINCIPAL
    # ============================================================

    async def _process_single_message(self, msg, tp):
        """Procesa un solo mensaje con control de concurrencia."""
        async with self._processing_semaphore:
            self.metrics["concurrent_processing"] += 1
            try:
                start_time = time.time()
                success = await self._process_message_with_retry(msg)
                elapsed = time.time() - start_time
                
                # Advertir si se acerca al límite de max_poll_interval
                if elapsed > 60:
                    log.info(f"⏱️ Mensaje procesado en {elapsed:.1f}s")
                if elapsed > 600:  # Más de 10 minutos
                    log.warning(f"⚠️ Callback LARGO: {elapsed:.1f}s - Cerca del límite!")
                
                if success:
                    self._pending_offsets[tp] = msg.offset
                else:
                    # Siempre marcar offset localmente para control
                    self._pending_offsets[tp] = msg.offset
                    # Si usamos DLQ, solo forzar commit inmediato si el envío a DLQ fue exitoso
                    if self.enable_dlq:
                        if getattr(self, "_last_dlq_ok", False):
                            try:
                                await self._commit_offsets()
                            except Exception as e:
                                log.error(f"❌ Error forzando commit tras DLQ: {e}")
                        else:
                            log.error("⚠️ DLQ no disponible o envío fallido; no se avanza offset.")
                    self._last_dlq_ok = None
                
                if self._should_commit():
                    await self._commit_offsets()
            finally:
                self.metrics["concurrent_processing"] -= 1

    async def _consume_loop(self):
        """Bucle principal de consumo con manejo robusto de errores."""
        log.info(f"🚀 [{self.group_id}] Conectando a Kafka en {self.bootstrap_servers}")
        log.info(f"⚙️ Configurado para callbacks largos: max_poll_interval={self.max_poll_interval_ms/1000:.0f}s, "
                 f"max_concurrent={self.max_concurrent_messages}")
        
        reconnect_attempts = 0
        max_reconnect_attempts = 5

        while self._running:
            try:
                # Crear consumidor si no existe
                if self._consumer is None:
                    log.info(f"[{self.group_id}] Creando consumidor inicial...")
                    self._consumer = await self._create_consumer()
                    reconnect_attempts = 0
                    continue

                # Polling de mensajes
                try:
                    result = await self._consumer.getmany(
                        timeout_ms=1000,
                        max_records=self.max_concurrent_messages
                    )
                except IllegalStateError:
                    log.warning(f"[{self.group_id}] ⚠️ Consumidor en estado ilegal, recreando…")
                    await self._consumer.stop()
                    self._consumer = None
                    reconnect_attempts = 0
                    continue
                
                if not result:
                    # Health check periódico
                    await self._periodic_health_check()
                    continue
                
                # Procesar mensajes concurrentemente
                tasks = []
                for tp, messages in result.items():
                    for msg in messages:
                        if not self._running:
                            break
                        task = asyncio.create_task(self._process_single_message(msg, tp))
                        tasks.append(task)
                
                # Esperar a que todos los mensajes se procesen
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                
                # Commit final de lo que quede
                if self._pending_offsets:
                    await self._commit_offsets()
            
            except KafkaTimeoutError:
                log.warning("⏱️ Timeout en Kafka, reintentando...")
                await asyncio.sleep(1)
                
            except KafkaError as e:
                log.error(f"❌ Error de Kafka: {e}")
                await self._handle_kafka_error(e)
                reconnect_attempts += 1
                
                if reconnect_attempts >= max_reconnect_attempts:
                    log.critical(f"🛑 Máximo de reintentos alcanzado ({max_reconnect_attempts})")
                    self._is_healthy = False
                    break
                
                backoff = min(2 ** reconnect_attempts, 30)
                log.info(f"🔄 Reintentando conexión en {backoff}s...")
                await asyncio.sleep(backoff)
                
            except Exception as e:
                log.error(f"❌ Error inesperado en bucle de consumo: {e}", exc_info=True)
                await asyncio.sleep(5)
        
        # Cleanup
        await self._cleanup()

    async def _handle_kafka_error(self, error: KafkaError):
        """Maneja errores específicos de Kafka."""
        if self._consumer:
            try:
                await self._consumer.stop()
            except:
                pass
            self._consumer = None
        
        log.warning(f"🔧 Consumidor cerrado debido a error: {error}")

    async def _periodic_health_check(self):
        """Verifica la salud del consumidor periódicamente."""
        now = time.time()
        if now - self._last_health_check >= HEALTH_CHECK_INTERVAL:
            self._last_health_check = now

            if self._consumer:
                try:
                    assignment = self._consumer.assignment()
                    if assignment:
                        if not self._is_healthy:
                            log.info(f"✅ Consumidor recuperado - Particiones asignadas: {len(assignment)}")
                        self._is_healthy = True
                        self._last_assigned = now
                    else:
                        log.warning("⚠️ No hay particiones asignadas")
                        
                        # Si lleva demasiado tiempo sin particiones, reinicia
                        if now - self._last_assigned > UNASSIGNED_TIMEOUT:
                            log.warning("⏳ Reiniciando consumidor tras timeout sin particiones...")
                            await self._restart_consumer()
                            self._last_assigned = now
                            self._is_healthy = False
                        else:
                            self._is_healthy = False

                except Exception as e:
                    log.error(f"❌ Error en health check: {e}")
                    self._is_healthy = False

    async def _restart_consumer(self):
        """Reinicia el consumidor."""
        try:
            if self._consumer:
                try:
                    await self._consumer.stop()
                    log.info("🧹 Consumidor cerrado correctamente")
                except Exception:
                    pass
                self._consumer = None

            await asyncio.sleep(2)
            self._consumer = await self._create_consumer()
            log.info("🔄 Consumidor reiniciado (nuevo AIOKafkaConsumer creado).")

        except Exception as e:
            log.error(f"❌ Error al reiniciar el consumidor: {e}", exc_info=True)

    async def _cleanup(self):
        """Limpia recursos al detener."""
        log.info(f"🧹 [{self.group_id}] Limpiando recursos...")
        
        # Commit final
        if self._pending_offsets:
            await self.safe_commit()
        
        # Cerrar consumidor
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                log.error(f"Error cerrando consumidor: {e}")
            finally:
                self._consumer = None
        
        log.info(f"🛑 [{self.group_id}] Consumidor detenido correctamente")

    # ============================================================
    # CONTROL DEL CONSUMIDOR
    # ============================================================

    async def start(self):
        """Inicia el consumidor de forma asíncrona."""
        if self._running:
            log.warning("⚠️ El consumidor ya está en ejecución")
            return

        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        log.info(f"✅ [{self.group_id}] Tarea de consumo iniciada")

    async def stop(self, timeout: int = 10):
        """Detiene el consumidor de forma ordenada."""
        if not self._running:
            log.warning("⚠️ El consumidor no está en ejecución")
            return
        
        log.info(f"🛑 [{self.group_id}] Deteniendo consumidor...")
        self._running = False
        
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=timeout)
            except asyncio.TimeoutError:
                log.warning(f"⚠️ La tarea no terminó en {timeout}s")
                self._task.cancel()
        
        log.info(f"✅ [{self.group_id}] Consumidor detenido")

    async def safe_commit(self, max_retries: int = 30):
        """Commit con timeout para evitar loops infinitos."""
        if not self._consumer:
            return

        for attempt in range(max_retries):
            try:
                await self._consumer.commit()
                return
            except CommitFailedError:
                if attempt == 0:
                    log.warning("🔄 Commit aplazado: rebalance en progreso. Retentando...")
                await asyncio.sleep(0.1)
            except Exception as e:
                log.error(f"❌ Error en commit: {e}")
                return
        
        log.error(f"❌ Commit falló tras {max_retries} reintentos (rebalance prolongado)")

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

    def is_running(self) -> bool:
        """Verifica si el consumidor está en ejecución."""
        return self._running

    # -------------------------------------------------------------------------
    # LECTURA DIRECTA DE UN OFFSET
    # -------------------------------------------------------------------------
    async def leer_offset(self, offset: int, partition: int = 0, timeout_ms: int = 5000):
        """Lee un offset específico de forma asíncrona."""
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                enable_auto_commit=False,
                auto_offset_reset="none",
                consumer_timeout_ms=timeout_ms
            )
            await consumer.start()

            tp = TopicPartition(self.topic, partition)
            
            partitions = await consumer.partitions_for_topic(self.topic)
            if partitions is None:
                raise RuntimeError(f"El topic '{self.topic}' no existe en el broker {self.bootstrap_servers}")
            if partition not in partitions:
                raise RuntimeError(f"La partición {partition} no existe en el topic '{self.topic}'")

            consumer.assign([tp])
            consumer.seek(tp, offset)

            async for msg in consumer:
                if msg.offset == offset:
                    return msg
                elif msg.offset > offset:
                    break

            return None

        except KafkaConnectionError:
            raise RuntimeError(f"No se pudo conectar al broker: {self.bootstrap_servers}")
        except KafkaTimeoutError:
            raise RuntimeError(f"Timeout al leer offset {offset}")
        except KafkaError as e:
            raise RuntimeError(f"Error de Kafka: {e}")
        finally:
            if consumer is not None:
                await consumer.stop()


# ============================================================
# EJEMPLO DE USO
# ============================================================
async def main():
    """Ejemplo de uso del consumidor asíncrono."""
    
    async def callback_prueba_async(mensaje):
        """Callback asíncrono que recibe el mensaje completo de Kafka."""
        payload = json.loads(mensaje.value.decode("utf-8"))
        print("📦 Payload decodificado:", payload)
        print(f"   - Topic: {mensaje.topic}")
        print(f"   - Partition: {mensaje.partition}")
        print(f"   - Offset: {mensaje.offset}")
        
        # Simular procesamiento asíncrono
        await asyncio.sleep(0.5)
        print("✅ Mensaje procesado correctamente")

    consumer = AsyncKafkaEventConsumer(
        topic="rfq.processed",
        callback=callback_prueba_async,
        bootstrap_servers="alcazar:29092",
        max_retries=3,
        enable_dlq=True,
        batch_commit=True,
        commit_interval_ms=5000,
        pass_raw_message=True,
        max_concurrent_messages=1,  # Procesar 1 mensaje a la vez
    )

    await consumer.start()

    print("🟢 Consumidor Kafka ASYNC arrancado. Ctrl+C para salir.")
    print("📊 Métricas disponibles con consumer.get_metrics()")
    
    try:
        while True:
            await asyncio.sleep(10)
            # Mostrar métricas periódicamente
            metrics = consumer.get_metrics()
            print(f"\n📊 Métricas: {json.dumps(metrics, indent=2, default=str)}")
    except KeyboardInterrupt:
        print("\n👋 Deteniendo consumidor...")
        await consumer.stop()
        print("✅ Finalizado correctamente")


if __name__ == "__main__":
    asyncio.run(main())
