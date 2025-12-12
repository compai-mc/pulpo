import json
import uuid
import os
import sys
import threading
import time
from pathlib import Path
from collections import defaultdict
from typing import Optional, Dict, Any, List, Callable
from arango import ArangoClient
from arango.exceptions import (
    ArangoError,
    DocumentGetError,
    DocumentInsertError,
    DocumentUpdateError,
    ServerConnectionError
)
from datetime import datetime

from pulpo.logueador import log
log_time = datetime.now().isoformat(timespec='minutes')
log.set_propagate(True)
#log.set_log_file(f"log/persona_virtual[{log_time}].log")
log.set_log_level("DEBUG")

# Añadir el directorio raíz del proyecto al path de Python
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pulpo.util.util import require_env

from config import config_global
estado_tarea = config_global.get("estado_tarea", None)

# Importar productor, consumidor y utilidades
from pulpo.publicador.publicador import KafkaEventPublisher
from pulpo.consumidor.consumidor import KafkaEventConsumer


# ========================================================
# 🔧 Configuración
# ========================================================
ARANGO_HOST = require_env("ARANGO_URL")
ARANGO_DB_COMPAI = require_env("ARANGO_DB_COMPAI")
ARANGO_USER = require_env("ARANGO_USER")
ARANGO_PASSWORD = require_env("ARANGO_PASSWORD")
ARANGO_COLLECTION = require_env("ARANGO_COLLECTION_TAREAS")
TOPIC_TASK_EVENTS = require_env("TOPIC_TASK_EVENTS")

DLQ_CONSUMER = require_env("DLQ_CONSUMER")
DLQ_COMPAI = require_env("DLQ_COMPAI")

# Configuración de retry y health check
DB_RETRY_ATTEMPTS = int(os.getenv("DB_RETRY_ATTEMPTS", "3"))
DB_RETRY_BACKOFF_MS = int(os.getenv("DB_RETRY_BACKOFF_MS", "1000"))  # 1 segundo
HEALTH_CHECK_INTERVAL = int(os.getenv("GESTOR_HEALTH_CHECK_INTERVAL", "60"))  # 1 minuto

# ========================================================
# 🧠 Clase principal: GestorTareas
# ========================================================
class GestorTareas:
    """
    Gestor centralizado de tareas y jobs usando un único tópico Kafka.
    
    Features:
    - Thread-safe con locks para operaciones ArangoDB
    - Singleton pattern con protección de threading
    - Métricas detalladas de operaciones
    - Retry logic para operaciones DB
    - Health checks para ArangoDB y Kafka
    - Error handling específico para ArangoError
    """

    _instance = None
    _lock = threading.Lock()  # Lock para singleton thread-safe

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(
        self,
        on_complete_callback: Optional[Callable[[str], None]] = None,
        on_all_complete_callback: Optional[Callable[[], None]] = None,
        on_task_complete_callback: Optional[Callable[[str, str], None]] = None,
        group_id: Optional[str] = None,
        db_retry_attempts: int = DB_RETRY_ATTEMPTS,
        db_retry_backoff_ms: int = DB_RETRY_BACKOFF_MS,
        health_check_interval: int = HEALTH_CHECK_INTERVAL
    ):
        if self._initialized:
            log.warning("[GestorTareas] ⚠️ Ya inicializado, ignorando nueva configuración")
            return

        # Configuración
        self.db_retry_attempts = db_retry_attempts
        self.db_retry_backoff_ms = db_retry_backoff_ms
        self.health_check_interval = health_check_interval
        
        # Thread-safety para operaciones DB
        self._db_lock = threading.Lock()
        self._closed = False
        
        # Métricas
        self._metrics = defaultdict(int)
        self._last_db_operation = time.time()
        self._last_health_check = time.time()

        # --- Conexión a ArangoDB ---
        try:
            log.info(f"[GestorTareas] 🔄 Conectando a ArangoDB: {ARANGO_HOST}/{ARANGO_DB_COMPAI}")
            self.client = ArangoClient(hosts=ARANGO_HOST)
            self.db = self.client.db(
                ARANGO_DB_COMPAI, username=ARANGO_USER, password=ARANGO_PASSWORD
            )
            if not self.db.has_collection(ARANGO_COLLECTION):
                raise RuntimeError(
                    f"❌ La colección {ARANGO_COLLECTION} no existe en {ARANGO_DB_COMPAI}"
                )
            self.collection = self.db.collection(ARANGO_COLLECTION)
            log.info(f"[GestorTareas] ✅ Conectado a ArangoDB: {ARANGO_HOST}/{ARANGO_DB_COMPAI}")
            self._metrics['db_connections'] += 1
        except ServerConnectionError as e:
            log.error(f"[GestorTareas] ❌ Error de conexión a ArangoDB: {e}")
            raise
        except Exception as e:
            log.error(f"[GestorTareas] ❌ Error inesperado conectando a ArangoDB: {e}")
            raise

        # --- Callbacks opcionales ---
        self.on_complete_callback = on_complete_callback
        self.on_all_complete_callback = on_all_complete_callback
        self.on_task_complete_callback = on_task_complete_callback

        # --- Kafka Producer y Consumer ---
        self.producer = KafkaEventPublisher()
        self.producer_started = False

        if group_id is None:
            group_id = f"job_monitor_group_{uuid.uuid4()}"
        
        self.consumer = KafkaEventConsumer(
            topic=TOPIC_TASK_EVENTS,
            callback=self._on_kafka_message,
            group_id=group_id,
        )
        log.info(f"[GestorTareas] 📬 Consumer configurado para topic: {TOPIC_TASK_EVENTS}")

        # ID de instancia (para evitar procesar mensajes propios)
        self.instance_id = str(uuid.uuid4())
        
        # Health check thread
        self._health_check_thread: Optional[threading.Thread] = None
        self._stop_health_check = threading.Event()

        self._initialized = True
        log.info(f"[GestorTareas] ✅ Instancia única creada correctamente (id={self.instance_id[:8]}...)")

    # ========================================================
    # 🚀 Ciclo de vida
    # ========================================================
    def start(self):
        """Inicia productor, consumidor y health check."""
        try:
            if not self.producer_started:
                self.producer.start()
                self.producer_started = True
                log.info("[GestorTareas] ✅ Producer iniciado")
                self._metrics['producer_starts'] += 1

            if not self.consumer.is_running():
                self.consumer.start()
                log.info("[GestorTareas] ✅ Consumer iniciado")
                self._metrics['consumer_starts'] += 1
            
            # Iniciar health check
            self._start_health_check_thread()
            
            self._closed = False
            log.info("[GestorTareas] 🚀 GestorTareas completamente iniciado")
            
        except Exception as e:
            log.error(f"[GestorTareas] ❌ Error en start(): {e}")
            raise

    def stop(self, timeout: int = 30):
        """
        Detiene el consumer, producer y health check de forma ordenada.
        
        Args:
            timeout: Tiempo máximo en segundos para esperar
        """
        self._closed = True
        
        # Detener health check
        self._stop_health_check.set()
        if self._health_check_thread and self._health_check_thread.is_alive():
            self._health_check_thread.join(timeout=5)
            log.info("[GestorTareas] 🏥 Health check detenido")
        
        # Detener consumer
        if self.consumer:
            try:
                self.consumer.stop()
                log.info("[GestorTareas] ✅ Consumer detenido")
            except Exception as e:
                log.error(f"[GestorTareas] ❌ Error al parar consumer: {e}")

        # Detener producer
        if self.producer_started:
            try:
                self.producer.stop(timeout=timeout)
                self.producer_started = False
                log.info("[GestorTareas] ✅ Producer detenido")
            except Exception as e:
                log.error(f"[GestorTareas] ❌ Error al parar producer: {e}")
        
        # Log métricas finales
        self._log_metrics()
        log.info("[GestorTareas] 🔌 GestorTareas completamente detenido")

    # ========================================================
    # 📦 Gestión de Jobs
    # ========================================================
    def add_job(self, tasks: List[Dict[str, Any]], job_id: Optional[str] = None) -> Optional[str]:
        """
        Añade tareas a un job y publica eventos start_task.
        
        Args:
            tasks: Lista de diccionarios con tareas (cada uno debe tener 'task_id')
            job_id: ID opcional del job (se genera automáticamente si no se provee)
            
        Returns:
            job_id del job creado/actualizado, o None si falla
        """
        if not tasks:
            log.debug("[GestorTareas] ⚠️ Intento de crear un job sin tareas")

        job_id = job_id or str(uuid.uuid4())
        nuevas_tareas = []

        def _add_job_operation():
            nonlocal nuevas_tareas
            
            with self._db_lock:
                if self.collection.has(job_id):
                    job = self.collection.get(job_id)
                    existing_tasks = job.get("tasks", {})

                    for task in tasks:
                        task_id = task.get("task_id")
                        if not task_id:
                            log.warning(f"[GestorTareas] ⚠️ Tarea sin task_id ignorada: {task}")
                            continue
                        if task_id not in existing_tasks:
                            existing_tasks[task_id] = {
                                **{k: v for k, v in task.items() if k != "task_id"},
                                "completed": False,
                            }
                            nuevas_tareas.append(task)

                    job["tasks"] = existing_tasks
                    self.collection.update(job)
                    self._metrics['jobs_updated'] += 1
                    log.info(f"[GestorTareas] ✅ Job '{job_id}' actualizado con {len(nuevas_tareas)} nuevas tareas")
                else:
                    doc = {
                        "_key": job_id,
                        "tasks": {
                            task["task_id"]: {
                                **{k: v for k, v in task.items() if k != "task_id"},
                                "completed": False,
                            }
                            for task in tasks if "task_id" in task
                        },
                    }
                    self.collection.insert(doc)
                    nuevas_tareas = tasks[:]
                    self._metrics['jobs_created'] += 1
                    log.info(f"[GestorTareas] ✅ Job '{job_id}' creado con {len(nuevas_tareas)} tareas")
                
                self._last_db_operation = time.time()

        # Ejecutar con retry
        success = self._retry_db_operation(_add_job_operation, f"add_job(job_id={job_id})")
        
        if not success:
            self._metrics['jobs_failed'] += 1
            return None

        # Publicar inicio de cada tarea
        for task in nuevas_tareas:
            msg = {"job_id": job_id, "task_id": task["task_id"]}
            self._publicar_evento("start_task", msg)

        return job_id
    

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Recupera el documento completo en ArangoDB correspondiente a un job_id.
        
        Args:
            job_id: ID del job a recuperar
            
        Returns:
            Diccionario con los datos del job, o None si no existe
        """
        if not job_id:
            log.error("[GestorTareas] ❌ get_job() llamado sin job_id")
            return None

        job = None
        
        def _get_job_operation():
            nonlocal job
            with self._db_lock:
                if not self.collection.has(job_id):
                    log.warning(f"[GestorTareas] ⚠️ No existe ningún documento con job_id '{job_id}'")
                    return
                
                job = self.collection.get(job_id)
                self._metrics['jobs_retrieved'] += 1
                self._last_db_operation = time.time()
                log.debug(f"[GestorTareas] 📥 Job recuperado: {job_id}")

        success = self._retry_db_operation(_get_job_operation, f"get_job(job_id={job_id})")
        
        if not success:
            self._metrics['get_job_errors'] += 1
        
        return job

    def update_task(self, job_id: str, task_id: str, updates: Dict[str, Any]) -> bool:
        """
        Actualiza campos de una tarea específica dentro de un job.
        
        Args:
            job_id: ID del job
            task_id: ID de la tarea
            updates: Diccionario con campos a actualizar
            
        Returns:
            True si se actualizó exitosamente, False en caso contrario
        """
        success = False
        
        def _update_task_operation():
            nonlocal success
            with self._db_lock:
                job = self.collection.get(job_id)
                if not job or task_id not in job.get("tasks", {}):
                    log.error(f"[GestorTareas] ❌ No se encontró la tarea '{task_id}' en el job '{job_id}'")
                    return
                
                job["tasks"][task_id].update(updates)
                self.collection.update(job)
                self._metrics['tasks_updated'] += 1
                self._last_db_operation = time.time()
                log.info(f"[GestorTareas] ✅ Tarea '{task_id}' del job '{job_id}' actualizada")
                success = True
        
        self._retry_db_operation(_update_task_operation, f"update_task(job={job_id}, task={task_id})")
        return success
    
    def update_job(self, job_id: str, updates: Dict[str, Any]) -> bool:
        """
        Actualiza campos del job.
        
        Args:
            job_id: ID del job
            updates: Diccionario con campos a actualizar
            
        Returns:
            True si se actualizó exitosamente, False en caso contrario
        """
        if job_id is None:
            log.warning("[GestorTareas] ⚠️ Intento de actualizar job con job_id=None")
            return False
            
        success = False
        
        def _update_job_operation():
            nonlocal success
            with self._db_lock:
                job = self.collection.get(job_id)
                if not job:
                    log.error(f"[GestorTareas] ❌ No se encontró el job '{job_id}'")
                    return

                job.update(updates)
                self.collection.update(job)
                self._metrics['jobs_updated'] += 1
                self._last_db_operation = time.time()
                log.info(f"[GestorTareas] ✅ Job '{job_id}' actualizado")
                success = True
        
        self._retry_db_operation(_update_job_operation, f"update_job(job_id={job_id})")
        return success
    
    def get_task_field(self, job_id: str, task_id: str, field: str) -> Any:
        """
        Busca en el job indicado el valor de un campo concreto (field) asociado a un task_id.
        
        Args:
            job_id: ID del job
            task_id: ID de la tarea
            field: Nombre del campo a recuperar
            
        Returns:
            Valor del campo si existe, None en caso contrario
        """
        value = None
        
        def _get_task_field_operation():
            nonlocal value
            with self._db_lock:
                job = self.collection.get(job_id)
                if not job:
                    return
                tasks = job.get("tasks", {})
                if task_id in tasks:
                    value = tasks[task_id].get(field)
                    self._last_db_operation = time.time()
        
        self._retry_db_operation(_get_task_field_operation, f"get_task_field(job={job_id}, task={task_id}, field={field})")
        return value
    
    def get_job_field(self, job_id: str, field: str) -> Any:
        """
        Busca en el job indicado el valor de un campo concreto (field).
        
        Args:
            job_id: ID del job
            field: Nombre del campo a recuperar
            
        Returns:
            Valor del campo si existe, None en caso contrario
        """
        value = None
        
        def _get_job_field_operation():
            nonlocal value
            with self._db_lock:
                job = self.collection.get(job_id)
                if job:
                    value = job.get(field)
                    self._last_db_operation = time.time()
        
        self._retry_db_operation(_get_job_field_operation, f"get_job_field(job_id={job_id}, field={field})")
        return value

    def set_task_field(self, job_id: str, task_id: str, field: str, value: Any) -> bool:
        """
        Modifica o crea un campo dentro del task indicado de un job.
        Si el task no existe, lo crea con el campo indicado.
        
        Args:
            job_id: ID del job
            task_id: ID de la tarea
            field: Nombre del campo
            value: Valor a asignar
            
        Returns:
            True si se actualizó exitosamente, False en caso contrario
        """
        success = False
        
        def _set_task_field_operation():
            nonlocal success
            with self._db_lock:
                job = self.collection.get(job_id)
                if not job:
                    log.error(f"[GestorTareas] ❌ Job '{job_id}' no encontrado para set_task_field")
                    return

                tasks = job.get("tasks", {})
                if task_id not in tasks:
                    tasks[task_id] = {}
                    log.info(f"[GestorTareas] 📝 Creando nueva tarea '{task_id}' en job '{job_id}'")

                tasks[task_id][field] = value
                job["tasks"] = tasks
                self.collection.update(job)
                self._metrics['task_fields_set'] += 1
                self._last_db_operation = time.time()
                success = True
        
        self._retry_db_operation(_set_task_field_operation, f"set_task_field(job={job_id}, task={task_id}, field={field})")
        return success

    def set_job_field(self, job_id: str, field: str, value: Any) -> bool:
        """
        Modifica o crea un campo dentro del job indicado.
        
        Args:
            job_id: ID del job
            field: Nombre del campo
            value: Valor a asignar
            
        Returns:
            True si se actualizó exitosamente, False en caso contrario
        """
        success = False
        
        def _set_job_field_operation():
            nonlocal success
            with self._db_lock:
                job = self.collection.get(job_id)
                if not job:
                    log.error(f"[GestorTareas] ❌ Job '{job_id}' no encontrado para set_job_field")
                    return

                job[field] = value
                self.collection.update(job)
                self._metrics['job_fields_set'] += 1
                self._last_db_operation = time.time()
                success = True
        
        self._retry_db_operation(_set_job_field_operation, f"set_job_field(job_id={job_id}, field={field})")
        return success

    # ========================================================
    # 📬 Publicación y recepción de eventos
    # ========================================================
    def _publicar_evento(self, event_type: str, data: Dict[str, Any]) -> bool:
        """
        Publica un evento genérico en el tópico único.
        
        Args:
            event_type: Tipo de evento ('start_task', 'end_task', 'end_job', etc.)
            data: Datos del evento
            
        Returns:
            True si se publicó exitosamente, False en caso contrario
        """
        if not self.producer_started:
            log.warning("[GestorTareas] ⚠️ Producer no iniciado, arrancando automáticamente...")
            try:
                self.start()
            except Exception as e:
                log.error(f"[GestorTareas] ❌ Error arrancando producer: {e}")
                self._metrics['publish_errors'] += 1
                return False

        msg = {
            "event_type": event_type,
            **data,
            "origin": self.instance_id,
        }
        
        success = self.producer.publish(TOPIC_TASK_EVENTS, msg)
        
        if success:
            self._metrics['events_published'] += 1
            log.debug(f"[GestorTareas] 📤 Evento publicado: {event_type} ({data})")
        else:
            self._metrics['publish_errors'] += 1
            log.error(f"[GestorTareas] ❌ Error publicando evento: {event_type}")
        
        return success


    def _on_kafka_message(self, message, *args, **kwargs):
        """Procesa mensajes de Kafka."""
        try:
            # Compatibilidad con mensajes tipo dict o Kafka RawMessage
            if isinstance(message, dict):
                data = message
            elif hasattr(message, "value"):
                data = json.loads(message.value.decode("utf-8"))
            else:
                log.warning(f"[GestorTareas] ⚠️ Formato de mensaje desconocido: {type(message)}")
                self._metrics['invalid_messages'] += 1
                return

            event_type = data.get("event_type")
            origin = data.get("origin")
            job_id = data.get("job_id")
            task_id = data.get("task_id")

            # Ignorar mensajes producidos por esta misma instancia
            if origin == self.instance_id:
                log.debug(f"[GestorTareas] 🔁 Ignorando mensaje propio: {event_type}")
                self._metrics['self_messages_ignored'] += 1
                return

            log.info(f"[GestorTareas] 📬 Evento recibido: {event_type} job={job_id}, task={task_id}")
            self._metrics['messages_received'] += 1

            # Manejo de tipos de evento
            if event_type == "end_task":
                self.task_completed(job_id, task_id)
                self._metrics['end_task_events'] += 1
            elif event_type == "end_job" and self.on_complete_callback:
                try:
                    self.on_complete_callback(job_id)
                    self._metrics['job_callbacks_executed'] += 1
                except Exception as e:
                    log.error(f"[GestorTareas] ❌ Error en on_complete_callback: {e}")
                    self._metrics['callback_errors'] += 1
            elif event_type == "all_jobs_complete" and self.on_all_complete_callback:
                try:
                    self.on_all_complete_callback()
                    self._metrics['all_complete_callbacks_executed'] += 1
                except Exception as e:
                    log.error(f"[GestorTareas] ❌ Error en on_all_complete_callback: {e}")
                    self._metrics['callback_errors'] += 1

        except json.JSONDecodeError as e:
            log.error(f"[GestorTareas] ❌ Error decodificando JSON: {e}")
            self._metrics['json_decode_errors'] += 1
        except Exception as e:
            log.error(f"[GestorTareas] ❌ Error procesando mensaje Kafka: {e}")
            self._metrics['message_processing_errors'] += 1


    # ========================================================
    # 🧩 Lógica de finalización de tarea al llegar evento
    # ========================================================
    def task_completed(self, job_id: str, task_id: str):
        """
        Marca una tarea como completada y verifica si el job está completo.
        
        Args:
            job_id: ID del job
            task_id: ID de la tarea completada
        """
        all_completed = False
        
        def _task_completed_operation():
            nonlocal all_completed
            with self._db_lock:
                job = self.collection.get(job_id)
                if not job or task_id not in job.get("tasks", {}):
                    log.error(f"[GestorTareas] ❌ No se encontró tarea '{task_id}' en job '{job_id}'")
                    self._metrics['task_complete_errors'] += 1
                    return

                # Marcar la tarea completada
                job["tasks"][task_id]["completed"] = True
                self.collection.update(job)
                self._metrics['tasks_completed'] += 1
                self._last_db_operation = time.time()
                log.info(f"[GestorTareas] ✅ Tarea '{task_id}' completada en job '{job_id}'")

                # Verificar si todas las tareas están completadas
                all_completed = all(t.get("completed", False) for t in job["tasks"].values())
        
        success = self._retry_db_operation(_task_completed_operation, f"task_completed(job={job_id}, task={task_id})")
        
        if not success:
            return
        
        # Callback (fuera del lock para evitar deadlocks)
        if self.on_task_complete_callback:
            try:
                self.on_task_complete_callback(job_id, task_id)
                self._metrics['task_callbacks_executed'] += 1
            except Exception as e:
                log.error(f"[GestorTareas] ❌ Error en on_task_complete_callback: {e}")
                self._metrics['callback_errors'] += 1

        # Si todas completadas, procesar job completado
        if all_completed:
            self.job_completed(job_id)


    def job_completed(self, job_id: str):
        """
        Procesa la finalización de un job completo.
        
        Args:
            job_id: ID del job completado
        """
        def _job_completed_operation():
            with self._db_lock:
                # Recargar el job para obtener el nuevo _rev
                job = self.collection.get(job_id)
                if not job:
                    log.error(f"[GestorTareas] ❌ Job '{job_id}' no encontrado en job_completed")
                    return

                # Marcar la historia con interpretado a done
                historia = job.get("metadata", {})
                if "clasificacion" in historia and "status" in historia["clasificacion"]:
                    historia \
                        .setdefault("clasificacion", {}) \
                        .setdefault("status", {})[estado_tarea["INTERPRETADO"]] = "done"

                # Actualizar la historia en base de datos
                self.collection.update(job)
                self._metrics['jobs_completed'] += 1
                self._last_db_operation = time.time()
                log.info(f"[GestorTareas] 🎉 Job '{job_id}' completado")
        
        success = self._retry_db_operation(_job_completed_operation, f"job_completed(job_id={job_id})")
        
        if success:
            # Publicar evento de fin de job (fuera del lock)
            self._publicar_evento("end_job", {"job_id": job_id})


    def enviar_a_dlq(self, job_id: str, razon: str, payload: Optional[Dict[str, Any]] = None) -> bool:
        """
        Envía un job a la Dead Letter Queue de Compai.
        
        Args:
            job_id: ID del job con error
            razon: Razón por la que se envía a DLQ
            payload: Datos adicionales opcionales
            
        Returns:
            True si se envió exitosamente, False en caso contrario
        """
        mensaje = {
            "job_id": job_id,
            "razon": razon,
            "payload": payload or {},
            "timestamp": time.time()
        }

        success = self.producer.publish(DLQ_COMPAI, mensaje)
        
        if success:
            self._metrics['dlq_sent'] += 1
            log.info(f"[GestorTareas] 💀 job_id={job_id} enviado a DLQ_COMPAI | razón: {razon}")
        else:
            self._metrics['dlq_errors'] += 1
            log.error(f"[GestorTareas] ❌ Error enviando job_id={job_id} a DLQ_COMPAI")
        
        return success

    def enviar_a_dlq_consumer(self, job_id: str, razon: str, payload: Optional[Dict[str, Any]] = None) -> bool:
        """
        Envía un job a la Dead Letter Queue de Consumer.
        
        Args:
            job_id: ID del job con error
            razon: Razón por la que se envía a DLQ
            payload: Datos adicionales opcionales
            
        Returns:
            True si se envió exitosamente, False en caso contrario
        """
        mensaje = {
            "job_id": job_id,
            "razon": razon,
            "payload": payload or {},
            "timestamp": time.time()
        }

        success = self.producer.publish(DLQ_CONSUMER, mensaje)
        
        if success:
            self._metrics['dlq_consumer_sent'] += 1
            log.info(f"[GestorTareas] 💀 job_id={job_id} enviado a DLQ_CONSUMER | razón: {razon}")
        else:
            self._metrics['dlq_consumer_errors'] += 1
            log.error(f"[GestorTareas] ❌ Error enviando job_id={job_id} a DLQ_CONSUMER")
        
        return success

    # ========================================================
    # 🔧 Métodos auxiliares
    # ========================================================
    def _retry_db_operation(self, operation: Callable, operation_name: str) -> bool:
        """
        Ejecuta una operación de DB con retry logic.
        
        Args:
            operation: Función a ejecutar
            operation_name: Nombre descriptivo para logging
            
        Returns:
            True si la operación se completó exitosamente, False en caso contrario
        """
        for attempt in range(1, self.db_retry_attempts + 1):
            try:
                operation()
                return True
            except DocumentGetError as e:
                log.error(f"[GestorTareas] ❌ Error obteniendo documento en {operation_name}: {e}")
                self._metrics['db_get_errors'] += 1
                return False  # No reintentar para errores de documento no encontrado
            except DocumentInsertError as e:
                log.error(f"[GestorTareas] ❌ Error insertando documento en {operation_name}: {e}")
                self._metrics['db_insert_errors'] += 1
                if attempt < self.db_retry_attempts:
                    backoff = (self.db_retry_backoff_ms * attempt) / 1000.0
                    log.warning(f"[GestorTareas] 🔄 Reintento {attempt}/{self.db_retry_attempts} en {backoff}s...")
                    time.sleep(backoff)
                else:
                    return False
            except DocumentUpdateError as e:
                log.error(f"[GestorTareas] ❌ Error actualizando documento en {operation_name}: {e}")
                self._metrics['db_update_errors'] += 1
                if attempt < self.db_retry_attempts:
                    backoff = (self.db_retry_backoff_ms * attempt) / 1000.0
                    log.warning(f"[GestorTareas] 🔄 Reintento {attempt}/{self.db_retry_attempts} en {backoff}s...")
                    time.sleep(backoff)
                else:
                    return False
            except ServerConnectionError as e:
                log.error(f"[GestorTareas] ❌ Error de conexión a servidor en {operation_name}: {e}")
                self._metrics['db_connection_errors'] += 1
                if attempt < self.db_retry_attempts:
                    backoff = (self.db_retry_backoff_ms * attempt) / 1000.0
                    log.warning(f"[GestorTareas] 🔄 Reintento {attempt}/{self.db_retry_attempts} en {backoff}s...")
                    time.sleep(backoff)
                else:
                    return False
            except ArangoError as e:
                log.error(f"[GestorTareas] ❌ Error de ArangoDB en {operation_name}: {e}")
                self._metrics['db_arango_errors'] += 1
                if attempt < self.db_retry_attempts:
                    backoff = (self.db_retry_backoff_ms * attempt) / 1000.0
                    log.warning(f"[GestorTareas] 🔄 Reintento {attempt}/{self.db_retry_attempts} en {backoff}s...")
                    time.sleep(backoff)
                else:
                    return False
            except Exception as e:
                log.error(f"[GestorTareas] ❌ Error inesperado en {operation_name}: {e}")
                self._metrics['db_unknown_errors'] += 1
                if attempt < self.db_retry_attempts:
                    backoff = (self.db_retry_backoff_ms * attempt) / 1000.0
                    log.warning(f"[GestorTareas] 🔄 Reintento {attempt}/{self.db_retry_attempts} en {backoff}s...")
                    time.sleep(backoff)
                else:
                    return False
        
        return False

    def get_metrics(self) -> Dict[str, Any]:
        """
        Retorna métricas actuales del gestor.
        
        Returns:
            Diccionario con todas las métricas
        """
        return {
            **dict(self._metrics),
            'last_db_operation': self._last_db_operation,
            'last_health_check': self._last_health_check,
            'is_closed': self._closed,
            'producer_started': self.producer_started,
            'instance_id': self.instance_id[:8] + "..."
        }

    def _log_metrics(self):
        """Log de métricas para debugging."""
        metrics = self.get_metrics()
        log.info(
            f"[GestorTareas] 📊 Métricas: "
            f"jobs_created={metrics.get('jobs_created', 0)}, "
            f"jobs_completed={metrics.get('jobs_completed', 0)}, "
            f"tasks_completed={metrics.get('tasks_completed', 0)}, "
            f"events_published={metrics.get('events_published', 0)}, "
            f"messages_received={metrics.get('messages_received', 0)}, "
            f"db_errors={metrics.get('db_connection_errors', 0) + metrics.get('db_update_errors', 0)}, "
            f"dlq_sent={metrics.get('dlq_sent', 0)}"
        )

    def _start_health_check_thread(self):
        """Inicia hilo de health check en background."""
        if self.health_check_interval <= 0:
            log.info("[GestorTareas] 🏥 Health check deshabilitado (intervalo <= 0)")
            return
        
        self._stop_health_check.clear()
        self._health_check_thread = threading.Thread(
            target=self._health_check_loop,
            name="gestor-tareas-health-check",
            daemon=True
        )
        self._health_check_thread.start()
        log.info(f"[GestorTareas] 🏥 Health check iniciado (intervalo: {self.health_check_interval}s)")

    def _health_check_loop(self):
        """Loop de health check periódico para ArangoDB y Kafka."""
        while not self._stop_health_check.is_set():
            try:
                time.sleep(self.health_check_interval)
                
                if self._stop_health_check.is_set():
                    break
                
                # Health check de ArangoDB
                try:
                    with self._db_lock:
                        # Verificar que la conexión esté viva
                        self.db.version()
                        self._metrics['health_check_db_success'] += 1
                        log.debug("[GestorTareas] 🏥 Health check DB OK")
                except ServerConnectionError as e:
                    log.error(f"[GestorTareas] 🏥 Health check DB FAILED: {e}")
                    self._metrics['health_check_db_failures'] += 1
                except Exception as e:
                    log.error(f"[GestorTareas] 🏥 Health check DB error: {e}")
                    self._metrics['health_check_db_failures'] += 1
                
                # Health check de Producer
                try:
                    if self.producer_started and not self.producer.is_closed():
                        self._metrics['health_check_producer_success'] += 1
                        log.debug("[GestorTareas] 🏥 Health check Producer OK")
                    else:
                        log.warning("[GestorTareas] 🏥 Health check Producer: no iniciado o cerrado")
                        self._metrics['health_check_producer_warnings'] += 1
                except Exception as e:
                    log.error(f"[GestorTareas] 🏥 Health check Producer error: {e}")
                    self._metrics['health_check_producer_failures'] += 1
                
                # Health check de Consumer
                try:
                    if self.consumer.is_running():
                        self._metrics['health_check_consumer_success'] += 1
                        log.debug("[GestorTareas] 🏥 Health check Consumer OK")
                    else:
                        log.warning("[GestorTareas] 🏥 Health check Consumer: no está corriendo")
                        self._metrics['health_check_consumer_warnings'] += 1
                except Exception as e:
                    log.error(f"[GestorTareas] 🏥 Health check Consumer error: {e}")
                    self._metrics['health_check_consumer_failures'] += 1
                
                self._last_health_check = time.time()
                
            except Exception as e:
                log.error(f"[GestorTareas] ❌ Error en health check loop: {e}")

    def is_healthy(self) -> bool:
        """
        Verifica si el gestor está en estado saludable.
        
        Returns:
            True si todos los componentes están funcionando, False en caso contrario
        """
        try:
            # Verificar DB
            with self._db_lock:
                self.db.version()
            
            # Verificar Producer
            if self.producer_started and self.producer.is_closed():
                return False
            
            # Verificar Consumer
            if not self.consumer.is_running():
                return False
            
            return True
        except Exception as e:
            log.error(f"[GestorTareas] ❌ Error verificando salud: {e}")
            return False


# ========================================================
# 🧪 Ejemplo de uso
# ========================================================
def on_task_complete(job_id, task_id):
    print(f"🔹 Callback: Tarea {task_id} del job {job_id} completada.")

def on_job_complete(job_id):
    print(f"✅ Callback: Job {job_id} completado.")

def on_all_jobs_complete():
    print("🌍 Callback: Todos los jobs completados.")


if __name__ == "__main__":

    gestor = GestorTareas(
        on_complete_callback=on_job_complete,
        on_all_complete_callback=on_all_jobs_complete,
        on_task_complete_callback=on_task_complete,
    )

    gestor.start()

    tasks = [
        {"task_id": "task_1", "card_id": "card_1"},
        {"task_id": "task_2", "card_id": "card_2"},
    ]

    job_id = gestor.add_job(tasks)

    for task in tasks:
        gestor.task_completed(job_id, task["task_id"])

    gestor.stop()
