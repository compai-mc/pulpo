import json
import uuid
import os
import sys
from pathlib import Path
from arango import ArangoClient
from pulpo.logueador import log

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


# ========================================================
# 🧠 Clase principal: GestorTareas
# ========================================================
class GestorTareas:
    """Gestor centralizado de tareas y jobs usando un único tópico Kafka."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
        on_complete_callback=None,
        on_all_complete_callback=None,
        on_task_complete_callback=None,
        group_id=f"job_monitor_group_{uuid.uuid4()}",
    ):
        if self._initialized:
            log.warning("[GestorTareas] Ya inicializado, ignorando nueva configuración")
            return

        # --- Conexión a ArangoDB ---
        try:
            self.client = ArangoClient(hosts=ARANGO_HOST)
            self.db = self.client.db(
                ARANGO_DB_COMPAI, username=ARANGO_USER, password=ARANGO_PASSWORD
            )
            if not self.db.has_collection(ARANGO_COLLECTION):
                raise RuntimeError(
                    f"La colección {ARANGO_COLLECTION} no existe en {ARANGO_DB_COMPAI}"
                )
            self.collection = self.db.collection(ARANGO_COLLECTION)
            log.info(f"[GestorTareas] Conectado a ArangoDB: {ARANGO_HOST}/{ARANGO_DB_COMPAI}")
        except Exception as e:
            log.error(f"[GestorTareas] Error conectando a ArangoDB: {e}")
            raise

        # --- Callbacks opcionales ---
        self.on_complete_callback = on_complete_callback
        self.on_all_complete_callback = on_all_complete_callback
        self.on_task_complete_callback = on_task_complete_callback

        # --- Kafka Producer y Consumer ---
        self.producer = KafkaEventPublisher()
        self.producer_started = False

        self.consumer = KafkaEventConsumer(
            topic=TOPIC_TASK_EVENTS,
            callback=self._on_kafka_message,
            group_id=group_id,
        )
        log.info(f"[GestorTareas] Consumer configurado para topic: {TOPIC_TASK_EVENTS}")

        # ID de instancia (para evitar procesar mensajes propios)
        self.instance_id = str(uuid.uuid4())

        self._initialized = True
        log.info("[GestorTareas] Instancia única creada correctamente")

    # ========================================================
    # 🚀 Ciclo de vida
    # ========================================================
    def start(self):
        """Inicia productor y consumidor."""
        try:
            if not self.producer_started:
                self.producer.start()
                self.producer_started = True
                log.info("[GestorTareas] Producer iniciado")

            if not getattr(self.consumer, "_running", False):
                self.consumer.start()
                log.info("[GestorTareas] Consumer iniciado")
        except Exception as e:
            log.error(f"[GestorTareas] Error en start(): {e}")
            raise

    def stop(self):
        """Detiene el consumer y el producer."""
        if self.consumer:
            try:
                self.consumer.stop()
                log.info("[GestorTareas] Consumer detenido")
            except Exception as e:
                log.error(f"[GestorTareas] Error al parar consumer: {e}")

        if self.producer_started:
            try:
                self.producer.stop()
                self.producer_started = False
                log.info("[GestorTareas] Producer detenido")
            except Exception as e:
                log.error(f"[GestorTareas] Error al parar producer: {e}")

    # ========================================================
    # 📦 Gestión de Jobs
    # ========================================================
    def add_job(self, tasks: list[dict], job_id: str = None):
        """Añade tareas a un job y publica eventos start_task."""
        if not tasks:
            log.debug("[GestorTareas] Intento de crear un job sin tareas")

        job_id = job_id or str(uuid.uuid4())
        nuevas_tareas = []

        try:
            if self.collection.has(job_id):
                job = self.collection.get(job_id)
                existing_tasks = job.get("tasks", {})

                for task in tasks:
                    task_id = task.get("task_id")
                    if not task_id:
                        continue
                    if task_id not in existing_tasks:
                        existing_tasks[task_id] = {
                            **{k: v for k, v in task.items() if k != "task_id"},
                            "completed": False,
                        }
                        nuevas_tareas.append(task)

                job["tasks"] = existing_tasks
                self.collection.update(job)
                log.info(f"[GestorTareas] Job '{job_id}' actualizado con {len(nuevas_tareas)} nuevas tareas")
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
                log.info(f"[GestorTareas] Job '{job_id}' creado con {len(nuevas_tareas)} tareas")
        except Exception as e:
            log.error(f"[GestorTareas] Error creando job '{job_id}': {e}")
            return None

        # Publicar inicio de cada tarea
        for task in nuevas_tareas:
            msg = {"job_id": job_id, "task_id": task["task_id"]}
            self._publicar_evento("start_task", msg)

        return job_id
    

    def get_job(self, job_id: str) -> dict | None:
        """
        Recupera el documento completo en ArangoDB correspondiente a un job_id.
        Devuelve el dict con los datos del job, o None si no existe.
        """
        try:
            if not job_id:
                log.error("[GestorTareas] get_job() llamado sin job_id")
                return None

            if not self.collection.has(job_id):
                log.warning(f"[GestorTareas] No existe ningún documento con job_id '{job_id}'")
                return None

            job = self.collection.get(job_id)
            log.debug(f"[GestorTareas] Job recuperado: {job_id}")
            return job

        except Exception as e:
            log.error(f"[GestorTareas] Error recuperando job '{job_id}': {e}")
            return None

    def update_task(self, job_id: str, task_id: str, updates: dict):
        job = self.collection.get(job_id)
        if not job or task_id not in job["tasks"]:
            log.error(f"[!] No se encontró la tarea '{task_id}' en el job '{job_id}'")
            return False
        job["tasks"][task_id].update(updates)
        self.collection.update(job)
        log.info(f"[✔] Tarea '{task_id}' del job '{job_id}' actualizada")
        return True
    
    def update_job(self, job_id: str, updates: dict):
        job = self.collection.get(job_id)
        if not job:
            log.error(f"[!] No se encontró el job '{job_id}'")
            return False

        job.update(updates)
        self.collection.update(job)
        log.info(f"[✔] Job '{job_id}' actualizado")
        return True
    
    def get_task_field(self, job_id: str, task_id: str, field: str):
        """
        Busca en el job indicado el valor de un campo concreto (field) asociado a un task_id.
        Devuelve el valor si lo encuentra, si no None.
        """
        job = self.collection.get(job_id)
        if not job:
            return None
        tasks = job.get("tasks", {})
        if task_id in tasks:
            return tasks[task_id].get(field)
        return None
    
    def set_task_field(self, job_id: str, task_id: str, field: str, value):
        """
        Modifica o crea un campo dentro del task indicado de un job.
        Si el job y el task existen, actualiza el campo y guarda el documento.
        Si el task no existe, lo crea con el campo indicado.
        """
        job = self.collection.get(job_id)
        if not job:
            return False  

        tasks = job.get("tasks", {})

        if task_id not in tasks:
            tasks[task_id] = {}

        tasks[task_id][field] = value
        job["tasks"] = tasks

        self.collection.update(job)  
        return True

    # ========================================================
    # 📬 Publicación y recepción de eventos
    # ========================================================
    def _publicar_evento(self, event_type: str, data: dict):
        """Publica un evento genérico en el tópico único."""
        if not self.producer_started:
            log.warning("[GestorTareas] Producer no iniciado, arrancando automáticamente...")
            self.start()

        msg = {
            "event_type": event_type,
            **data,
            "origin": self.instance_id,
        }
        self.producer.publish(TOPIC_TASK_EVENTS, msg)
        log.debug(f"[GestorTareas] Evento publicado: {event_type} ({data})")


    def _on_kafka_message(self, message, *args, **kwargs):
        """Procesa mensajes de Kafka."""
        try:
            # 🧩 Compatibilidad con mensajes tipo dict o Kafka RawMessage
            if isinstance(message, dict):
                data = message
            elif hasattr(message, "value"):
                data = json.loads(message.value.decode("utf-8"))
            else:
                log.warning(f"[GestorTareas] Formato de mensaje desconocido: {type(message)}")
                return

            event_type = data.get("event_type")
            origin = data.get("origin")

            # Ignorar mensajes producidos por esta misma instancia
            if origin == self.instance_id:
                return  

            job_id = data.get("job_id")
            task_id = data.get("task_id")

            log.info(f"[Kafka] Evento recibido: {event_type} job={job_id}, task={task_id}")

            # 🚦 Manejo de tipos de evento
            if event_type == "end_task":
                self.task_completed(job_id, task_id)
            elif event_type == "end_job" and self.on_complete_callback:
                self.on_complete_callback(job_id)
            elif event_type == "all_jobs_complete" and self.on_all_complete_callback:
                self.on_all_complete_callback()

        except Exception as e:
            log.error(f"[GestorTareas] Error procesando mensaje Kafka: {e}")


    # ========================================================
    # 🧩 Lógica de finalización de tarea al llegar evento
    # ========================================================
    def task_completed(self, job_id: str, task_id: str):
        """Marca una tarea como completada y publica los eventos asociados."""
        try:
            job = self.collection.get(job_id)
            if not job or task_id not in job["tasks"]:
                log.error(f"[GestorTareas] No se encontró tarea '{task_id}' en job '{job_id}'")
                return

            job["tasks"][task_id]["completed"] = True
            self.collection.update(job)
            log.info(f"[GestorTareas] ✔ Tarea '{task_id}' completada en job '{job_id}'")

            # Callback por tarea
            if self.on_task_complete_callback:
                self.on_task_complete_callback(job_id, task_id)

            # ¿Está todo completado? --> actualizo job yenvio evento end_job
            if all(t["completed"] for t in job["tasks"].values()):
            
                # Cambia el estado de la historia de INTERPRETADO a done
                historia = job.get("metadata", {})
                if "clasificacion" in historia and "status" in historia["clasificacion"]:
                    historia \
                        .setdefault("clasificacion", {}) \
                        .setdefault("status", {})[estado_tarea["INTERPRETADO"]] = "done"
                self.collection.update(job)

                #  Publicar mensaje de job completado  
                self._publicar_evento("end_job", {"job_id": job_id})
                log.info(f"[GestorTareas] 🎉 Job '{job_id}' completado")

        except Exception as e:
            log.error(f"[GestorTareas] Error en task_completed: {e}")


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
