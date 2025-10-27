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

# Importar el productor y consumidor
from publicador.publicador import KafkaEventPublisher
from consumidor.consumidor import KafkaEventConsumer
from pulpo.util.util import require_env
# ========================================================
# 🔧 Configuración
# ========================================================
ARANGO_HOST = require_env("ARANGO_URL")
ARANGO_DB_COMPAI = require_env("ARANGO_DB_COMPAI")
ARANGO_USER = require_env("ARANGO_USER")
ARANGO_PASSWORD = require_env("ARANGO_PASS")
ARANGO_COLLECTION = require_env("ARANGO_COLLECTION_TAREAS")

TOPIC_TASK = require_env("TOPIC_TASK")
TOPIC_END_TASK = require_env("TOPIC_END_TASK")
TOPIC_END_JOB = require_env("TOPIC_END_JOB")
TOPIC_END_JOBS = require_env("TOPIC_END_JOBS")

# ========================================================
# 🧠 Clase principal: GestorTareas
# ========================================================
class GestorTareas:
    """Gestor centralizado de tareas y jobs, con soporte Singleton."""

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(
        self,
        topic_finalizacion_tareas: str = TOPIC_END_TASK,
        topic_finalizacion_global: str = TOPIC_END_JOB,
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

        # --- Kafka Producer ---
        self.producer = KafkaEventPublisher()
        self.producer_started = False

        # --- Kafka Consumer (si hay callbacks) ---
        self.consumer = None
        if any([on_complete_callback, on_all_complete_callback, on_task_complete_callback]):
            self.consumer = KafkaEventConsumer(
                topic=topic_finalizacion_tareas,
                callback=self._on_kafka_message,
                group_id=group_id,
            )
            log.info(f"[GestorTareas] Consumer configurado para topic: {topic_finalizacion_tareas}")
        else:
            log.info("[GestorTareas] Sin callbacks configurados, consumer no creado")

        self._initialized = True
        log.info("[GestorTareas] Instancia única creada correctamente")

    # ========================================================
    # 🚀 Ciclo de vida
    # ========================================================
    def start(self):
        """Inicia el productor y el consumidor (si lo hay)."""
        try:
            if not self.producer_started:
                self.producer.start()
                self.producer_started = True
                log.info("[GestorTareas] Producer iniciado")

            if self.consumer and not getattr(self.consumer, "_running", False):
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
        """Añade tareas a un job (nuevo o existente) y publica mensajes de inicio."""
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

        # Publicar las tareas
        for task in nuevas_tareas:
            msg = {"job_id": job_id, "task_id": task["task_id"], "action": "start_task"}
            self._publicar_tarea(msg)

        return job_id
    
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
    # 📬 Publicación y eventos
    # ========================================================
    def _publicar_tarea(self, msg: dict):
        """Publica una tarea asegurando que el productor esté activo."""
        if not self.producer_started:
            log.warning("[GestorTareas] Producer no iniciado, arrancando automáticamente...")
            self.start()
        self.producer.publish(TOPIC_TASK, msg)

    def _on_kafka_message2(self, message, *args, **kwargs):
        """Procesa mensajes de Kafka sobre tareas completadas."""
        try:
            data = json.loads(message.value.decode("utf-8"))
            job_id = data.get("job_id")
            task_id = data.get("task_id")

            log.info(f"[GestorTareas] [Kafka] Mensaje recibido: {data}")

            if job_id and task_id:
                self.task_completed(job_id, task_id)
        except Exception as e:
            log.error(f"[GestorTareas] Error procesando mensaje Kafka: {e}")

    def _on_kafka_message(self, message, *args, **kwargs):
        """Procesa mensajes de Kafka sobre tareas completadas."""
        try:
            # Si el mensaje es un objeto Kafka (tiene .value)
            if hasattr(message, "value"):
                data = json.loads(message.value.decode("utf-8"))
            else:
                # Si ya es un dict o string
                data = message if isinstance(message, dict) else json.loads(message)

            job_id = data.get("job_id")
            task_id = data.get("task_id")

            log.info(f"[GestorTareas] [Kafka] Mensaje recibido: {data}")

            if job_id and task_id:
                self.task_completed(job_id, task_id)

        except Exception as e:
            log.error(f"[GestorTareas] Error procesando mensaje Kafka: {e}")


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

            self.producer.publish(
                TOPIC_END_TASK,
                {"job_id": job_id, "task_id": task_id, "status": "completed", "uuid": str(uuid.uuid4())},
            )

            if all(t["completed"] for t in job["tasks"].values()):
                self.producer.publish(
                    TOPIC_END_JOB,
                    {"job_id": job_id, "status": "completed", "uuid": str(uuid.uuid4())},
                )
                log.info(f"[GestorTareas] 🎉 Job '{job_id}' completado")

                if self.on_complete_callback:
                    if self.on_complete_callback(job_id): 
                        self.consumer._consumer.commit()
                        log.debug(f"[GestorTareas] Commit realizado tras on_complete_callback para job '{job_id}'")

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
