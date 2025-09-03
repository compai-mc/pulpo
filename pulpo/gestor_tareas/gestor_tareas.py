import asyncio
import json
import uuid
from arango import ArangoClient
import os
import sys
from pathlib import Path 

# Añadir el directorio raíz del proyecto al path de Python
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from consumidor import KafkaEventConsumer
from publicador import KafkaEventPublisher
#from consumidor.consumidor import KafkaEventConsumer
#from publicador.publicador import KafkaEventPublisher

ARANGO_HOST = os.getenv("ARANGO_HOST", "http://alcazar:8529")
ARANGO_DB_COMPAI = os.getenv("ARANGO_DB_COMPAI", "compai_db")
ARANGO_USER = os.getenv("ARANGO_USER", "root")
ARANGO_PASSWORD = os.getenv("ARANGO_PASSWORD", "sabbath")
ARANGO_COLLECTION = os.getenv("ARANGO_COLLECTION", "tareas")

TOPIC_TASK = os.getenv("TOPIC_TASK", "job.task.start")
TOPIC_END_TASK = os.getenv("TOPIC_END_TASK", "job.task.completed")
TOPIC_END_JOB = os.getenv("TOPIC_END_JOB", "job.completed")
TOPIC_END_JOBS = os.getenv("TOPIC_END_JOBS", "jobs.all.completed")

class GestorTareas:
    def __init__(
        self,
        topic_finalizacion_tareas: str = TOPIC_END_TASK,
        topic_finalizacion_global: str = TOPIC_END_JOB,
        on_complete_callback=None,
        on_all_complete_callback=None,
        on_task_complete_callback=None,
        id_grupo="job_monitor_group"
    ):
        # si no hay callbacks definidos, NO creo un consumer 
        self.crear_consumer =  on_complete_callback or on_all_complete_callback or on_task_complete_callback

        self.topic_finalizacion_tareas = topic_finalizacion_tareas
        self.topic_finalizacion_global = topic_finalizacion_global

        self.client = ArangoClient(hosts=ARANGO_HOST)
        self.db = self.client.db(ARANGO_DB_COMPAI, username=ARANGO_USER, password=ARANGO_PASSWORD)
        
        print(f"[INFO] Conectado a DB: {self.db.name}")
        print("[INFO] Colecciones disponibles:", [c["name"] for c in self.db.collections()])

        if not self.db.has_collection(ARANGO_COLLECTION):
            raise RuntimeError(f"La colección {ARANGO_COLLECTION} no existe en la DB {ARANGO_DB_COMPAI}")

        self.collection = self.db.collection(ARANGO_COLLECTION)

        self.consumer = None
        self.producer = KafkaEventPublisher()
        if self.crear_consumer: 
            self.consumer = KafkaEventConsumer(
                topic=self.topic_finalizacion_tareas,
                callback=self._on_kafka_message,
                id_grupo=id_grupo
            )

        self.on_complete_callback = on_complete_callback
        self.on_all_complete_callback = on_all_complete_callback
        self.on_task_complete_callback = on_task_complete_callback

    async def start(self):
        await self.producer.start()
        if self.crear_consumer:
            await self.consumer.start()

    async def stop(self):
        if self.crear_consumer:
            try:
                await self.consumer.stop()
            except Exception as e:
                print(f"[!] Problema al parar consumer Kafka: {e}")
        await self.producer.stop()

    async def add_job(self, tasks: list[dict], job_id: str = None):
        if job_id is None:
            job_id = str(uuid.uuid4())

        nuevas_tareas = []
        if self.collection.has(job_id):
            job = self.collection.get(job_id)
            existing_tasks = job.get("tasks", {})
            for task in tasks:
                if task["task_id"] not in existing_tasks:
                    existing_tasks[task["task_id"]] = {
                        **{k: v for k, v in task.items() if k != "task_id"},
                        "completed": False
                    }
                    nuevas_tareas.append(task)
            job["tasks"] = existing_tasks
            self.collection.update(job)
        else:
            # Si no existe, crea el job con las tareas nuevas
            doc = {
                "_key": job_id,
                "tasks": {
                    task["task_id"]: {
                        **{k: v for k, v in task.items() if k != "task_id"},
                        "completed": False
                    }
                    for task in tasks
                }
            }
            self.collection.insert(doc)
            nuevas_tareas = tasks  # Todas son nuevas

        print(f"Job '{job_id}' actualizado/creado con tareas: {[task['task_id'] for task in nuevas_tareas]}")

        # Publicar inicio de tareas SOLO para las nuevas
        tasks_msgs = []
        for task in nuevas_tareas:
            msg = {
                "job_id": job_id,
                "task_id": task["task_id"],
                "action": "start_task"
            }
            tasks_msgs.append(self._publicar_tarea(msg))
        await asyncio.gather(*tasks_msgs)

        return job_id

    def update_task(self, job_id: str, task_id: str, updates: dict):
        job = self.collection.get(job_id)
        if not job or task_id not in job["tasks"]:
            print(f"[!] No se encontró la tarea '{task_id}' en el job '{job_id}'")
            return False
        job["tasks"][task_id].update(updates)
        self.collection.update(job)
        print(f"[✔] Tarea '{task_id}' del job '{job_id}' actualizada con {updates}")
        return True
    
    def update_job(self, job_id: str, updates: dict):
        job = self.collection.get(job_id)
        if not job:
            print(f"[!] No se encontró el job '{job_id}'")
            return False

        job.update(updates)
        self.collection.update(job)
        print(f"[✔] Job '{job_id}' actualizado con {updates}")
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
    


    async def _publicar_tarea(self, msg: dict):
        await self.producer.publish(TOPIC_TASK, msg)

    async def _on_kafka_message(self, message):
        try:
            data = json.loads(message.value.decode("utf-8"))
            print(data)
            job_id = data.get("job_id")
            task_id = data.get("task_id")
            print(f"[Kafka **************************************************] Mensaje recibido en offset {message.offset}: {data}")
            if job_id and task_id:
                if self.on_task_complete_callback:
                    await self.on_task_complete_callback(job_id, task_id)
                print(f"Terminado callback: job_id={job_id}, tipo={type(job_id)}")
                await self.task_completed(job_id, task_id)
                print("Terminado el marcado de tarea como completada")
                job = self.collection.get(job_id)
                if job and all(t["completed"] for t in job["tasks"].values()):
                    if self.on_complete_callback:
                        await self.on_complete_callback(job_id)

        except Exception as e:
            print(f"[!] Error procesando mensaje offset {message.offset}: {e} en on_kafka")
            # ⚠️ Importante: aunque falle, hacer commit del offset
            #await self.consumer.consumer.commit()

    async def task_completed(self, job_id: str, task_id: str):
        print(f"Marcando tarea '{task_id}' del job '{job_id}' como completada.")
        print([col["name"] for col in self.db.collections()])
        job = self.collection.get(job_id)
        print(f"Marcada tarea '{task_id}' del job '{job_id}' como completada...  Ahora: {job["tasks"][task_id]["completed"]}")
        if not job:
            print(f"[!] Job '{job_id}' no encontrado")
            return {"error": "Job no encontrado"}

        if task_id not in job["tasks"]:
            print(f"[!] Tarea '{task_id}' no pertenece al job '{job_id}'")
            return {"error": "Tarea no encontrada en el job"}
        
        if job["tasks"][task_id]["completed"]:
            print(f"[!] Tarea '{task_id}' del job '{job_id}' ya estaba marcada como completada")
            return {"status": "completed", "message": f"Tarea '{task_id}' del job '{job_id}' ya estaba completada"}    

        job["tasks"][task_id]["completed"] = True
        print(f"[✔] Tarea '{task_id}' actualizandose en BBDD en job '{job_id}'")
        self.collection.update(job)
        print(f"[✔] Tarea '{task_id}' completada en job '{job_id}'")

        # ✅ Publicar evento de finalización de tarea
        await self.producer.publish(TOPIC_END_TASK, {
            "job_id": job_id,
            "task_id": task_id,
            "status": "completed",
            "uuid": str(uuid.uuid4())
        })

        # ✅ Callback
        if self.on_task_complete_callback:
            await self.on_task_complete_callback(job_id, task_id)

        # Si todas las tareas del job están completas
        for t in job["tasks"].values():  print(f"{t}\n")

        if all(t["completed"] for t in job["tasks"].values()):
            print(f"[🎉] Job '{job_id}' completado")
            if self.on_complete_callback:
                await self.on_complete_callback(job_id)

            await self.producer.publish(self.topic_finalizacion_global, {
                "job_id": job_id,
                "status": "completed",
                "uuid": str(uuid.uuid4())
            })

            if self._all_jobs_completed():
                print("[🎉] Todos los jobs completados")
                if self.on_all_complete_callback:
                    await self.on_all_complete_callback()

            return {"status": "ok", "message": f"Tarea '{task_id}' completada en job '{job_id}'. Todas las tareas del job están completas."}
        
        return {"status": "ok", "message": f"Tarea '{task_id}' completada en job '{job_id}'"}

    def _all_jobs_completed(self) -> bool:
        cursor = self.collection.find({})
        for job in cursor:
            if not all(t["completed"] for t in job["tasks"].values()):
                return False
        return True

########################################
# Para pruebas y demostración

# Callbacks de pruebas

async def on_task_complete(job_id, task_id):
    print(f"🔹🔹🔹🔹🔹🔹🔹🔹 Callback: Tarea {task_id} del job {job_id} completada.")

async def on_job_complete(job_id):
    print(f"✅✅✅✅✅✅✅✅ Callback: Job {job_id} completado.")

async def on_all_jobs_complete():
    print("🌍🌍🌍🌍🌍🌍🌍🌍 Callback: Todos los jobs completados.")

# Ejemplo de uso adaptado
async def main():
    monitor = GestorTareas(
        on_complete_callback=on_job_complete,
        on_all_complete_callback=on_all_jobs_complete,
        on_task_complete_callback=on_task_complete  
    )

    await monitor.start()

    # Crear el job y obtener el job_id generado usando tareas con campos adicionales
    tasks = [
        {"task_id": "task_id_1", "card_id": "card_id_1", "xxx_id": "xxx_id_1"},
        {"task_id": "task_id_2", "card_id": "card_id_2"}
    ]
    job_id = await monitor.add_job(tasks)

    # Simular finalización de tareas usando los task_id de la lista de tareas
    for task in tasks:
        await monitor.task_completed(job_id, task["task_id"])

    # Mantener corriendo para escuchar mensajes reales
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        await monitor.stop()

async def main2():
    monitor = GestorTareas()
    await monitor.start()
    devolucion = monitor.get_task_field("c37565d2-f436-46fd-a7a7-508e1b72f379","tarea_prueba1", "card_id")
    print(devolucion)
    await monitor.stop()



if __name__ == "__main__":
    
    asyncio.run(main())


    