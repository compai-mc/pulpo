from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from minio import Minio
from minio.error import S3Error

from pulpo.logueador import log

__all__ = [
    "MinioPromptStore",
    "store_llm_prompt",
    "list_llm_prompts",
    "get_llm_prompt",
    "download_llm_prompt",
    "download_llm_prompts",
]

class MinioPromptStore:
    """Almacena y recupera prompts LLM desde MinIO."""

    def __init__(self, *, bucket: Optional[str] = None) -> None:
        self._bucket = bucket or os.getenv("MINIO_PROMPTS_BUCKET", "llm-prompts")
        self._client = self._build_client()
        if self._client:
            self._ensure_bucket()

    def _build_client(self) -> Optional[Minio]:
        url = os.getenv("PROMPTS_MINIO_URL") or os.getenv("MINIO_URL")
        access_key = os.getenv("PROMPTS_MINIO_ACCESS_KEY") or os.getenv("ACCESS_KEY")
        secret_key = os.getenv("PROMPTS_MINIO_SECRET_KEY") or os.getenv("SECRET_KEY")

        if not url or not access_key or not secret_key:
            log.debug(
                "Prompt storage disabled: missing PROMPTS_MINIO_URL/ACCESS_KEY/SECRET_KEY env vars."
            )
            return None

        secure = os.getenv("PROMPTS_MINIO_SECURE", "false").strip().lower() in {
            "1",
            "true",
            "yes",
            "y",
            "on",
        }

        if "://" in url:
            url = url.split("://", 1)[1]

        try:
            return Minio(url, access_key=access_key, secret_key=secret_key, secure=secure)
        except Exception as exc:  # noqa: BLE001
            log.error(f"Prompt storage disabled: unable to initialise MinIO client ({exc}).")
            return None

    def _ensure_bucket(self) -> None:
        if not self._client:
            return
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
                log.debug("Created MinIO bucket for prompts: %s", self._bucket)
        except S3Error as exc:
            log.error("Unable to ensure MinIO bucket '%s': %s", self._bucket, exc)
            raise

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def store(
        self,
        *,
        prompt: str,
        prompt_template: Optional[str] = None,
        model: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Guarda un prompt en MinIO y devuelve el nombre del objeto."""
        if not self._client:
            return None

        timestamp = datetime.now(timezone.utc)
        payload: Dict[str, Any] = {
            "timestamp": timestamp.isoformat(),
            "prompt": prompt,
            "prompt_template": prompt_template,
            "model": model,
        }
        if metadata:
            payload["metadata"] = metadata

        metadata = metadata.copy() if metadata else {}

        environment = metadata.get("environment") or "dev"
        namespace = metadata.get("namespace") or "compai"
        pv_name = metadata.get("pv_name") or "default"
        prompt_name = metadata.get("prompt_name") or "prompt"

        object_name = f"{namespace}/{environment}/{pv_name}/{prompt_name}.json"

        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        data_stream = io.BytesIO(data)

        try:
            self._ensure_bucket()
            self._client.put_object(
                self._bucket,
                object_name,
                data_stream,
                len(data),
                content_type="application/json",
            )
            log.debug(
                "Stored prompt in MinIO: bucket=%s key=%s size=%sB",
                self._bucket,
                object_name,
                len(data),
            )
            return object_name
        except S3Error as exc:
            log.error("Failed to upload prompt to MinIO: %s", exc)
        except Exception as exc:  # noqa: BLE001
            log.error("Unexpected error storing prompt in MinIO: %s", exc)
        return None

    def list(self, *, prefix: Optional[str] = None, recursive: bool = True) -> Iterable[str]:
        """Lista los objetos disponibles bajo un prefijo dado."""
        if not self._client:
            return []
        objects = self._client.list_objects(
            self._bucket, prefix=prefix or "", recursive=recursive
        )
        return (obj.object_name for obj in objects)

    def get(self, object_name: str) -> Optional[Dict[str, Any]]:
        """Recupera el contenido JSON de un objeto almacenado."""
        if not self._client:
            return None
        try:
            response = self._client.get_object(self._bucket, object_name)
            try:
                return json.loads(response.read().decode("utf-8"))
            finally:
                response.close()
                response.release_conn()
        except S3Error as exc:
            log.error("Failed to retrieve prompt '%s': %s", object_name, exc)
        except Exception as exc:  # noqa: BLE001
            log.error("Unexpected error retrieving prompt '%s': %s", object_name, exc)
        return None

    def download_many(self, prefix: str) -> Dict[str, Dict[str, Any]]:
        """Descarga todos los prompts bajo un prefijo y los devuelve como dict {object_name: payload}."""
        results: Dict[str, Dict[str, Any]] = {}
        for name in self.list(prefix=prefix, recursive=True):
            data = self.get(name)
            if data is not None:
                results[name] = data
        return results
    
        
    def delete(self, object_name: str) -> bool:
        """Elimina un prompt concreto de MinIO."""
        if not self._client:
            return False
        try:
            self._client.remove_object(self._bucket, object_name)
            log.debug("🗑️ Eliminado prompt de MinIO: %s", object_name)
            return True
        except S3Error as exc:
            if exc.code == "NoSuchKey":
                log.warning("⚠️ El objeto '%s' no existe en el bucket '%s'.", object_name, self._bucket)
                return False
            log.error("❌ Error S3 al eliminar '%s': %s", object_name, exc)
        except Exception as exc:  # noqa: BLE001
            log.error("❌ Error inesperado al eliminar '%s': %s", object_name, exc)
        return False

    def delete_many(self, prefix: Optional[str] = None) -> int:
        """
        Elimina todos los prompts bajo un prefijo dado (p.ej. 'compai/dev/operaciones/').
        Devuelve el número de objetos eliminados.
        """
        if not self._client:
            return 0
        count = 0
        try:
            objects = list(self._client.list_objects(self._bucket, prefix=prefix or "", recursive=True))
            for obj in objects:
                self._client.remove_object(self._bucket, obj.object_name)
                count += 1
            log.debug("🧹 Eliminados %d objetos bajo prefijo '%s'", count, prefix or "")
        except Exception as exc:
            log.error("❌ Error eliminando objetos bajo '%s': %s", prefix, exc)
        return count



# ---------------------------------------------------------------------- #
# Helper functions
# ---------------------------------------------------------------------- #

def _get_store() -> MinioPromptStore:
    return MinioPromptStore()


def store_llm_prompt(**kwargs: Any) -> Optional[str]:
    """Guarda un prompt LLM usando la configuración global."""
    return _get_store().store(**kwargs)


def list_llm_prompts(prefix: Optional[str] = None, recursive: bool = True) -> Iterable[str]:
    """Devuelve un iterador con los nombres de objetos almacenados."""
    return _get_store().list(prefix=prefix, recursive=recursive)


def get_llm_prompt(object_name: str) -> Optional[Dict[str, Any]]:
    """Obtiene un prompt almacenado por su nombre de objeto."""
    return _get_store().get(object_name)


def download_llm_prompt(object_name: str) -> Optional[Dict[str, Any]]:
    """Alias de `get_llm_prompt` para mantener compatibilidad semántica."""
    return get_llm_prompt(object_name)


def download_llm_prompts(prefix: str) -> Dict[str, Dict[str, Any]]:
    """Recupera todos los prompts bajo un prefijo dado (p.ej. 'compai/dev')."""
    return _get_store().download_many(prefix)

def delete_llm_prompt(object_name: str) -> bool:
    """Elimina un prompt específico del bucket."""
    return _get_store().delete(object_name)


def delete_all_llm_prompts(prefix: Optional[str] = None) -> int:
    """Elimina todos los prompts bajo un prefijo (p.ej. 'compai/dev/')."""
    return _get_store().delete_many(prefix)




# ---------------------------------------------------------------------- #
# Bloque de pruebas
# ---------------------------------------------------------------------- #

if __name__ == "__main__":
    print("\n=== 🧪 PRUEBA DE MinioPromptStore ===\n")

    # Configuración mínima (puedes cambiarla)
    os.environ["PROMPTS_MINIO_URL"] = "alcazar:19000"
    os.environ["PROMPTS_MINIO_ACCESS_KEY"] = "minioadmin"
    os.environ["PROMPTS_MINIO_SECRET_KEY"] = "minioadmin"
    os.environ["PROMPTS_MINIO_SECURE"] = "false"
    os.environ["MINIO_PROMPTS_BUCKET"] = "llm-prompts"

    try:
        store = MinioPromptStore()

        print("➡️ Guardando un prompt de prueba...")
        object_name = store.store(
            prompt="¿Quién fue Pericles?",
            model="gpt-5",
            metadata={
                "environment": "dev",
                "namespace": "compai",
                "pv_name": "operaciones",
                "prompt_name": "clasificador"
            },
        )
        if object_name:
            print(f"✅ Prompt guardado con nombre: {object_name}")
        else:
            print("❌ Error al guardar el prompt.")
            exit(1)

        print("\n📋 Listando objetos:")
        for name in store.list(prefix="compai/dev"):
            print(" -", name)

        print("\n📥 Descargando el prompt guardado:")
        data = store.get("compai/desarrollo/Chatbot/nombreprompt.json")
        if data:
            print(json.dumps(data, indent=4, ensure_ascii=False))
        else:
            print("⚠️ No se pudo recuperar el objeto.")

    except Exception as e:
        print("⚠️ Error en la prueba:", e)

        