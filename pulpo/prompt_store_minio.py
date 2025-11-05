from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Optional

from minio import Minio
from minio.error import S3Error

from .logueador import log

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
        agent_name: str,
        prompt: str,
        persona_name: Optional[str] = None,
        persona_id: Optional[str] = None,
        original_message: Optional[str] = None,
        prompt_template: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Guarda un prompt en MinIO y devuelve el nombre del objeto."""
        if not self._client:
            return None

        timestamp = datetime.now(timezone.utc)
        payload: Dict[str, Any] = {
            "timestamp": timestamp.isoformat(),
            "agent_name": agent_name,
            "persona_name": persona_name,
            "persona_id": persona_id,
            "prompt": prompt,
            "original_message": original_message,
            "prompt_template": prompt_template,
            "model": model,
            "temperature": temperature,
        }
        if metadata:
            payload["metadata"] = metadata

        metadata = metadata.copy() if metadata else {}

        job_id = metadata.get("job_id")
        environment = (
            metadata.get("environment")
            or os.getenv("PROMPTS_ENV")
            or os.getenv("ENVIRONMENT")
            or "dev"
        )
        namespace = metadata.get("namespace") or os.getenv("PROMPTS_NAMESPACE") or "compai"

        job_segment = "no-job"
        if job_id:
            job_segment = str(job_id).replace("/", "_").strip() or job_segment

        agent_segment = agent_name.replace(" ", "_").lower() or "agent"
        object_name = f"{namespace}/{environment}/{job_segment}-{agent_segment}-{timestamp:%Y%m%d%H%M%S%f}.json"

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
