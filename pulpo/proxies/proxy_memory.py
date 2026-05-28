from typing import Dict, Any, Optional

import httpx
import os

from pulpo.util.util import require_env


URL_MEMORY = require_env("URL_MEMORY")
MEMORY_TIMEOUT = float(os.getenv("MEMORY_TIMEOUT", "30"))


class MemoryAPIClient:
    def __init__(
        self,
        base_url: str = URL_MEMORY,
        memory_id: Optional[str] = None,
        overwrite: bool = False
    ):
        self.base_url = base_url.rstrip("/")
        self.memory_id = memory_id
        self.headers = {
            "Content-Type": "application/json"
        }
        self.client = httpx.Client(timeout=MEMORY_TIMEOUT)

        data = self._post(
            "/create",
            json={
                "memory_id": memory_id,
                "overwrite": overwrite
            }
        )
        self.memory_id = data["memory_id"]

    def _get(self, path: str, **kwargs):
        response = self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )
        response.raise_for_status()
        return response.json()

    def _post(self, path: str, **kwargs):
        response = self.client.post(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )
        response.raise_for_status()
        return response.json()

    def _delete(self, path: str, **kwargs):
        response = self.client.delete(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )
        response.raise_for_status()
        return response.json()

    def add(
        self,
        content: str,
        metadata: Optional[Dict] = None,
        memory_id: Optional[str] = None
    ) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError(
                "Se necesita un memory_id (llama primero a create_memory o pasalo explicitamente)"
            )
        return self._post(
            "/add",
            json={
                "memory_id": mid,
                "content": content,
                "metadata": metadata
            }
        )

    def similarity_search(
        self,
        query: str,
        k: int = 5,
        min_score: float = 0.7,
        memory_id: Optional[str] = None
    ) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id")
        return self._post(
            "/search",
            json={
                "memory_id": mid,
                "query": query,
                "k": k,
                "min_score": min_score
            }
        )

    def list_items(
        self,
        memory_id: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id para listar items")
        return self._get(
            f"/{mid}/list",
            params={
                "limit": limit
            }
        )

    def delete_memory(self, memory_id: Optional[str] = None) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id para borrar memoria")
        return self._delete(f"/{mid}")

    def list_memories(self) -> Dict[str, Any]:
        return self._get("")

    def close(self):
        self.client.close()
