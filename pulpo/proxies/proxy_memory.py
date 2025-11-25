import requests
from typing import Dict, Any, Optional
from pulpo.util.util import require_env

URL_MEMORY = require_env("URL_MEMORY")

class MemoryAPIClient:
    def __init__(
        self,
        base_url: str = URL_MEMORY,
        memory_id: Optional[str] = None,
        overwrite: bool = False
    ):
        self.base_url = base_url.rstrip("/")
        self.memory_id = memory_id  

        url = f"{self.base_url}/create"
        resp = requests.post(url, json={"memory_id": memory_id, "overwrite": overwrite})
        resp.raise_for_status()
        data = resp.json()
        self.memory_id = data["memory_id"]

    def add(self, content: str, metadata: Optional[Dict] = None, memory_id: Optional[str] = None) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id (llama primero a create_memory o pásalo explícitamente)")
        url = f"{self.base_url}/add"
        resp = requests.post(url, json={"memory_id": mid, "content": content, "metadata": metadata})
        resp.raise_for_status()
        return resp.json()

    def similarity_search(self, query: str, k: int = 5, min_score: float = 0.7, memory_id: Optional[str] = None) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id")
        url = f"{self.base_url}/search"
        resp = requests.post(url, json={"memory_id": mid, "query": query, "k": k, "min_score": min_score})
        resp.raise_for_status()
        return resp.json()

    def list_items(self, memory_id: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id para listar ítems")
        url = f"{self.base_url}/{mid}/list"
        resp = requests.get(url, params={"limit": limit})
        resp.raise_for_status()
        return resp.json()

    def delete_memory(self, memory_id: Optional[str] = None) -> Dict[str, Any]:
        mid = memory_id or self.memory_id
        if not mid:
            raise ValueError("Se necesita un memory_id para borrar memoria")
        url = f"{self.base_url}/{mid}"
        resp = requests.delete(url)
        resp.raise_for_status()
        return resp.json()

    def list_memories(self) -> Dict[str, Any]:
        resp = requests.get(self.base_url)
        resp.raise_for_status()
        return resp.json()