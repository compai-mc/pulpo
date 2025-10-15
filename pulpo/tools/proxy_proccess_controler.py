import os
import requests
from typing import Dict, Optional
from dotenv import load_dotenv
load_dotenv()

PROCCESSCONTROLER_URL = os.getenv("PROCCESSCONTROLER_URL", "http://alcazar:7417")

# === Cliente proxy para microservicio de similitud de datos en milvus (7417)===
class ProccessControlerProxy:
    def __init__(self, base_url: str = PROCCESSCONTROLER_URL, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {"Content-Type": "application/json"}
        if api_key:
            self.headers["X-API-Key"] = api_key

    def similarity_product(self, query: str, codigo: str, numero_resultados: int = 15, min_score: int = 0) -> Dict:
        response = requests.get(
            f"{self.base_url}/product/similarity",
            params={
                "codigo": codigo,
                "query": query,
                "numero_resultados": numero_resultados,
                "min_score": min_score
            },
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()

    def similarity_client(self, query: str, numero_resultados: int = 15, min_score: int = 0) -> Dict:
        response = requests.get(
            f"{self.base_url}/client/similarity",
            params={
                "query": query,
                "numero_resultados": numero_resultados,
                "min_score": min_score
            },
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()