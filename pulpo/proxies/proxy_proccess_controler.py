from typing import Dict, Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

PROCESSCONTROLLER_URL = require_env("PROCESSCONTROLLER_URL")
CLIENT_ID = require_env("CLIENT_ID_PROCESSCONTROLLER")
CLIENT_SECRET = require_env("CLIENT_SECRET_PROCESSCONTROLLER")


# Cliente proxy para microservicio de similitud de datos en milvus (7417)
class ProccessControlerProxy:
    def __init__(self, base_url: str = PROCESSCONTROLLER_URL, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }
        if api_key:
            self.headers["X-API-Key"] = api_key

        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.client = MicroHttpClient(self.tm)

    def _get(self, path: str, **kwargs):
        return self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _post(self, path: str, **kwargs):
        return self.client.post(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def similarity_product(self, query: str, codigo: str, numero_resultados: int = 15, min_score: int = 0) -> Dict:
        return self._get(
            "/product/similarity",
            params={
                "codigo": codigo,
                "query": query,
                "numero_resultados": numero_resultados,
                "min_score": min_score
            }
        )

    def similarity_client(self, query: str, numero_resultados: int = 15, min_score: int = 0) -> Dict:
        return self._get(
            "/client/similarity",
            params={
                "query": query,
                "numero_resultados": numero_resultados,
                "min_score": min_score
            }
        )

    def get_product_price(self, product_id: str, client_id: str) -> Dict:
        return self._get(
            "/product/price",
            params={
                "referencia_producto": product_id,
                "referencia_cliente": client_id
            }
        )

    def validar_precios(self, payload: Dict) -> Dict:
        return self._post(
            "/price/validate",
            json=payload
        )

    def buscar_contacto_por_email(
        self,
        thirdparty_id: str,
        email: str,
        numero_resultados: int = 1
    ) -> Dict:
        return self._get(
            "/contact/search/email",
            params={
                "thirdparty_id": thirdparty_id,
                "email": email,
                "numero_resultados": numero_resultados
            }
        )

    def buscar_contacto(
        self,
        query: str,
        thirdparty_id: Optional[str] = None,
        numero_resultados: int = 5,
        min_score: int = 0
    ) -> Dict:
        params = {
            "query": query,
            "numero_resultados": numero_resultados,
            "min_score": min_score
        }

        if thirdparty_id is not None:
            params["thirdparty_id"] = thirdparty_id

        return self._get(
            "/contact/search",
            params=params
        )
