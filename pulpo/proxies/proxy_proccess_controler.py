from typing import Dict, Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

PROCCESSCONTROLER_URL = require_env("PROCCESSCONTROLER_URL")
CLIENT_ID = require_env("CLIENT_ID_PROCESSCONTROLLER")
CLIENT_SECRET = require_env("CLIENT_SECRET_PROCESSCONTROLLER")


# Cliente proxy para microservicio de similitud de datos en milvus (7417)
class ProccessControlerProxy:
    def __init__(self, base_url: str = PROCCESSCONTROLER_URL, api_key: Optional[str] = None):
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

    @staticmethod
    def _params(**params):
        clean_params = {}
        for key, value in params.items():
            if value is None:
                continue
            if isinstance(value, str):
                value = value.strip()
                if key.startswith("min_score") and value.endswith("'"):
                    value = value.rstrip("'")
            clean_params[key] = value
        return clean_params

    def similarity_product(
        self,
        query: str,
        codigo: str = "",
        numero_resultados: int = 15,
        min_score: int = 0,
        **extra_params
    ) -> Dict:
        return self._get(
            "/product/similarity",
            params=self._params(
                codigo=codigo,
                query=query,
                numero_resultados=numero_resultados,
                min_score=min_score,
                **extra_params
            )
        )

    def similarity_client(
        self,
        query: str,
        numero_resultados: int = 15,
        min_score: int = 0,
        **extra_params
    ) -> Dict:
        return self._get(
            "/client/similarity",
            params=self._params(
                query=query,
                numero_resultados=numero_resultados,
                min_score=min_score,
                **extra_params
            )
        )

    def get_product_price(
        self,
        product_id: Optional[str] = None,
        client_id: Optional[str] = None,
        referencia_producto: Optional[str] = None,
        referencia_cliente: Optional[str] = None,
        **extra_params
    ) -> Dict:
        return self._get(
            "/product/price",
            params=self._params(
                referencia_producto=referencia_producto or product_id,
                referencia_cliente=referencia_cliente or client_id,
                **extra_params
            )
        )

    def validar_precios(self, payload: Optional[Dict] = None, **extra_payload) -> Dict:
        data = payload.copy() if payload else {}
        data.update(extra_payload)
        return self._post(
            "/price/validate",
            json=data
        )

    def buscar_contacto_por_email(
        self,
        email: str,
        thirdparty_id: Optional[str] = None,
        numero_resultados: int = 1,
        **extra_params
    ) -> Dict:
        return self._get(
            "/contact/search/email",
            params=self._params(
                thirdparty_id=thirdparty_id,
                email=email,
                numero_resultados=numero_resultados,
                **extra_params
            )
        )

    def buscar_contacto(
        self,
        query: Optional[str] = None,
        thirdparty_id: Optional[str] = None,
        numero_resultados: int = 5,
        min_score: int = 0,
        **extra_params
    ) -> Dict:
        return self._get(
            "/contact/search",
            params=self._params(
                query=query,
                thirdparty_id=thirdparty_id,
                numero_resultados=numero_resultados,
                min_score=min_score,
                **extra_params
            )
        )
