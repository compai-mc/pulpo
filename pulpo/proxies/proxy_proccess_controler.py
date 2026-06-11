import os
from typing import Dict, Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

PROCCESSCONTROLER_URL = require_env("PROCCESSCONTROLER_URL")
CLIENT_ID = require_env("CLIENT_ID_PROCESSCONTROLLER")
CLIENT_SECRET = require_env("CLIENT_SECRET_PROCESSCONTROLLER")


def _float_env(var_name: str, default: float) -> float:
    value = os.getenv(var_name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


PROCCESSCONTROLER_TIMEOUT = _float_env("PROCCESSCONTROLER_TIMEOUT", 600.0)


# Cliente proxy para microservicio de similitud de datos en milvus (7417)
class ProccessControlerProxy:
    def __init__(
        self,
        base_url: str = PROCCESSCONTROLER_URL,
        api_key: Optional[str] = None,
        timeout: float = PROCCESSCONTROLER_TIMEOUT
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
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
        kwargs.setdefault("timeout", self.timeout)
        return self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def _post(self, path: str, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
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

    def resolver_producto(
        self,
        query: str = "",
        codigo: str = "",
        client_ref: str = "",
        **extra_params
    ) -> Dict:
        """
        Llama al resolver LLM de productos.
        """
        return self._get(
            "/product/resolverLLM",
            params=self._params(
                query=query,
                codigo=codigo,
                client_ref=client_ref,
                **extra_params
            )
        )

    def obtener_auditoria_resolver(self, auditoria_id: str) -> Dict:
        """
        Recupera la auditoria completa de una decision del resolver.
        """
        return self._get(f"/product/resolverLLM/auditoria/{auditoria_id}")

    def registrar_precedente_producto(
        self,
        client_ref: str,
        query_original: str,
        fk_product_humano: int,
        fk_product_ia: Optional[int] = None,
        modo: Optional[str] = None,
        confianza: Optional[str] = None,
        auditoria_id: Optional[str] = None,
        **extra_payload
    ) -> Dict:
        """
        Registra un precedente tras la validacion humana de un producto.

        fk_product_ia se envia siempre que exista, aunque coincida con el humano.
        modo, confianza y auditoria_id cierran el bucle de medicion del resolver.
        """
        payload = {
            "client_ref": client_ref,
            "query_original": query_original,
            "fk_product_humano": fk_product_humano,
        }
        if fk_product_ia is not None:
            payload["fk_product_ia"] = fk_product_ia
        for campo, valor in (
            ("modo", modo),
            ("confianza", confianza),
            ("auditoria_id", auditoria_id)
        ):
            if valor is not None:
                payload[campo] = valor
        payload.update(extra_payload)
        return self._post("/product/precedente", json=payload)

    def crear_instruccion_producto(
        self,
        scope_ref: str,
        texto: str,
        **extra_payload
    ) -> Dict:
        """
        Crea una instruccion manual asociada a un grupo de empresas.
        """
        payload = {
            "scope_ref": scope_ref,
            "texto": texto,
        }
        payload.update(extra_payload)
        return self._post("/context/instruccion", json=payload)

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
