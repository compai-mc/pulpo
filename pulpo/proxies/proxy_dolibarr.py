from typing import Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

BASE_URL = require_env("ERPDOLIBARR_URL")
CLIENT_ID = require_env("CLIENT_ID_ERPDOLIBARR")
CLIENT_SECRET = require_env("CLIENT_SECRET_ERPDOLIBARR")


class DolibarrProxy:
    def __init__(self, base_url: str = BASE_URL, api_key: Optional[str] = None):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }

        if api_key:
            self.headers["DOLAPIKEY"] = api_key

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

    def pedidos(self, fecha):
        return self._get(f"/orders/{fecha}/url")

    def pedidos_producto_cliente_mes(self, fecha):
        return self._get(f"/orders/group-by-product-client-month/{fecha}/url")

    def pedidos_sync(self):
        return self._post("/orders/sync", timeout=6000)

    def productos(self):
        return self._get("/products", timeout=6000)

    def clientes(self):
        return self._get("/clients", timeout=6000)


_default_proxy: DolibarrProxy | None = None


def _proxy() -> DolibarrProxy:
    global _default_proxy
    if _default_proxy is None:
        _default_proxy = DolibarrProxy()
    return _default_proxy


def get_pedidos_url(fecha):
    try:
        print(_proxy().pedidos(fecha))
    except Exception as e:
        print(f"Error al obtener la URL de los pedidos para la fecha {fecha}:", e)


def get_pedidos_producto_cliente_mes_url(fecha):
    try:
        print(_proxy().pedidos_producto_cliente_mes(fecha))
    except Exception as e:
        print(f"Error al obtener la URL de los pedidos por producto y cliente para la fecha {fecha}:", e)


def pedidos_sync():
    try:
        _proxy().pedidos_sync()
        print("Datos de pedidos sincronizados correctamente")
    except Exception as e:
        print("Error en la sincronizacion de pedidos:", e)


def get_productos():
    try:
        return _proxy().productos()
    except Exception as e:
        print("Error al obtener los productos", e)


def get_clientes():
    try:
        return _proxy().clientes()
    except Exception as e:
        print("Error al obtener los clientes", e)


if __name__ == "__main__":
    data = get_productos()
    from pprint import pprint

    filtrados = [
        {k: item[k] for k in ["ref", "label", "multilangs"] if k in item}
        for item in data
    ]

    pprint(filtrados[76])
