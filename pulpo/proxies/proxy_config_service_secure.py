from typing import Any, Dict, Optional

from pulpo.util.util import require_env


class SecureConfigClient:
    """
    Cliente securizado para config-service.

    Importante: este cliente no se usa en load_env(). El cliente de bootstrap
    sigue siendo proxy_config_service.ConfigClient para evitar ciclos de arranque.
    """

    def __init__(self, base_url: Optional[str] = None):
        from pulpo.auth.general import MicroHttpClient, MicroTokenManager

        self.base_url = (base_url or require_env("URL_CONFIG_SERVICE")).rstrip("/")
        self.tm = MicroTokenManager(
            client_id=require_env("CLIENT_ID_CONFIG_SERVICE"),
            client_secret=require_env("CLIENT_SECRET_CONFIG_SERVICE"),
        )
        self.client = MicroHttpClient(self.tm)

    def _get(self, path: str, **kwargs):
        return self.client.get(f"{self.base_url}{path}", **kwargs)

    def _post(self, path: str, payload: Optional[Dict[str, Any]] = None, **kwargs):
        return self.client.post(f"{self.base_url}{path}", json=payload or {}, **kwargs)

    def _put(self, path: str, payload: Optional[Dict[str, Any]] = None, **kwargs):
        return self.client.put(f"{self.base_url}{path}", json=payload or {}, **kwargs)

    def _delete(self, path: str, **kwargs):
        return self.client.delete(f"{self.base_url}{path}", **kwargs)

    def health(self) -> Dict[str, Any]:
        return self._get("/health")

    def get_all_config(self) -> Dict[str, Any]:
        return self._get("/config")

    def get_config(self, service: str) -> Optional[Dict[str, Any]]:
        return self._get(f"/config/{service}")

    def create_config(self, service: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._post(f"/config/{service}", payload)

    def update_config(self, service: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self._put(f"/config/{service}", payload)

    def delete_config(self, service: str):
        return self._delete(f"/config/{service}")
