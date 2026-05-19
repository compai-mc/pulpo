from typing import Optional, Dict, Any

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

URL_CONFIG_SERVICE = require_env("URL_CONFIG_SERVICE")
CLIENT_ID = require_env("CLIENT_ID_CONFIG_SERVICE")
CLIENT_SECRET = require_env("CLIENT_SECRET_CONFIG_SERVICE")


class ConfigClient:
    def __init__(self, base_url: str = URL_CONFIG_SERVICE):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }
        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.client = MicroHttpClient(self.tm)

    def _get(self, path: str, **kwargs):
        return self.client.get(
            f"{self.base_url}{path}",
            headers=self.headers,
            timeout=10.0,
            **kwargs
        )

    def health(self) -> Dict[str, Any]:
        """Verifica el estado del microservicio."""
        return self._get("/health")

    def get_all_config(self) -> Dict[str, Any]:
        """Obtiene toda la configuracion almacenada."""
        return self._get("/config")

    def get_config(self, service: str) -> Optional[Dict[str, Any]]:
        """Obtiene la configuracion especifica de un servicio."""
        try:
            return self._get(f"/config/{service}")
        except Exception as e:
            if getattr(getattr(e, "response", None), "status_code", None) == 404:
                print(f"[WARN] No se encontro configuracion para el servicio '{service}'")
                return None
            raise

    def close(self):
        pass


if __name__ == "__main__":
    config_api = ConfigClient("http://alcazar:7416")

    print("Estado del microservicio:")
    print(config_api.health())

    print("\nConfiguracion completa:")
    print(config_api.get_all_config())

    print("\nConfiguracion de 'compai.roundtable':")
    print(config_api.get_config("compai.roundtable"))

    config_api.close()
