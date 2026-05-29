from typing import Optional, Dict, Any

import httpx

from pulpo.util.util import require_env


URL_CONFIG_SERVICE = require_env("URL_CONFIG_SERVICE")


class ConfigClient:
    def __init__(self, base_url: str = URL_CONFIG_SERVICE):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=10.0)

    def health(self) -> Dict[str, Any]:
        """Verifica el estado del microservicio."""
        resp = self.client.get(f"{self.base_url}/health")
        resp.raise_for_status()
        return resp.json()

    def get_all_config(self) -> Dict[str, Any]:
        """Obtiene toda la configuracion almacenada."""
        resp = self.client.get(f"{self.base_url}/config")
        resp.raise_for_status()
        return resp.json()

    def get_config(self, service: str) -> Optional[Dict[str, Any]]:
        """Obtiene la configuracion especifica de un servicio."""
        resp = self.client.get(f"{self.base_url}/config/{service}")
        if resp.status_code == 404:
            print(f"[WARN] No se encontro configuracion para el servicio '{service}'")
            return None
        resp.raise_for_status()
        return resp.json()

    def close(self):
        self.client.close()


if __name__ == "__main__":
    config_api = ConfigClient("http://alcazar:7416")

    print("Estado del microservicio:")
    print(config_api.health())

    print("\nConfiguracion completa:")
    print(config_api.get_all_config())

    print("\nConfiguracion de 'compai.roundtable':")
    print(config_api.get_config("compai.roundtable"))

    config_api.close()
