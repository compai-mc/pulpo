from typing import Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

CLIENT_ID = require_env("CLIENT_ID_CORREO")
CLIENT_SECRET = require_env("CLIENT_SECRET_CORREO")


class CorreoClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }
        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.client = MicroHttpClient(self.tm)

    def _post(self, path: str, **kwargs):
        return self.client.post(
            f"{self.base_url}{path}",
            headers=self.headers,
            **kwargs
        )

    def enviar_correo(
        self,
        destinatario: str,
        asunto: str,
        mensaje: str,
        remitente: Optional[str] = None,
        adjuntos: Optional[list[tuple[str, str, str]]] = None
    ) -> dict:
        payload = {
            "destinatario": destinatario,
            "asunto": asunto or "Correo sin asunto",
            "mensaje": mensaje,
            "remitente": remitente or ""
        }

        if adjuntos:
            payload["adjuntos"] = [
                {"nombre": nombre, "contenido": contenido, "tipo": tipo}
                for nombre, contenido, tipo in adjuntos
            ]

        return self._post("/email/send", json=payload)
