import json
import httpx
from typing import Optional

class CorreoClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    def enviar_correo(
        self,
        destinatario: str,
        asunto: str,
        mensaje: str,
        remitente: Optional[str] = None,
        adjuntos: Optional[list[tuple[str, str, str]]] = None
    ) -> dict:
        url = f"{self.base_url}/email/send"

        params = {
            "destinatario": destinatario,
            "asunto": asunto or "Correo sin asunto",
            "mensaje": mensaje
        }

        params["remitente"] = remitente or ""

        if adjuntos:
            params["adjuntos"] = [
                {"nombre": nombre, "contenido": contenido, "tipo": tipo}
                for nombre, contenido, tipo in adjuntos
            ]

        with httpx.Client() as client:
            response = client.post(url, json=params) 
            response.raise_for_status()
            return response.json()


    