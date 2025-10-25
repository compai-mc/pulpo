import json
import httpx
from typing import List, Dict, Optional

class CorreoClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")

    async def enviar_correo(
        self,
        destinatario: str,
        asunto: str,
        mensaje: str,
        remitente: str = None,
        adjuntos: list[tuple[str, str, str]] = None
    ) -> dict:
        url = f"{self.base_url}/email/send"

        params = {
            "destinatario": destinatario,
            "asunto": asunto,
            "mensaje": mensaje
        }

        if remitente:
            params["remitente"] = remitente

        if adjuntos:
            # Convierte la lista de tuplas a JSON string como espera el endpoint
            params["adjuntos"] = json.dumps([
                {"nombre": nombre, "contenido": contenido, "tipo": tipo}
                for nombre, contenido, tipo in adjuntos
            ])

        async with httpx.AsyncClient() as client:
            response = await client.post(url, params=params)
            response.raise_for_status()
            return response.json()