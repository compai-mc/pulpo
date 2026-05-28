from typing import Any, Optional

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env

CLIENT_ID = require_env("CLIENT_ID_CORREO")
CLIENT_SECRET = require_env("CLIENT_SECRET_CORREO")


class CorreoClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.json_headers = {
            "Content-Type": "application/json"
        }
        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.client = MicroHttpClient(self.tm)

    def _post(self, path: str, headers: Optional[dict[str, str]] = None, **kwargs):
        return self.client.post(
            f"{self.base_url}{path}",
            headers=self.json_headers if headers is None else headers,
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

    def send_email(
        self,
        to: str,
        subject: str,
        body: str,
        from_email: Optional[str] = None,
        type: str = "html",
        cc: Optional[list[str]] = None,
        bcc: Optional[list[str]] = None,
        reply_to: Optional[str] = None,
        importance: str = "normal",
        attachments: Optional[list[dict[str, Any]]] = None,
    ) -> dict:
        payload = {
            "to": to,
            "subject": subject or "Correo sin asunto",
            "body": body,
            "from_email": from_email,
            "type": type,
            "cc": cc,
            "bcc": bcc,
            "reply_to": reply_to,
            "importance": importance,
            "attachments": attachments,
        }
        return self._post("/send-email", json=payload)

    def enviar_correo_completo(
        self,
        to: str,
        subject: str,
        body: str,
        from_email: Optional[str] = None,
        type: str = "html",
        cc: Optional[list[str]] = None,
        bcc: Optional[list[str]] = None,
        reply_to: Optional[str] = None,
        importance: str = "normal",
        attachments: Optional[list[dict[str, Any]]] = None,
    ) -> dict:
        return self.send_email(
            to=to,
            subject=subject,
            body=body,
            from_email=from_email,
            type=type,
            cc=cc,
            bcc=bcc,
            reply_to=reply_to,
            importance=importance,
            attachments=attachments,
        )

    def enviar_correo_con_archivos(
        self,
        to: str,
        subject: str,
        body: str,
        files: Optional[list[tuple[str, tuple[str, bytes, str]]]] = None,
        from_email: Optional[str] = None,
        type: str = "html",
        cc: Optional[str | list[str]] = None,
        bcc: Optional[str | list[str]] = None,
        reply_to: Optional[str] = None,
        importance: str = "normal",
    ) -> dict:
        data = {
            "to": to,
            "subject": subject or "Correo sin asunto",
            "body": body,
            "type": type,
            "importance": importance,
        }

        if from_email:
            data["from_email"] = from_email
        if cc:
            data["cc"] = ",".join(cc) if isinstance(cc, list) else cc
        if bcc:
            data["bcc"] = ",".join(bcc) if isinstance(bcc, list) else bcc
        if reply_to:
            data["reply_to"] = reply_to

        return self._post(
            "/enviar-correo-con-archivos",
            headers={},
            data=data,
            files=files or [],
        )
