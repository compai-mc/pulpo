from typing import Any, Mapping, Optional
from urllib.parse import quote
import os

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.util import require_env


CLIENT_ID = require_env("CLIENT_ID_FRONT")
CLIENT_SECRET = require_env("CLIENT_SECRET_FRONT")


def _require_first_env(*names: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value

    joined = ", ".join(names)
    raise RuntimeError(f"Variable de entorno obligatoria no definida. Define una de: {joined}")


FRONT_URL = _require_first_env(
    "COMPAI_FRONT_URL",
    "COMPAI_FRONTEND_URL",
    "FRONT_URL",
    "FRONTEND_URL",
    "URL_FRONT",
    "URL_FRONTEND",
)


class FrontProxy:
    """
    Proxy securizado para las APIs del front.

    Todas las llamadas salen con token Bearer generado mediante MicroTokenManager
    usando CLIENT_ID_FRONT y CLIENT_SECRET_FRONT.
    """

    def __init__(self, base_url: str = FRONT_URL):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json",
        }
        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )
        self.client = MicroHttpClient(self.tm)

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _get(self, path: str, params: Optional[Mapping[str, Any]] = None, **kwargs):
        return self.client.get(
            self._url(path),
            params=dict(params or {}),
            headers=self.headers,
            **kwargs,
        )

    def _post(self, path: str, payload: Optional[Mapping[str, Any]] = None, **kwargs):
        return self.client.post(
            self._url(path),
            json=dict(payload or {}),
            headers=self.headers,
            **kwargs,
        )

    def _response(self, method: str, path: str, **kwargs):
        return self.client._request(
            method,
            self._url(path),
            headers=self.headers,
            **kwargs,
        )

    @staticmethod
    def _path(value: str | int) -> str:
        return quote(str(value), safe="")

    # ======================================================
    # Tareas
    # ======================================================
    def tarea(self, tarea_id: int | str):
        return self._get(f"/tareas/api/{self._path(tarea_id)}/")

    def archivar_tarea(self, tarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/archivar/", payload)

    def card_html_tarea(self, tarea_id: int | str) -> str:
        response = self._response("GET", f"/tareas/api/{self._path(tarea_id)}/card-html/")
        return response.text

    def editar_tarea(self, tarea_id: int | str, payload: Mapping[str, Any]):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/editar/", payload)

    def eliminar_tarea(self, tarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/eliminar/", payload)

    def historial_tarea(self, tarea_id: int | str):
        return self._get(f"/tareas/api/{self._path(tarea_id)}/historial/")

    def mover_tarea(self, tarea_id: int | str, payload: Mapping[str, Any]):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/mover/", payload)

    def crear_tarea(self, payload: Mapping[str, Any]):
        return self._post("/tareas/api/crear/", payload)

    # ======================================================
    # Flujo de aceptacion
    # ======================================================
    def aceptar_tarea(self, tarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/aceptar/", payload)

    def rechazar_tarea(self, tarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/rechazar/", payload)

    # ======================================================
    # Flujo de operaciones
    # ======================================================
    def asignar_owner(self, tarea_id: int | str, payload: Mapping[str, Any]):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/asignar-owner/", payload)

    def enviar_presupuesto(self, tarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/enviar-presupuesto/", payload)

    def marcar_completada(self, tarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/marcar-completada/", payload)

    # ======================================================
    # Subtareas
    # ======================================================
    def crear_subtarea(self, tarea_id: int | str, payload: Mapping[str, Any]):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/subtareas/", payload)

    def delegar_subtarea(self, tarea_id: int | str, payload: Mapping[str, Any]):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/subtareas/delegar/", payload)

    def eliminar_subtarea(self, subtarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/subtarea/{self._path(subtarea_id)}/eliminar/", payload)

    def toggle_subtarea(self, subtarea_id: int | str, payload: Optional[Mapping[str, Any]] = None):
        return self._post(f"/tareas/api/subtarea/{self._path(subtarea_id)}/toggle/", payload)

    # ======================================================
    # Comentarios
    # ======================================================
    def crear_comentario(self, tarea_id: int | str, payload: Mapping[str, Any]):
        return self._post(f"/tareas/api/{self._path(tarea_id)}/comentarios/", payload)

    # ======================================================
    # Tablero
    # ======================================================
    def historico_tareas(self, params: Optional[Mapping[str, Any]] = None):
        return self._get("/tareas/api/historico/", params=params)

    def tablero_snapshot(self, params: Optional[Mapping[str, Any]] = None):
        return self._get("/tareas/api/tablero-snapshot/", params=params)

    # ======================================================
    # Mis tareas
    # ======================================================
    def tareas_asignadas(self, params: Optional[Mapping[str, Any]] = None):
        return self._get("/tareas/api/tareas-asignadas/", params=params)

    # ======================================================
    # Integracion externa
    # ======================================================
    def crear_tarea_externa(self, payload: Mapping[str, Any]):
        return self._post("/tareas/api/externa/crear/", payload)

    # ======================================================
    # Documentos
    # ======================================================
    def listar_adjuntos(self, job_id: int | str):
        return self._get(f"/api/adjuntos/{self._path(job_id)}/")

    def descargar_adjunto(self, job_id: int | str, filename: str) -> bytes:
        response = self._response(
            "GET",
            f"/api/adjuntos/{self._path(job_id)}/descargar/{self._path(filename)}/",
        )
        return response.content

    def preview_adjunto(self, job_id: int | str, filename: str):
        return self._response(
            "GET",
            f"/api/adjuntos/{self._path(job_id)}/preview/{self._path(filename)}/",
        )

    # ======================================================
    # Contabilidad
    # ======================================================
    def cuentas_contables(self, params: Optional[Mapping[str, Any]] = None):
        return self._get("/api/cuentas-contables/", params=params)


FrontendProxy = FrontProxy
