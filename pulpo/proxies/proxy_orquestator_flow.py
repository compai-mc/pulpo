from typing import Optional, List, Dict, Any

from pulpo.auth.general import MicroTokenManager, MicroHttpClient
from pulpo.util.esquema import CompaiMessage
from pulpo.util.util import require_env

BASE_URL_ORQUESTATOR = require_env("BASE_URL_ORQUESTATOR")
CLIENT_ID = require_env("CLIENT_ID_ORQUESTATOR")
CLIENT_SECRET = require_env("CLIENT_SECRET_ORQUESTATOR")


class OrchestratorError(Exception):
    """Raised when orchestrator returns a non-2xx response."""
    pass


class OrchestratorProxy:
    def __init__(self, base_url: str = BASE_URL_ORQUESTATOR):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json"
        }
        self.tm = MicroTokenManager(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET
        )
        self.client = MicroHttpClient(self.tm)

    def _post(self, path: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
        try:
            return self.client.post(
                f"{self.base_url}{path}",
                json=payload,
                headers=self.headers,
                timeout=timeout
            )
        except Exception as e:
            response = getattr(e, "response", None)
            if response is not None:
                body = response.text[:2000] if response.text else ""
                raise OrchestratorError(
                    f"Orchestrator error {response.status_code}: {body}"
                ) from e
            raise OrchestratorError(f"Network error calling orchestrator: {e}") from e


_default_proxy: OrchestratorProxy | None = None


def _proxy() -> OrchestratorProxy:
    global _default_proxy
    if _default_proxy is None:
        _default_proxy = OrchestratorProxy()
    return _default_proxy


def _post_json(url: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    Internal helper kept for compatibility with callers that import it directly.
    """
    if not url.startswith(BASE_URL_ORQUESTATOR):
        raise OrchestratorError(
            "_post_json only supports BASE_URL_ORQUESTATOR after secure proxy migration"
        )

    path = url[len(BASE_URL_ORQUESTATOR.rstrip("/")):]
    return _proxy()._post(path, payload, timeout=timeout)


def create_card(
    board_id: str,
    title: str,
    description: str = "",
    list_name: Optional[str] = None,
    swimlane_name: Optional[str] = None,
    checklists: Optional[List[Dict[str, Any]]] = None,
    timeout: int = 20,
    retry_without_checklists: bool = True,
    **extra_payload: Any,
) -> Dict[str, Any]:
    """
    Calls orchestrator endpoint /card/create to create a Wekan card.
    """
    payload: Dict[str, Any] = {
        "board_id": board_id,
        "title": title,
        "description": description,
        "list_name": list_name,
        "swimlane_name": swimlane_name,
    }

    if checklists:
        payload["checklists"] = checklists

    if extra_payload:
        payload.update(extra_payload)

    try:
        return _proxy()._post("/card/create", payload, timeout=timeout)
    except OrchestratorError as e:
        if checklists and retry_without_checklists:
            payload.pop("checklists", None)
            try:
                return _proxy()._post("/card/create", payload, timeout=timeout)
            except OrchestratorError:
                raise e
        raise


def publish_complete_job(mensaje: CompaiMessage, timeout: int = 20) -> Dict[str, Any]:
    """
    Calls orchestrator endpoint /job/publish/complete to publish a completed job.
    """
    payload = mensaje.model_dump(mode="json", exclude_none=True)
    return _proxy()._post("/job/publish/complete", payload, timeout=timeout)


if __name__ == "__main__":
    try:
        res = create_card(
            board_id="mi_board_id",
            title="Titulo de prueba",
            description="Descripcion de prueba",
            list_name="NUEVOS",
            swimlane_name="Virtual",
            checklists=[
                {"title": "user johanel", "items": ["Confirmar disponibilidad / stock / alternativa"]},
                {"title": "user ingrid.galarza", "items": ["Revisar solicitud y extraer requisitos"]},
                {"title": "user angel", "items": ["Estimar plazo de entrega y condiciones"]},
                {"title": "user arek", "items": ["Preparar borrador de oferta"]},
            ],
        )
        print(res)
    except Exception as e:
        print(e)
