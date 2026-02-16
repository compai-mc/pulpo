import requests
from typing import Optional, List, Dict, Any

from pulpo.util.esquema import CompaiMessage
from pulpo.util.util import require_env

BASE_URL_ORQUESTATOR = require_env("BASE_URL_ORQUESTATOR")


class OrchestratorError(Exception):
    """Raised when orchestrator returns a non-2xx response."""
    pass


def _post_json(url: str, payload: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """
    Internal helper to POST JSON and return parsed JSON response or raise OrchestratorError.
    """
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
    except requests.RequestException as e:
        raise OrchestratorError(f"Network error calling orchestrator: {e}") from e

    if not resp.ok:
        body = resp.text[:2000] if resp.text else ""
        raise OrchestratorError(f"Orchestrator error {resp.status_code}: {body}")

    try:
        return resp.json()
    except Exception as e:
        raise OrchestratorError(
            f"Orchestrator returned non-JSON response: {resp.text[:2000]}"
        ) from e


def create_card(
    board_id: str,
    title: str,
    description: str = "",
    list_name: Optional[str] = None,
    swimlane_name: Optional[str] = None,
    checklists: Optional[List[Dict[str, Any]]] = None,  # ✅ NEW
    timeout: int = 20,
    retry_without_checklists: bool = True,
    **extra_payload: Any,  # ✅ NEW
) -> Dict[str, Any]:
    """
    Calls orchestrator endpoint /card/create to create a Wekan card.

    Optional:
      - checklists: [{"title": "👤 user", "items": ["task 1", "task 2"]}, ...]
      - retry_without_checklists: if orchestrator doesn't support checklists yet,
        retry once without the checklists field.

    Returns orchestrator JSON response.
    """
    url = f"{BASE_URL_ORQUESTATOR}/card/create"

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
        return _post_json(url, payload, timeout=timeout)
    except OrchestratorError as e:
        if checklists and retry_without_checklists:
            payload.pop("checklists", None)
            try:
                return _post_json(url, payload, timeout=timeout)
            except OrchestratorError:
                raise e
        raise


def publish_complete_job(mensaje: CompaiMessage, timeout: int = 20) -> Dict[str, Any]:
    """
    Calls orchestrator endpoint /job/publish/complete to publish a completed job.
    """
    url = f"{BASE_URL_ORQUESTATOR}/job/publish/complete"
    payload = mensaje.model_dump(mode="json", exclude_none=True)
    return _post_json(url, payload, timeout=timeout)


if __name__ == "__main__":
    try:
        res = create_card(
            board_id="mi_board_id",
            title="Título de prueba",
            description="Descripción de prueba",
            list_name="NUEVOS",
            swimlane_name="Virtual",
            checklists=[
                {"title": "👤 johanel", "items": ["Confirmar disponibilidad / stock / alternativa"]},
                {"title": "👤 ingrid.galarza", "items": ["Revisar solicitud y extraer requisitos"]},
                {"title": "👤 angel", "items": ["Estimar plazo de entrega y condiciones"]},
                {"title": "👤 arek", "items": ["Preparar borrador de oferta"]},
            ],
        )
        print(res)
    except Exception as e:
        print(e)
